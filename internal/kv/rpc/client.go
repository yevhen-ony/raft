package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	api "raft/gen/proto/kv/api/v1"
	"raft/internal/kv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeClient struct {
	addr   string
	conn   *grpc.ClientConn
	client api.KVServiceClient
}

type KVTransport struct {
	mu      sync.Mutex
	curr    int
	clients []*NodeClient
}

func NewKVTransport(addrs []string) (*KVTransport, error) {
	if len(addrs) == 0 {
		return nil, errors.New("missing addresses")
	}

	clients := make([]*NodeClient, len(addrs))
	for i, addr := range addrs {
		conn, err := newConn(addr)
		if err != nil {
			closeClients(clients)
			return nil, fmt.Errorf("create conn for %s: %w", addr, err)
		}
		clients[i] = &NodeClient{
			addr:   addr,
			conn:   conn,
			client: api.NewKVServiceClient(conn),
		}
	}

	t := &KVTransport{clients: clients}
	return t, nil
}

func (t *KVTransport) Close() error {
	return closeClients(t.clients)
}

func (t *KVTransport) setCurr(i int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.curr = i % len(t.clients)
}

func (t *KVTransport) getCurr() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.curr
}

func (t *KVTransport) client(i int) api.KVServiceClient {
	ii := i % len(t.clients)
	return t.clients[ii].client
}

func (t *KVTransport) Get(ctx context.Context, key kv.Key) (kv.Value, error) {
	value := kv.ZeroV

	err := t.withClient(ctx, func(ctx context.Context, client api.KVServiceClient) error {
		val, err := t.get(ctx, client, key)
		if err != nil {
			return err
		}
		value = val
		return nil
	})

	return value, err
}

func (t *KVTransport) get(ctx context.Context, client api.KVServiceClient, key kv.Key) (kv.Value, error) {
	rsp, err := client.Get(ctx, &api.GetRequest{Key: string(key)})
	if err != nil {
		return kv.ZeroV, err
	}
	if !rsp.GetFound() {
		return kv.ZeroV, kv.ErrNotFound
	}
	return kv.Value(rsp.GetValue()), nil
}

func (t *KVTransport) Put(ctx context.Context, key kv.Key, value kv.Value) error {
	return t.withClient(ctx, func(ctx context.Context, client api.KVServiceClient) error {
		return t.put(ctx, client, key, value)
	})
}

func (t *KVTransport) put(ctx context.Context, client api.KVServiceClient, key kv.Key, value kv.Value) error {
	_, err := client.Put(ctx, &api.PutRequest{
		Key:   string(key),
		Value: string(value),
	})
	return err
}

func (t *KVTransport) List(ctx context.Context) ([]kv.Pair, error) {
	var items []kv.Pair
	err := t.withClient(ctx, func(ctx context.Context, client api.KVServiceClient) error {
		ii, err := t.list(ctx, client)
		if err != nil {
			return err
		}
		items = ii
		return nil
	})
	return items, err
}

func (t *KVTransport) list(ctx context.Context, client api.KVServiceClient) ([]kv.Pair, error) {
	rsp, err := client.List(ctx, &api.ListRequest{})
	if err != nil {
		return nil, err
	}
	items := Map(rsp.GetItems(), PairFromPB)
	return items, nil
}

func (t *KVTransport) Delete(ctx context.Context, key kv.Key) error {
	return t.withClient(ctx, func(ctx context.Context, client api.KVServiceClient) error {
		return t.delete(ctx, client, key)
	})
}

func (t *KVTransport) delete(ctx context.Context, client api.KVServiceClient, key kv.Key) error {
	rsp, err := client.Delete(ctx, &api.DeleteRequest{Key: string(key)})
	if err != nil {
		return err
	}

	if rsp.GetDeleted() {
		return nil
	}
	return kv.ErrNotFound
}

func (t *KVTransport) withClient(
	ctx context.Context,
	fn func(context.Context, api.KVServiceClient) error,
) error {

	curr := t.getCurr()
	var lastErr error

	for range len(t.clients) {
		err := fn(ctx, t.client(curr))
		err = fromRPCError(err)

		if err == nil || !errors.Is(err, kv.ErrUnavailable) {
			t.setCurr(curr)
			return err
		}
		lastErr = err
		curr++
	}
	return lastErr
}

func closeClients(clients []*NodeClient) error {
	var lastErr error
	for _, node := range clients {
		if node == nil || node.conn == nil {
			continue
		}
		if err := node.conn.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func newConn(addr string) (*grpc.ClientConn, error) {
	opts := grpc.WithTransportCredentials(insecure.NewCredentials())

	return grpc.NewClient(addr, opts)
}

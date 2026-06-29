package core

import (
	cmd "raft/gen/proto/kv/cmd/v1"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Codec interface{
	Marshal(command *cmd.Command) ([]byte, error)
	Unmarshal([]byte) (*cmd.Command, error)
}

// PROTO codec
type ProtoCodec struct{}

func NewProtoCodec() *ProtoCodec { return &ProtoCodec{} }

func (*ProtoCodec) Marshal(command *cmd.Command) ([]byte, error) {
	return proto.Marshal(command)
}

func (*ProtoCodec) Unmarshal(raw []byte) (*cmd.Command, error) {
	var command cmd.Command
	err := proto.Unmarshal(raw, &command)
	return &command, err
}

// JSON codec
type JSONCodec struct{}

func NewJSONCodec() *JSONCodec { return &JSONCodec{} }

func (*JSONCodec) Marshal(command *cmd.Command) ([]byte, error) {
	return protojson.Marshal(command)
}

func (*JSONCodec) Unmarshal(raw []byte) (*cmd.Command, error) {
	var command cmd.Command
	if err := protojson.Unmarshal(raw, &command); err != nil {
		return nil, err
	}
	return &command, nil
}

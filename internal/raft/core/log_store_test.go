package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileLogStore_ReloadsAppendedEntries(tt *testing.T) {
  	ctx := context.Background()

  	store := NewInMemLogStore()
  	log, err := NewLog(ctx, store)
  	require.NoError(tt, err)

  	require.NoError(tt, log.Append(ctx,
  		LogEntry{LogID: LogID{Index: 1, Term: 1}, Command: []byte("one")},
  		LogEntry{LogID: LogID{Index: 2, Term: 1}, Command: []byte("two")},
  	))

  	reloaded, err := NewLog(ctx, store)
  	require.NoError(tt, err)

  	entries, err := reloaded.EntriesAfter(ZeroLogID)
  	require.NoError(tt, err)
  	require.Len(tt, entries, 2)
  	require.Equal(tt, "one", string(entries[0].Command))
  	require.Equal(tt, "two", string(entries[1].Command))
}

func TestFileLogStore_ReloadsAppendAfterReplacement(tt *testing.T) {
  	ctx := context.Background()

	store := NewInMemLogStore()

  	log, err := NewLog(ctx, store)
  	require.NoError(tt, err)

  	require.NoError(tt, log.Append(ctx,
  		LogEntry{LogID: LogID{Index: 1, Term: 1}, Command: []byte("one")},
  		LogEntry{LogID: LogID{Index: 2, Term: 1}, Command: []byte("bad")},
  	))

  	require.NoError(tt, log.AppendAfter(ctx,
  		LogID{Index: 1, Term: 1},
  		LogEntry{LogID: LogID{Index: 2, Term: 2}, Command: []byte("two")},
  	))

  	reloaded, err := NewLog(ctx, store)
  	require.NoError(tt, err)

  	entries, err := reloaded.EntriesAfter(ZeroLogID)
  	require.NoError(tt, err)
  	require.Len(tt, entries, 2)
  	require.Equal(tt, Term(1), entries[0].Term)
  	require.Equal(tt, "one", string(entries[0].Command))
  	require.Equal(tt, Term(2), entries[1].Term)
  	require.Equal(tt, "two", string(entries[1].Command))
}

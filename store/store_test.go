package store_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uw-labs/broximo/store"
)

func TestBadgerStore_TopicStore(t *testing.T) {
	dbPath, err := ioutil.TempDir("", "commission-test-badger")
	require.NoError(t, err)
	defer os.RemoveAll(dbPath)

	s, err := store.NewBadgerStore(dbPath, 0)
	require.NoError(t, err)
	defer s.Close()

	topic1Store, err := s.TopicStore("topic1")
	require.NoError(t, err)
	topic2Store, err := s.TopicStore("topic2")
	require.NoError(t, err)

	topic1Msgs := [][]byte{
		[]byte("t1-msg1"),
		[]byte("t1-msg2"),
		[]byte("t1-msg3"),
	}
	topic2Msgs := [][]byte{
		[]byte("t2-msg1"),
		[]byte("t2-msg2"),
		[]byte("t2-msg3"),
	}
	for i := 0; i < 3; i++ {
		require.NoError(t, topic1Store.SaveMessage(topic1Msgs[i]))
		require.NoError(t, topic2Store.SaveMessage(topic2Msgs[i]))
	}

	retrievedTopic1Msgs := make([][]byte, 0, 3)
	require.NoError(t, topic1Store.ReadAllMessages(func(msg []byte) error {
		retrievedTopic1Msgs = append(retrievedTopic1Msgs, msg)
		return nil
	}))
	require.ElementsMatch(t, topic1Msgs, retrievedTopic1Msgs)

	retrievedTopic2Msgs := make([][]byte, 0, 3)
	require.NoError(t, topic2Store.ReadAllMessages(func(msg []byte) error {
		retrievedTopic2Msgs = append(retrievedTopic2Msgs, msg)
		return nil
	}))
	require.ElementsMatch(t, topic2Msgs, retrievedTopic2Msgs)
}

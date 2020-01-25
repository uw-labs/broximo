package store_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uw-labs/broximo/store"
)

func TestBadgerStore_TopicStore(t *testing.T) {
	t.Parallel()

	dbPath, err := ioutil.TempDir("", "commission-test-badger")
	require.NoError(t, err)
	defer os.RemoveAll(dbPath)

	s, err := store.NewBadgerStore(dbPath, 0)
	require.NoError(t, err)
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	testCases := []struct {
		topicName      string
		messages       [][]byte
		consumerID     string
		consumerOffset store.ConsumerOffset
		expectedOffset uint64
	}{{
		topicName: "topic1",
		messages: [][]byte{
			[]byte("t1-msg1"),
			[]byte("t1-msg2"),
			[]byte("t1-msg3"),
		},
		consumerID:     "consumer",
		consumerOffset: store.ConsumerOffsetOldest,
		expectedOffset: 0,
	}, {
		topicName: "topic2",
		messages: [][]byte{
			[]byte("t2-msg1"),
			[]byte("t2-msg2"),
			[]byte("t2-msg3"),
		},
		consumerID:     "consumer",
		consumerOffset: store.ConsumerOffsetNewest,
		expectedOffset: 3,
	}}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.topicName, func(t *testing.T) {
			topicStore, err := s.TopicStore(testCase.topicName)
			require.NoError(t, err)

			go func() { // Test blocking mechanism.
				msg, err := topicStore.GetMessage(ctx, 1, true)
				require.NoError(t, err)
				require.Equal(t, testCase.messages[0], msg)
			}()

			for _, msg := range testCase.messages {
				require.NoError(t, topicStore.SaveMessage(msg))
			}

			retrievedMsgs := make([][]byte, 0, 3)
			require.NoError(t, topicStore.ReadAllMessages(func(msg []byte) error {
				retrievedMsgs = append(retrievedMsgs, msg)
				return nil
			}))
			require.ElementsMatch(t, testCase.messages, retrievedMsgs)

			_, err = topicStore.GetMessage(ctx, 0, false)
			require.Error(t, err)
			_, err = topicStore.GetMessage(ctx, 4, false)
			require.Equal(t, store.ErrMessageNotFound, err)

			for i := 0; i < 3; i++ {
				retrievedMsgs[i], err = topicStore.GetMessage(ctx, uint64(i+1), false)
				require.NoError(t, err)
			}
			require.ElementsMatch(t, testCase.messages, retrievedMsgs)

			offset, err := topicStore.GetOrCreateConsumer(testCase.consumerID, testCase.consumerOffset)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedOffset, offset)
		})
	}
}

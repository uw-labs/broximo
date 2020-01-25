package store

import (
	"context"
	"encoding/binary"
	"math"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"
)

// ErrMessageNotFound is na error returned when a message with a given offset doesn't exist.
var ErrMessageNotFound = errors.New("message not found")

// ConsumerOffset represents consumer offset.
type ConsumerOffset int8

const (
	// ConsumerOffsetOldest causes the consumer to read all messages from the beginning.
	ConsumerOffsetOldest ConsumerOffset = iota
	// ConsumerOffsetOldest causes the consumer to read all messages sent after the
	// subscription is set up.
	ConsumerOffsetNewest
)

// TopicStore interface for persisting messages for a specific topic.
type TopicStore interface {
	SaveMessage(msg []byte) error
	GetMessage(ctx context.Context, offset uint64, wait bool) ([]byte, error)
	ReadAllMessages(cb func([]byte) error) error

	GetOrCreateConsumer(consumerID string, initialOffset ConsumerOffset) (offset uint64, err error)
	SetConsumerOffset(consumerID string, offset uint64) error
}

type badgerTopicStore struct {
	db         *badger.DB
	topic      []byte
	newMessage *sync.Cond
}

func (store badgerTopicStore) init() error {
	err := store.db.Update(func(tx *badger.Txn) error {
		return store.writeMsg(tx, 0, []byte{})
	})
	if err != nil {
		return err
	}
	go func() {
		// Periodically broadcast so that any blocking call
		// to get message can get it in case it just missed
		// a new message before it was suspended or terminate
		// if the context was cancelled.
		ticker := time.NewTicker(time.Second * 5)
		for range ticker.C {
			store.newMessage.Broadcast()
		}
	}()
	return nil
}

func (store badgerTopicStore) SaveMessage(msg []byte) error {
	err := store.db.Update(func(tx *badger.Txn) error {
		// First retrieve sequence for the last message.
		seq, err := store.lastSequence(tx)
		if err != nil {
			return err
		}
		// Increase message sequence by 1 and store the message
		seq++
		return store.writeMsg(tx, seq, msg)
	})
	if err == nil {
		store.newMessage.Broadcast()
	}
	return err
}

func (store badgerTopicStore) GetMessage(ctx context.Context, offset uint64, wait bool) (msg []byte, err error) {
	if offset == 0 {
		return nil, errors.New("message offset must be larger than 0")
	}
	err = store.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(store.messageKey(offset))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return ErrMessageNotFound
			}
			return err
		}
		return item.Value(func(val []byte) error {
			msg = val
			return nil
		})
	})
	// Wait for the message to be written.
	if err == ErrMessageNotFound && wait {
		store.newMessage.L.Lock()
		store.newMessage.Wait()
		store.newMessage.L.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		return store.GetMessage(ctx, offset, wait)
	}
	return msg, err
}

func (store badgerTopicStore) ReadAllMessages(cb func([]byte) error) error {
	return store.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.IteratorOptions{
			PrefetchValues: true,
			PrefetchSize:   100,
			Reverse:        false,
			AllVersions:    false,
		})
		defer it.Close()

		for it.Seek(store.messageKey(1)); it.ValidForPrefix(store.topic); it.Next() {
			if err := it.Item().Value(func(msg []byte) error { return cb(msg) }); err != nil {
				return err
			}
		}
		return nil
	})
}

func (store badgerTopicStore) GetOrCreateConsumer(consumerID string, initialOffset ConsumerOffset) (offset uint64, err error) {
	key := store.consumerKey(consumerID)
	err = store.db.Update(func(tx *badger.Txn) error {
		item, err := tx.Get(key)
		switch err {
		case nil:
			return item.Value(func(val []byte) error {
				offset = binary.BigEndian.Uint64(val)
				return nil
			})
		case badger.ErrKeyNotFound:
		default:
			return err
		}
		switch initialOffset {
		case ConsumerOffsetOldest:
			offset = 0
		case ConsumerOffsetNewest:
			// Retrieve last sequence.
			offset, err = store.lastSequence(tx)
			if err != nil {
				return err
			}
		default:
			panic(errors.Errorf("unknown offset: %v", initialOffset))
		}
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, offset)

		return tx.Set(key, val)
	})

	return offset, err
}

func (store badgerTopicStore) SetConsumerOffset(consumerID string, offset uint64) error {
	key := store.consumerKey(consumerID)
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, offset)

	return store.db.Update(func(tx *badger.Txn) error {
		// TODO: check that offset is increasing.
		return tx.Set(key, val)
	})
}

func (store badgerTopicStore) lastSequence(tx *badger.Txn) (uint64, error) {
	it := tx.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Reverse:        true,
	})
	defer it.Close()
	if it.Seek(store.messageKey(math.MaxUint64)); !it.ValidForPrefix(store.topic) {
		return 0, errors.Errorf("topic %s was not created yet", string(store.topic))
	}
	lastKey := it.Item().Key()

	return binary.BigEndian.Uint64(lastKey[len(store.topic):]), nil
}

func (store badgerTopicStore) writeMsg(tx *badger.Txn, seq uint64, msg []byte) error {
	return tx.Set(store.messageKey(seq), msg)
}

func (store badgerTopicStore) messageKey(seq uint64) []byte {
	key := make([]byte, len(store.topic)+8)
	copy(key, store.topic)
	binary.BigEndian.PutUint64(key[len(store.topic):], seq)

	return key
}

func (store badgerTopicStore) consumerKey(consumerID string) []byte {
	key := make([]byte, len(store.topic)+len(consumerID)+2)
	copy(key, "s:")
	copy(key[2:], store.topic)
	copy(key[2+len(store.topic):], consumerID)

	return key
}

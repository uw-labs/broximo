package store

import (
	"encoding/binary"
	"math"

	"github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"
)

// TopicStore interface for persisting messages for a specific topic.
type TopicStore interface {
	SaveMessage(msg []byte) error
	ReadAllMessages(cb func([]byte) error) error
}

type badgerTopicStore struct {
	db    *badger.DB
	topic []byte
}

func (store badgerTopicStore) init() error {
	return store.db.Update(func(tx *badger.Txn) error {
		return store.writeMsg(tx, 0, []byte{})
	})
}

func (store badgerTopicStore) SaveMessage(msg []byte) error {
	return store.db.Update(func(tx *badger.Txn) error {
		// First retrieve sequence for the last message.
		it := tx.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			Reverse:        true,
		})
		if it.Seek(store.key(math.MaxUint64)); !it.ValidForPrefix(store.topic) {
			it.Close()
			return errors.Errorf("topic %s was not created yet", string(store.topic))
		}
		lastKey := it.Item().Key()
		it.Close()
		seq := binary.BigEndian.Uint64(lastKey[len(store.topic):])

		// Increase message id by 1 and store the message
		seq++
		return store.writeMsg(tx, seq, msg)
	})
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

		for it.Seek(store.key(1)); it.ValidForPrefix(store.topic); it.Next() {
			if err := it.Item().Value(func(msg []byte) error { return cb(msg) }); err != nil {
				return err
			}
		}
		return nil
	})
}

func (store badgerTopicStore) writeMsg(tx *badger.Txn, seq uint64, msg []byte) error {
	return tx.Set(store.key(seq), msg)
}

func (store badgerTopicStore) key(seq uint64) []byte {
	key := make([]byte, len(store.topic)+8)
	copy(key, store.topic)
	binary.BigEndian.PutUint64(key[len(store.topic):], seq)

	return key
}

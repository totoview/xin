package core

import (
	"encoding/binary"

	"github.com/boltdb/bolt"
)

// KVStore defines the interface for a generic key/value storage.
type KVStore interface {
	Get(k string) ([]byte, error)
	Put(k string, v []byte) error
	Delete(k string) error
}

// KVGetInt64 reads a int64 from s.
func KVGetInt64(s KVStore, k string) (int64, error) {
	dat, err := s.Get(k)
	if err != nil {
		return 0, err
	}
	if len(dat) == 0 {
		return 0, ErrNotFound
	}
	if len(dat) != 8 {
		return 0, ErrInvalidData
	}
	return int64(binary.LittleEndian.Uint64(dat)), nil
}

// KVPutInt64 writes a int64 to s.
func KVPutInt64(s KVStore, k string, v int64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v))
	return s.Put(k, buf)
}

// KVGetString reads a string from s.
func KVGetString(s KVStore, k string) (string, error) {
	dat, err := s.Get(k)
	if err != nil {
		return "", err
	}
	return string(dat), nil
}

// KVPutString writes a string to s.
func KVPutString(s KVStore, k string, v string) error {
	return s.Put(k, []byte(v))
}

// BoltKVStore implements offset storage on Bolt.
type BoltKVStore struct {
	db   *bolt.DB
	name string
}

// NewBoltKVStore creates an k/v store based on Bolt.
func NewBoltKVStore(name string, db *bolt.DB) (*BoltKVStore, error) {
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(name))
		return err
	}); err != nil {
		return nil, err
	}
	return &BoltKVStore{name: name, db: db}, nil
}

// Get returns value for key
func (s *BoltKVStore) Get(key string) ([]byte, error) {
	var v []byte
	if err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.name))
		v = b.Get([]byte(key))
		if len(v) == 0 {
			return ErrNotFound
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return v, nil
}

// Put saves key/value.
func (s *BoltKVStore) Put(key string, value []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.name))
		return b.Put([]byte(key), value)
	})
}

// Delete removes key.
func (s *BoltKVStore) Delete(key string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.name))
		return b.Delete([]byte(key))
	})
}

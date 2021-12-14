// Package mstore provides an ready-to-use wrapper arounc badgerDB
package mstore

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"

	"time"

	"github.com/dgraph-io/badger/v3"
)

const (
	STORAGE_PATH  = "/tmp/golog.d"
	DISCARD_RATIO = 0.5
	GC_INTERVAL   = 10 * time.Minute
)

var (
	db     *badger.DB
	isOpen bool
)

// Marshal takes in an CEvent and marshals it into a gob formatted byte slice..
func Marshal(e interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(e); err != nil {
		return nil, fmt.Errorf("could not encode to bytes: %v", err)
	}
	return buf.Bytes(), nil
}

// Unmarshal parses the gob-encoded data and stores the result in the value pointed to by v.
func Unmarshal(data []byte, v interface{}) (err error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("v must be a pointer and not nil")
	}
	t := fmt.Sprintf("%T", v)
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("could not unmarshal bytes to %s: %v", t, err)
	}

	return nil
}

// InitPersistentMode ensures that the data store is ready.
func InitPersistentMode() error {
	if db != nil && !db.IsClosed() {
		return errors.New("cannot renitialize db while it is still open")
	}

	opts := badger.
		DefaultOptions(STORAGE_PATH).
		WithSyncWrites(false)

	opts.Logger = nil
	d, err := badger.Open(opts)
	if err != nil {
		return err
	}
	go runGC()
	db = d
	isOpen = true
	return nil
}

// InitDisklessMode ensures that the data store is a memory-only store.
func InitDisklessMode() error {
	if db != nil && !db.IsClosed() {
		return errors.New("cannot renitialize db while it is still open")
	}

	opts := badger.
		DefaultOptions("").
		WithInMemory(true)

	opts.Logger = nil

	d, err := badger.Open(opts)
	if err != nil {
		return err
	}
	go runGC()
	db = d
	isOpen = true
	return nil
}

func GenPK(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("data for key is empty")
	}
	h := md5.New()
	_, err := h.Write(data)
	if err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

// Set adds and event to to cache
func Set(data []byte) ([]byte, error) {
	if !isOpen {
		return nil, errors.New("the storage is not open")
	}
	key, err := GenPK(data)
	if err != nil {
		return nil, err
	}

	if e, _ := Get(key); e != nil {
		return nil, errors.New("the entity already exists")
	}

	txn := db.NewTransaction(true)

	if err := txn.Set(key, data); err != nil {
		txn.Discard()
		return nil, err
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	return key, nil
}

// SetWithTTL allows an item to be saved to the database, yet only exist
// for the time set in the TTL. This allows for caching operations where
// a cached item is only valid for a certain period of time.
func SetWithTTL(data []byte, ttl time.Duration) ([]byte, error) {
	if !isOpen {
		return nil, errors.New("the storage is not open")
	}

	key, err := GenPK(data)
	if err != nil {
		return nil, err
	}

	txn := db.NewTransaction(true)
	entry := badger.NewEntry(key, data).WithTTL(ttl)
	if err := txn.SetEntry(entry); err != nil {
		txn.Discard()
		return nil, err
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	return key, nil
}

// Get retrieves the value from the data store.
func Get(key []byte) ([]byte, error) {
	if !isOpen {
		return nil, errors.New("the storage is not open")
	}

	if len(key) != 16 {
		return nil, errors.New("invalid key")
	}

	var value []byte

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return errors.New("key not found")
		}

		item.Value(func(val []byte) error {
			value = append([]byte{}, val...)
			return nil
		})

		return nil
	})

	if err != nil {
		return nil, err
	}
	return value, nil
}

func GetBatch() (me map[string][]byte, err error) {
	if !isOpen {
		return nil, errors.New("the storage is not open")
	}

	me = make(map[string][]byte)
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := base64.StdEncoding.EncodeToString(item.Key())
			err := item.Value(func(v []byte) error {
				me[k] = (v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return
}

// Removes an entry based on the given key.
func Remove(key []byte) (err error) {
	if !isOpen {
		return errors.New("the storage is not open")
	}

	txn := db.NewTransaction(true)
	defer txn.Discard()

	err = txn.Delete(key)
	if err != nil {
		return
	}
	err = txn.Commit()

	if err != nil {
		return
	}
	return
}

// Removes a batch of keys.
func RemoveBatch(keys [][]byte) (ok bool, errs map[string]error) {
	txn := db.NewTransaction(true)
	defer txn.Discard()
	errs = make(map[string]error)

	for _, k := range keys {
		if len(k) == 0 {
			continue
		}
		if err := Remove(k); err != nil {
			key := base64.StdEncoding.EncodeToString(k)
			errs[key] = err
		}
	}

	return len(errs) == 0, errs
}

// IsOpen indicates if the internal database is open or not.
func IsOpen() bool {
	return isOpen
}

// Close closes down the internal database.
func Close() error {
	if db == nil || db.IsClosed() {
		isOpen = false
		return nil
	}
	isOpen = false
	return db.Close()
}

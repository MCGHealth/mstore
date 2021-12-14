package mstore_test

import (
	"encoding/base64"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/MCGHealth/mstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testObj struct {
	Nbr int64
	Txt string
}

func testStruct() testObj {
	t := time.Now().UTC().UnixNano()
	return testObj{
		Nbr: t,
		Txt: fmt.Sprintf("%x", t),
	}
}

func TestGenPK(t *testing.T) {
	data := make([]byte, 0)
	pk, err := mstore.GenPK(data)
	assert.Error(t, err)
	assert.Nil(t, pk)
}

func TestMarshalUnMarshal(t *testing.T) {
	org := testStruct()
	data, err := mstore.Marshal(org)
	assert.NotNil(t, data)
	assert.NotEmpty(t, data)
	assert.NoError(t, err)

	var cpy testObj
	err = mstore.Unmarshal(data, &cpy)
	assert.NoError(t, err)
	assert.Equal(t, org, cpy)

	// be extra sure
	assert.True(t, org.Nbr == cpy.Nbr)
	assert.True(t, org.Txt == cpy.Txt)

	var cp2 testObj
	err = mstore.Unmarshal(data, cp2)
	assert.Errorf(t, err, "v must be a pointer and not nil")

	badData := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
	err = mstore.Unmarshal(badData, &cp2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not unmarshal bytes")

}

func TestStorage(t *testing.T) {
	t.Run("Test Initialize", testInitPersistentMode) // <-- must run first
	t.Run("Test Initialize while open", testInitWhileOpenReturnsError)
	t.Run("Test Initialize Diskless Mode", testInitDisklessMode)
	t.Run("Test Set and Get", testSetAndGet)
	t.Run("Test Set with TTL", testSetWithTTL)
	t.Run("Test Set Duplicate", testSetDupe)
	t.Run("Test Set and Remove", testSetAndRemove)
	t.Run("Test Get and Remove Batch", testGetAndRemoveBatch)
	t.Run("Test invoking after closed db", testAfterClosed)
}

func testInitPersistentMode(t *testing.T) {
	mstore.InitPersistentMode()
	assert.True(t, mstore.IsOpen())
	assert.DirExists(t, mstore.STORAGE_PATH)
	assert.True(t, mstore.IsOpen())

	err := mstore.Close()
	assert.NoError(t, err)
	assert.False(t, mstore.IsOpen())

	err = os.RemoveAll(mstore.STORAGE_PATH)
	assert.NoError(t, err)
	assert.NoDirExists(t, mstore.STORAGE_PATH)
}

func testInitWhileOpenReturnsError(t *testing.T) {
	err := mstore.InitPersistentMode()
	assert.NoError(t, err)

	err = mstore.InitPersistentMode()
	assert.Error(t, err)

	err = mstore.InitDisklessMode()
	assert.Error(t, err)
	mstore.Close()
}

func testInitDisklessMode(t *testing.T) {
	mstore.Close()
	err := mstore.InitDisklessMode()
	assert.NoError(t, err)
	assert.True(t, mstore.IsOpen())
}

func testSetAndGet(t *testing.T) {
	assert.True(t, mstore.IsOpen())
	obj1 := testStruct()
	data1, _ := mstore.Marshal(obj1)
	key, err := mstore.Set(data1)
	assert.NoError(t, err)
	assert.Len(t, key, 16)

	data2, err := mstore.Get(key)
	assert.NoError(t, err)
	assert.NotEmpty(t, data2)

	var obj2 testObj
	err = mstore.Unmarshal(data2, &obj2)
	assert.NoError(t, err)
	assert.NotNil(t, obj2)
	assert.Equal(t, obj1, obj2)

	shortKey := make([]byte, 17)
	longKey := make([]byte, 15)
	noValue := make([]byte, 16)

	data3, err := mstore.Get(shortKey)
	assert.Error(t, err)
	assert.Nil(t, data3)

	data4, err := mstore.Get(longKey)
	assert.Error(t, err)
	assert.Nil(t, data4)

	data5, err := mstore.Get(noValue)
	assert.Error(t, err)
	assert.Nil(t, data5)
}

func testSetWithTTL(t *testing.T) {
	assert.True(t, mstore.IsOpen())
	oneSecond := 1 * time.Second
	obj1 := testStruct()
	data1, _ := mstore.Marshal(obj1)
	key, err := mstore.SetWithTTL(data1, 1*oneSecond)
	require.NoError(t, err)
	require.Len(t, key, 16)

	// give time for the entry to expire
	time.Sleep(1100 * time.Millisecond)

	_, err = mstore.Get(key)
	require.Error(t, err)

	obj2 := testStruct()
	data2, _ := mstore.Marshal(obj2)
	key2, err := mstore.SetWithTTL(data2, 1*time.Minute)
	require.NoError(t, err)
	require.Len(t, key2, 16)

	data3, err := mstore.Get(key2)
	require.NoError(t, err)
	require.NotEmpty(t,data3)
}

func testSetDupe(t *testing.T) {
	org := testStruct()
	data, _ := mstore.Marshal(org)
	k1, err := mstore.Set(data)

	assert.NotEmpty(t, k1)
	assert.Len(t, k1, 16)
	assert.NoError(t, err)

	k2, err := mstore.Set(data)
	assert.NotEqual(t, k1, k2)
	assert.Error(t, err)
}

func testSetAndRemove(t *testing.T) {
	org := testStruct()
	data, _ := mstore.Marshal(org)
	key, err := mstore.Set(data)
	assert.NoError(t, err)

	obj, err := mstore.Get(key)
	assert.NoError(t, err)
	assert.NotNil(t, obj)

	err = mstore.Remove(key)
	assert.NoError(t, err)

	obj, err = mstore.Get(key)
	assert.Errorf(t, err, "key not found")
	assert.Nil(t, obj, "expected obj to be nil")
}

func testGetAndRemoveBatch(t *testing.T) {
	mstore.Close()
	mstore.InitDisklessMode()

	for i := 0; i < 5; i++ {
		org := testStruct()
		data, _ := mstore.Marshal(org)
		_, err := mstore.Set(data)
		assert.NoError(t, err)
	}

	// testing GetBatch()
	entries, err := mstore.GetBatch()
	assert.NoError(t, err)
	assert.Len(t, entries, 5, "expected batch size to be 5")

	keys := make([][]byte, len(entries))
	for k := range entries {
		if len(k) == 0 {
			continue
		}
		key, err := base64.StdEncoding.DecodeString(k)
		assert.NoError(t, err)
		if len(key) == 0 {
			continue
		}
		keys = append(keys, key)
	}

	ok, errs := mstore.RemoveBatch(keys)
	assert.True(t, ok)
	assert.Empty(t, errs)
}

func testAfterClosed(t *testing.T) {
	mstore.Close()

	s, err := mstore.Set(make([]byte, 16))
	assert.Errorf(t, err, "the storage is not open")
	assert.Nil(t, s)

	b, err := mstore.GetBatch()
	assert.Errorf(t, err, "the storage is not open")
	assert.Nil(t, b)

	o, err := mstore.Get(make([]byte, 16))
	assert.Errorf(t, err, "the storage is not open")
	assert.Nil(t, o)

	err = mstore.Remove(make([]byte, 16))
	assert.Errorf(t, err, "the storage is not open")
}

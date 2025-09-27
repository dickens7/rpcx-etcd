package etcdv3singe

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rpcxio/libkv"
	libkvstore "github.com/rpcxio/libkv/store"
	"github.com/rpcxio/libkv/testutils"
	estore "github.com/rpcxio/rpcx-etcd/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var client = "172.16.254.192:31379"

func makeEtcdClient(t *testing.T) libkvstore.Store {
	kv, err := New(
		[]string{client},
		&libkvstore.Config{
			ConnectionTimeout: 3 * time.Second,
			// Username:          "test",
			// Password:          "very-secure",
		},
	)
	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestEtcdV3Register(t *testing.T) {
	kv, err := libkv.NewStore(estore.ETCDV3_SINGLE, []string{client}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*EtcdV3Singe); !ok {
		t.Fatal("Error registering and initializing etcd")
	}
}

func TestEtcdV3Store(t *testing.T) {
	kv := makeEtcdClient(t)
	lockKV := makeEtcdClient(t)
	ttlKV := makeEtcdClient(t)

	defer testutils.RunCleanup(t, kv)

	testutils.RunTestCommon(t, kv)

	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLock(t, kv)
	testutils.RunTestLockTTL(t, kv, lockKV)
	testutils.RunTestLockWait(t, kv, lockKV)
	testutils.RunTestTTL(t, kv, ttlKV)
}

// TestLeaseManagement tests the lease management functionality
func TestLeaseManagement(t *testing.T) {
	store := makeEtcdClient(t)
	defer store.Close()

	etcdStore, ok := store.(*EtcdV3Singe)
	require.True(t, ok)

	// Test multiple keys with same TTL should share the same lease
	key1 := "test/lease/key1"
	key2 := "test/lease/key2"
	value := []byte("test-value")
	options := &libkvstore.WriteOptions{TTL: 10 * time.Second}

	// Put two keys with same TTL
	err := etcdStore.Put(key1, value, options)
	assert.NoError(t, err)

	err = etcdStore.Put(key2, value, options)
	assert.NoError(t, err)

	// Check that both keys use the same lease ID
	etcdStore.mu.RLock()
	assert.Equal(t, 1, len(etcdStore.leaseIDs), "Should have exactly one lease for TTL=10s")

	// Check both keys are in regItems
	assert.Contains(t, etcdStore.regItems, key1)
	assert.Contains(t, etcdStore.regItems, key2)
	etcdStore.mu.RUnlock()

	// Cleanup
	err = etcdStore.Delete(key1)
	assert.NoError(t, err)
	err = etcdStore.Delete(key2)
	assert.NoError(t, err)
}

// TestConcurrentPutSameTTL tests concurrent Put operations with same TTL
func TestConcurrentPutSameTTL(t *testing.T) {
	store := makeEtcdClient(t)
	defer store.Close()

	etcdStore, ok := store.(*EtcdV3Singe)
	require.True(t, ok)

	const numGoroutines = 10
	const ttl = 15 * time.Second

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	// Launch multiple goroutines to put keys with same TTL concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := "test/concurrent/" + string(rune('A'+id))
			value := []byte("value-" + string(rune('0'+id)))
			options := &libkvstore.WriteOptions{TTL: ttl}

			err := etcdStore.Put(key, value, options)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check no errors occurred
	for err := range errors {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify only one lease was created for the TTL
	etcdStore.mu.RLock()
	leaseCount := 0
	for ttlKey := range etcdStore.leaseIDs {
		if ttlKey == int64(ttl.Seconds()) {
			leaseCount++
		}
	}
	assert.Equal(t, 1, leaseCount, "Should have exactly one lease for the TTL")
	etcdStore.mu.RUnlock()
}

// TestRegItemsCacheConsistency tests regItems cache consistency
func TestRegItemsCacheConsistency(t *testing.T) {
	store := makeEtcdClient(t)
	defer store.Close()

	etcdStore, ok := store.(*EtcdV3Singe)
	require.True(t, ok)

	key := "test/cache/consistency"
	value1 := []byte("value1")
	value2 := []byte("value2")

	// Test Put updates cache
	err := etcdStore.Put(key, value1, nil)
	assert.NoError(t, err)

	etcdStore.mu.RLock()
	item, exists := etcdStore.regItems[key]
	assert.True(t, exists, "Key should exist in cache after Put")
	assert.Equal(t, value1, item.value, "Cache should contain the correct value")
	etcdStore.mu.RUnlock()

	// Test Put updates cache with new value
	err = etcdStore.Put(key, value2, nil)
	assert.NoError(t, err)

	etcdStore.mu.RLock()
	item, exists = etcdStore.regItems[key]
	assert.True(t, exists, "Key should still exist in cache after update")
	assert.Equal(t, value2, item.value, "Cache should contain the updated value")
	etcdStore.mu.RUnlock()

	// Test Delete removes from cache
	err = etcdStore.Delete(key)
	assert.NoError(t, err)

	etcdStore.mu.RLock()
	_, exists = etcdStore.regItems[key]
	assert.False(t, exists, "Key should not exist in cache after Delete")
	etcdStore.mu.RUnlock()
}

// TestWatchWithStopChannel tests Watch functionality with stop channel
func TestWatchWithStopChannel(t *testing.T) {
	store := makeEtcdClient(t)
	defer store.Close()

	key := "test/watch/stop"
	value := []byte("initial-value")

	// Put initial value
	err := store.Put(key, value, nil)
	assert.NoError(t, err)

	// Create stop channel
	stopCh := make(chan struct{})

	// Start watching
	watchCh, err := store.Watch(key, stopCh)
	assert.NoError(t, err)

	// Should receive initial value
	select {
	case pair := <-watchCh:
		assert.Equal(t, key, pair.Key)
		assert.Equal(t, value, pair.Value)
	case <-time.After(2 * time.Second):
		t.Fatal("Should receive initial value")
	}

	// Update the key
	newValue := []byte("updated-value")
	go func() {
		time.Sleep(100 * time.Millisecond)
		err := store.Put(key, newValue, nil)
		assert.NoError(t, err)
	}()

	// Should receive update
	select {
	case pair := <-watchCh:
		assert.Equal(t, key, pair.Key)
		assert.Equal(t, newValue, pair.Value)
	case <-time.After(2 * time.Second):
		t.Fatal("Should receive updated value")
	}

	// Close stop channel
	close(stopCh)

	// Watch channel should be closed
	select {
	case _, ok := <-watchCh:
		assert.False(t, ok, "Watch channel should be closed")
	case <-time.After(2 * time.Second):
		t.Fatal("Watch channel should be closed after stop signal")
	}

	// Cleanup
	err = store.Delete(key)
	assert.NoError(t, err)
}

// TestWatchTreeWithStopChannel tests WatchTree functionality with stop channel
func TestWatchTreeWithStopChannel(t *testing.T) {
	store := makeEtcdClient(t)
	defer store.Close()

	prefix := "test/watchtree"
	key1 := prefix + "/key1"
	key2 := prefix + "/key2"
	value := []byte("test-value")

	// Put initial values
	err := store.Put(key1, value, nil)
	assert.NoError(t, err)
	err = store.Put(key2, value, nil)
	assert.NoError(t, err)

	// Create stop channel
	stopCh := make(chan struct{})

	// Start watching tree
	watchCh, err := store.WatchTree(prefix, stopCh)
	assert.NoError(t, err)

	// Should receive initial list
	select {
	case pairs := <-watchCh:
		assert.Len(t, pairs, 2, "Should receive 2 initial pairs")
	case <-time.After(2 * time.Second):
		t.Fatal("Should receive initial pairs")
	}

	// Add a new key
	key3 := prefix + "/key3"
	go func() {
		time.Sleep(100 * time.Millisecond)
		err := store.Put(key3, value, nil)
		assert.NoError(t, err)
	}()

	// Should receive update with 3 pairs
	select {
	case pairs := <-watchCh:
		assert.Len(t, pairs, 3, "Should receive 3 pairs after adding key3")
	case <-time.After(2 * time.Second):
		t.Fatal("Should receive updated pairs")
	}

	// Close stop channel
	close(stopCh)

	// Watch channel should be closed
	select {
	case _, ok := <-watchCh:
		assert.False(t, ok, "WatchTree channel should be closed")
	case <-time.After(2 * time.Second):
		t.Fatal("WatchTree channel should be closed after stop signal")
	}

	// Cleanup
	err = store.DeleteTree(prefix)
	assert.NoError(t, err)
}

// TestDeleteTreeCacheSync tests DeleteTree synchronizes cache
func TestDeleteTreeCacheSync(t *testing.T) {
	store := makeEtcdClient(t)
	defer store.Close()

	etcdStore, ok := store.(*EtcdV3Singe)
	require.True(t, ok)

	prefix := "test/deletetree"
	key1 := prefix + "/key1"
	key2 := prefix + "/key2"
	key3 := prefix + "/subdir/key3"
	value := []byte("test-value")

	// Put test data
	err := etcdStore.Put(key1, value, nil)
	assert.NoError(t, err)
	err = etcdStore.Put(key2, value, nil)
	assert.NoError(t, err)
	err = etcdStore.Put(key3, value, nil)
	assert.NoError(t, err)

	// Verify all keys are in cache
	etcdStore.mu.RLock()
	assert.Contains(t, etcdStore.regItems, key1)
	assert.Contains(t, etcdStore.regItems, key2)
	assert.Contains(t, etcdStore.regItems, key3)
	etcdStore.mu.RUnlock()

	// Delete tree
	err = etcdStore.DeleteTree(prefix)
	assert.NoError(t, err)

	// Verify all keys are removed from cache
	etcdStore.mu.RLock()
	assert.NotContains(t, etcdStore.regItems, key1)
	assert.NotContains(t, etcdStore.regItems, key2)
	assert.NotContains(t, etcdStore.regItems, key3)
	etcdStore.mu.RUnlock()
}

// TestAtomicPutWithTTL tests AtomicPut with TTL functionality
func TestAtomicPutWithTTL(t *testing.T) {
	store := makeEtcdClient(t)
	defer store.Close()

	etcdStore, ok := store.(*EtcdV3Singe)
	require.True(t, ok)

	key := "test/atomic/ttl"
	value := []byte("atomic-value")
	options := &libkvstore.WriteOptions{TTL: 5 * time.Second}

	// AtomicPut new key with TTL
	success, pair, err := etcdStore.AtomicPut(key, value, nil, options)
	assert.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, key, pair.Key)
	assert.Equal(t, value, pair.Value)

	// Verify key is in cache with correct options
	etcdStore.mu.RLock()
	item, exists := etcdStore.regItems[key]
	assert.True(t, exists)
	assert.Equal(t, value, item.value)
	assert.Equal(t, options.TTL, item.options.TTL)
	etcdStore.mu.RUnlock()

	// Verify lease was created
	etcdStore.mu.RLock()
	assert.Contains(t, etcdStore.leaseIDs, int64(5))
	etcdStore.mu.RUnlock()

	// Cleanup
	err = etcdStore.Delete(key)
	assert.NoError(t, err)
}

// TestAtomicDeleteCacheSync tests AtomicDelete synchronizes cache
func TestAtomicDeleteCacheSync(t *testing.T) {
	store := makeEtcdClient(t)
	defer store.Close()

	etcdStore, ok := store.(*EtcdV3Singe)
	require.True(t, ok)

	key := "test/atomic/delete"
	value := []byte("delete-me")

	// Put key first
	err := etcdStore.Put(key, value, nil)
	assert.NoError(t, err)

	// Get the key to have version info
	pair, err := etcdStore.Get(key)
	assert.NoError(t, err)

	// Verify key is in cache
	etcdStore.mu.RLock()
	_, exists := etcdStore.regItems[key]
	assert.True(t, exists)
	etcdStore.mu.RUnlock()

	// AtomicDelete
	success, err := etcdStore.AtomicDelete(key, pair)
	assert.NoError(t, err)
	assert.True(t, success)

	// Verify key is removed from cache
	etcdStore.mu.RLock()
	_, exists = etcdStore.regItems[key]
	assert.False(t, exists)
	etcdStore.mu.RUnlock()
}

// TestFaultRecoveryConfig tests fault recovery configuration
func TestFaultRecoveryConfig(t *testing.T) {
	// Test with FaultRecovery disabled
	originalFaultRecovery := FaultRecovery
	FaultRecovery = false
	defer func() { FaultRecovery = originalFaultRecovery }()

	store := makeEtcdClient(t)
	defer store.Close()

	etcdStore, ok := store.(*EtcdV3Singe)
	require.True(t, ok)

	// Should inherit global FaultRecovery setting
	assert.False(t, etcdStore.FaultRecovery)

	// Test with FaultRecovery enabled
	FaultRecovery = true
	store2 := makeEtcdClient(t)
	defer store2.Close()

	etcdStore2, ok := store2.(*EtcdV3Singe)
	require.True(t, ok)

	assert.True(t, etcdStore2.FaultRecovery)
}

// TestAllowKeyNotFound tests AllowKeyNotFound functionality
func TestAllowKeyNotFound(t *testing.T) {
	store := makeEtcdClient(t)
	defer store.Close()

	etcdStore, ok := store.(*EtcdV3Singe)
	require.True(t, ok)

	// Set AllowKeyNotFound to true
	etcdStore.AllowKeyNotFound = true

	// WatchTree on non-existent directory should not error
	stopCh := make(chan struct{})
	defer close(stopCh)

	watchCh, err := etcdStore.WatchTree("nonexistent/path", stopCh)
	assert.NoError(t, err)
	assert.NotNil(t, watchCh)
}

// TestConnectionFailureHandling tests behavior when etcd connection fails
func TestConnectionFailureHandling(t *testing.T) {
	// Test connecting to non-existent etcd server
	store, err := New([]string{"127.0.0.1:9999"}, &libkvstore.Config{
		ConnectionTimeout: 1 * time.Second,
	})

	// Connection creation should succeed (lazy connection)
	assert.NoError(t, err)
	assert.NotNil(t, store)

	// But actual operations should fail
	err = store.Put("test/key", []byte("value"), nil)
	assert.Error(t, err, "Put should fail with connection error")

	store.Close()
}

// TestLargeValue tests handling of large values
func TestLargeValue(t *testing.T) {
	store := makeEtcdClient(t)
	defer store.Close()

	key := "test/large/value"
	// Create a 1MB value
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Put large value
	err := store.Put(key, largeValue, nil)
	assert.NoError(t, err)

	// Get large value
	pair, err := store.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, largeValue, pair.Value)

	// Cleanup
	err = store.Delete(key)
	assert.NoError(t, err)
}

// TestManyKeysWithDifferentTTLs tests handling many keys with different TTLs
func TestManyKeysWithDifferentTTLs(t *testing.T) {
	store := makeEtcdClient(t)
	defer store.Close()

	etcdStore, ok := store.(*EtcdV3Singe)
	require.True(t, ok)

	// Create keys with different TTLs
	ttls := []time.Duration{5, 10, 15, 20, 30}
	keys := make([]string, len(ttls))

	for i, ttl := range ttls {
		keys[i] = fmt.Sprintf("test/many/ttl%d", i)
		options := &libkvstore.WriteOptions{TTL: ttl * time.Second}
		err := etcdStore.Put(keys[i], []byte(fmt.Sprintf("value%d", i)), options)
		assert.NoError(t, err)
	}

	// Should have different leases for different TTLs
	etcdStore.mu.RLock()
	assert.Equal(t, len(ttls), len(etcdStore.leaseIDs), "Should have one lease per unique TTL")
	etcdStore.mu.RUnlock()

	// Cleanup
	for _, key := range keys {
		err := etcdStore.Delete(key)
		assert.NoError(t, err)
	}
}

// TestAtomicOperationsWithConflicts tests atomic operations with version conflicts
func TestAtomicOperationsWithConflicts(t *testing.T) {
	store := makeEtcdClient(t)
	defer store.Close()

	key := "test/atomic/conflict"
	value1 := []byte("value1")
	value2 := []byte("value2")

	// Put initial value
	err := store.Put(key, value1, nil)
	assert.NoError(t, err)

	// Get current pair
	pair, err := store.Get(key)
	assert.NoError(t, err)

	// Update the key externally (simulate another client)
	err = store.Put(key, value2, nil)
	assert.NoError(t, err)

	// Try to atomic put with old version info - should fail
	success, _, err := store.AtomicPut(key, []byte("new-value"), pair, nil)
	assert.NoError(t, err)
	assert.False(t, success, "AtomicPut should fail with version mismatch")

	// Try to atomic delete with old version info - should fail
	success, err = store.AtomicDelete(key, pair)
	assert.NoError(t, err)
	assert.False(t, success, "AtomicDelete should fail with version mismatch")

	// Cleanup
	err = store.Delete(key)
	assert.NoError(t, err)
}

// BenchmarkPutWithTTL benchmarks Put operations with TTL
func BenchmarkPutWithTTL(b *testing.B) {
	store := makeEtcdClient(&testing.T{})
	defer store.Close()

	options := &libkvstore.WriteOptions{TTL: 30 * time.Second}
	value := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench/put/ttl/%d", i)
		err := store.Put(key, value, options)
		if err != nil {
			b.Errorf("Put failed: %v", err)
		}
	}
}

// BenchmarkPutWithoutTTL benchmarks Put operations without TTL
func BenchmarkPutWithoutTTL(b *testing.B) {
	store := makeEtcdClient(&testing.T{})
	defer store.Close()

	value := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench/put/nottl/%d", i)
		err := store.Put(key, value, nil)
		if err != nil {
			b.Errorf("Put failed: %v", err)
		}
	}
}

// BenchmarkConcurrentPutSameTTL benchmarks concurrent Put operations with same TTL
func BenchmarkConcurrentPutSameTTL(b *testing.B) {
	store := makeEtcdClient(&testing.T{})
	defer store.Close()

	options := &libkvstore.WriteOptions{TTL: 30 * time.Second}
	value := []byte("benchmark-value")

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench/concurrent/same/%d", i)
			err := store.Put(key, value, options)
			if err != nil {
				b.Errorf("Put failed: %v", err)
			}
			i++
		}
	})
}

// TestEtcdV3StoreCompatibility tests compatibility with original store interface
func TestEtcdV3StoreCompatibility(t *testing.T) {
	// Test that our store can be used as a generic store.Store
	var genericStore libkvstore.Store = makeEtcdClient(t)
	defer genericStore.Close()

	// Basic operations should work through the generic interface
	key := "test/compatibility"
	value := []byte("compatibility-test")

	err := genericStore.Put(key, value, nil)
	assert.NoError(t, err)

	pair, err := genericStore.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, pair.Value)

	exists, err := genericStore.Exists(key)
	assert.NoError(t, err)
	assert.True(t, exists)

	err = genericStore.Delete(key)
	assert.NoError(t, err)

	exists, err = genericStore.Exists(key)
	assert.NoError(t, err)
	assert.False(t, exists)
}

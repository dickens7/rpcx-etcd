package etcdv3singe

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/rpcxio/libkv"
	"github.com/rpcxio/libkv/store"
	estore "github.com/rpcxio/rpcx-etcd/store"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func init() {
	libkv.AddStore(estore.ETCDV3_SINGLE, New)
}

// FaultRecovery control whether the fault is recovered
var FaultRecovery = true

// EtcdConfigAutoSyncInterval give a choice to those etcd cluster could not auto sync
// such I deploy clusters in docker they will dial tcp: lookup etcd1: Try again, can just set this to zero
var EtcdConfigAutoSyncInterval = time.Minute * 5

type RegItem struct {
	key     string
	value   []byte
	options *store.WriteOptions
}

// EtcdV3Singe is the receiver type for the Store interface
type EtcdV3Singe struct {
	timeout  time.Duration
	client   *clientv3.Client
	cfg      clientv3.Config
	regItems map[string]RegItem
	leaseIDs map[int64]clientv3.LeaseID
	done     chan struct{}

	AllowKeyNotFound bool
	FaultRecovery    bool

	mu sync.RWMutex
}

// New creates a new Etcd client given a list
// of endpoints and an optional tls config
func New(addrs []string, options *store.Config) (store.Store, error) {
	s := &EtcdV3Singe{
		done:          make(chan struct{}),
		regItems:      make(map[string]RegItem),
		leaseIDs:      make(map[int64]clientv3.LeaseID),
		FaultRecovery: FaultRecovery,
	}

	cfg := clientv3.Config{
		Endpoints: addrs,
	}

	if options != nil {
		s.timeout = options.ConnectionTimeout
		cfg.DialTimeout = options.ConnectionTimeout
		cfg.DialKeepAliveTimeout = options.ConnectionTimeout
		cfg.TLS = options.TLS
		cfg.Username = options.Username
		cfg.Password = options.Password

		cfg.AutoSyncInterval = EtcdConfigAutoSyncInterval
	}
	if s.timeout == 0 {
		s.timeout = 10 * time.Second
	}
	s.cfg = cfg
	cli, err := clientv3.New(s.cfg)
	if err != nil {
		return nil, err
	}
	s.client = cli
	return s, nil
}

func (s *EtcdV3Singe) keepAlive(ttl int64, leaseID clientv3.LeaseID) {
	go func(id clientv3.LeaseID) {
		defer func() {
			// 清理 lease ID 映射
			s.mu.Lock()
			delete(s.leaseIDs, ttl)
			s.mu.Unlock()
		}()

		ch, kaerr := s.client.KeepAlive(context.Background(), id)
		if kaerr != nil {
			log.Printf("Failed to keep alive lease %v: %v", id, kaerr)
			s.handleLeaseFailure(ttl, id)
			return
		}

		for ka := range ch {
			if ka == nil {
				log.Printf("lease %v has expired", id)
				s.handleLeaseFailure(ttl, id)
				return
			}
		}

		// KeepAlive channel 被关闭，说明连接断开
		log.Printf("lease %v keepalive channel closed", id)
		s.handleLeaseFailure(ttl, id)
	}(leaseID)
}

// handleLeaseFailure 处理 lease 失败的情况
func (s *EtcdV3Singe) handleLeaseFailure(ttl int64, expiredLeaseID clientv3.LeaseID) {
	if !s.FaultRecovery {
		return
	}

	go func() {
		log.Printf("lease fault recovery started for %v", expiredLeaseID)

		// 重试机制，最多重试10次
		maxRetries := 10
		for retry := 0; retry < maxRetries; retry++ {
			// 申请新的 lease（避免递归调用 getLeaseID）
			newLeaseID, err := s.grantDirectly(ttl)
			if err != nil {
				log.Printf("lease %v grant err (retry %d): %s", expiredLeaseID, retry, err)
				time.Sleep(time.Duration(retry+1) * time.Second) // 指数退避
				continue
			}

			// 更新 lease ID 映射
			s.mu.Lock()
			s.leaseIDs[ttl] = newLeaseID
			// 复制需要恢复的项目，避免并发问题
			var itemsToRecover []RegItem
			for _, v := range s.regItems {
				if v.options != nil && int64(v.options.TTL.Seconds()) == ttl {
					itemsToRecover = append(itemsToRecover, v)
				}
			}
			s.mu.Unlock()

			// 恢复所有相关的 key-value 对
			allSuccess := true
			for _, item := range itemsToRecover {
				ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
				_, err := s.client.Put(ctx, item.key, string(item.value), clientv3.WithLease(newLeaseID))
				cancel()
				if err != nil {
					log.Printf("lease %v fault recovery, put %v err: %s", expiredLeaseID, item.key, err)
					allSuccess = false
					break
				}
				log.Printf("lease fault recovery %v, path: %s", expiredLeaseID, item.key)
			}

			if allSuccess {
				// 启动新 lease 的 keepAlive
				s.keepAlive(ttl, newLeaseID)
				log.Printf("lease fault recovery completed for %v -> %v", expiredLeaseID, newLeaseID)
				return
			}
		}

		log.Printf("lease fault recovery failed after %d retries for %v", maxRetries, expiredLeaseID)
	}()
}

// grantDirectly 直接申请 lease，不启动 keepAlive
func (s *EtcdV3Singe) grantDirectly(ttl int64) (clientv3.LeaseID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	resp, err := s.client.Grant(ctx, ttl)
	cancel()
	if err != nil {
		return 0, err
	}
	return resp.ID, nil
}

// grant a lease.
func (s *EtcdV3Singe) grant(ttl int64) (clientv3.LeaseID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	resp, err := s.client.Grant(ctx, ttl)
	cancel()
	if err != nil {
		return 0, err
	}
	s.keepAlive(ttl, resp.ID)
	return resp.ID, err
}

func (s *EtcdV3Singe) getLeaseID(ttl int64) (clientv3.LeaseID, error) {
	s.mu.RLock()
	leaseID, ok := s.leaseIDs[ttl]
	s.mu.RUnlock()

	if ok {
		return leaseID, nil
	}

	// 避免持锁调用 grant
	newLeaseID, err := s.grant(ttl)
	if err != nil {
		return 0, err
	}

	// 双重检查锁定模式
	s.mu.Lock()
	defer s.mu.Unlock()

	// 再次检查是否有其他 goroutine 已经创建了相同 TTL 的 lease
	if existingID, exists := s.leaseIDs[ttl]; exists {
		// 如果已存在，撤销新创建的 lease，使用已存在的
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
			s.client.Revoke(ctx, newLeaseID)
			cancel()
		}()
		return existingID, nil
	}

	s.leaseIDs[ttl] = newLeaseID
	return newLeaseID, nil
}

// Put a value at the specified key
func (s *EtcdV3Singe) Put(key string, value []byte, options *store.WriteOptions) error {
	opts := make([]clientv3.OpOption, 0)
	if options != nil && options.TTL != -1 {
		leaseID, err := s.getLeaseID(int64(options.TTL.Seconds()))
		if err != nil {
			return err
		}
		opts = append(opts, clientv3.WithLease(leaseID))
	}

	// 先执行 etcd 操作
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	_, err := s.client.Put(ctx, key, string(value), opts...)
	cancel()

	// 只有 etcd 操作成功后才更新本地缓存
	if err == nil {
		s.mu.Lock()
		s.regItems[key] = RegItem{key: key, value: value, options: options}
		s.mu.Unlock()
	}

	return err
}

// Get a value given its key
func (s *EtcdV3Singe) Get(key string) (*store.KVPair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	resp, err := s.client.Get(ctx, key)
	cancel()
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, store.ErrKeyNotFound
	}

	pair := &store.KVPair{
		Key:       key,
		Value:     resp.Kvs[0].Value,
		LastIndex: uint64(resp.Kvs[0].Version),
	}

	return pair, nil
}

// Delete the value at the specified key
func (s *EtcdV3Singe) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	_, err := s.client.Delete(ctx, key)
	cancel()

	// 只有 etcd 操作成功后才删除本地缓存
	if err == nil {
		s.mu.Lock()
		delete(s.regItems, key)
		s.mu.Unlock()
	}

	return err
}

// Exists verifies if a Key exists in the store
func (s *EtcdV3Singe) Exists(key string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	resp, err := s.client.Get(ctx, key)
	cancel()
	if err != nil {
		return false, err
	}

	return len(resp.Kvs) != 0, nil
}

// Watch for changes on a key.
func (s *EtcdV3Singe) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	watchCh := make(chan *store.KVPair)

	go func() {
		defer close(watchCh)

		// put the current value into returned channel before watch
		pair, err := s.Get(key)
		if err != nil {
			return
		}
		watchCh <- pair

		rch := s.client.Watch(context.Background(), key)
		for {
			select {
			case <-s.done:
				return
			case <-stopCh:
				return
			case wresp, ok := <-rch:
				if !ok || wresp.Canceled { // watch is canceled
					return
				}
				for _, event := range wresp.Events {
					select {
					case watchCh <- &store.KVPair{
						Key:       string(event.Kv.Key),
						Value:     event.Kv.Value,
						LastIndex: uint64(event.Kv.Version),
					}:
					case <-s.done:
						return
					case <-stopCh:
						return
					}
				}
			}
		}
	}()

	return watchCh, nil
}

// WatchTree watches for changes on child nodes under a given directory
func (s *EtcdV3Singe) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	watchCh := make(chan []*store.KVPair)
	list, err := s.List(directory)
	if err != nil {
		if !s.AllowKeyNotFound || err != store.ErrKeyNotFound {
			return watchCh, err
		}
	}
	localKVPair := make(map[string]*store.KVPair)
	for _, v := range list {
		localKVPair[v.Key] = v
	}
	go func() {
		defer close(watchCh)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Send initial list
		select {
		case watchCh <- list:
		case <-s.done:
			return
		case <-stopCh:
			return
		}

		rch := s.client.Watch(ctx, directory, clientv3.WithPrefix())
		for {
			select {
			case <-s.done:
				return
			case <-stopCh:
				return
			case resp, ok := <-rch:
				if !ok || resp.Canceled { // watch is canceled
					return
				}

				for _, event := range resp.Events {
					switch event.Type {
					case mvccpb.PUT:
						localKVPair[string(event.Kv.Key)] = &store.KVPair{
							Key:       string(event.Kv.Key),
							Value:     event.Kv.Value,
							LastIndex: uint64(event.Kv.Version),
						}
					case mvccpb.DELETE:
						delete(localKVPair, string(event.Kv.Key))
					}
				}

				list = list[:0]
				for _, v := range localKVPair {
					list = append(list, v)
				}

				select {
				case watchCh <- list:
				case <-s.done:
					return
				case <-stopCh:
					return
				}
			}
		}
	}()

	return watchCh, nil
}

// NewLock creates a lock for a given key.
// The returned Locker is not held and must be acquired
// with `.Lock`. The Value is optional.
func (s *EtcdV3Singe) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, errors.New("not implemented")
}

// List the content of a given prefix
func (s *EtcdV3Singe) List(directory string) ([]*store.KVPair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	resp, err := s.client.Get(ctx, directory, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	kvpairs := make([]*store.KVPair, 0, len(resp.Kvs))

	if len(resp.Kvs) == 0 {
		return nil, store.ErrKeyNotFound
	}

	for _, kv := range resp.Kvs {
		pair := &store.KVPair{
			Key:       string(kv.Key),
			Value:     kv.Value,
			LastIndex: uint64(kv.Version),
		}
		kvpairs = append(kvpairs, pair)
	}

	return kvpairs, nil
}

// DeleteTree deletes a range of keys under a given directory
func (s *EtcdV3Singe) DeleteTree(directory string) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	_, err := s.client.Delete(ctx, directory, clientv3.WithPrefix())
	cancel()

	// 只有 etcd 操作成功后才删除本地缓存中的相关项
	if err == nil {
		s.mu.Lock()
		var keysToDelete []string
		for key := range s.regItems {
			if strings.HasPrefix(key, directory) {
				keysToDelete = append(keysToDelete, key)
			}
		}
		for _, key := range keysToDelete {
			delete(s.regItems, key)
		}
		s.mu.Unlock()
	}

	return err
}

// AtomicPut CAS operation on a single value.
// Pass previous = nil to create a new key.
func (s *EtcdV3Singe) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	var revision int64
	var presp *clientv3.PutResponse
	var txresp *clientv3.TxnResponse
	var err error
	var opts []clientv3.OpOption

	// 如果有TTL选项，获取lease
	if options != nil && options.TTL != -1 {
		leaseID, lerr := s.getLeaseID(int64(options.TTL.Seconds()))
		if lerr != nil {
			return false, nil, lerr
		}
		opts = append(opts, clientv3.WithLease(leaseID))
	}

	if previous == nil {
		if exist, err := s.Exists(key); err != nil { // not atomicput
			return false, nil, err
		} else if !exist {
			presp, err = s.client.Put(ctx, key, string(value), opts...)
			if err != nil {
				return false, nil, err
			}
			if presp != nil {
				revision = presp.Header.GetRevision()
			}
		} else {
			return false, nil, store.ErrKeyExists
		}
	} else {
		cmps := []clientv3.Cmp{
			clientv3.Compare(clientv3.Value(key), "=", string(previous.Value)),
			clientv3.Compare(clientv3.Version(key), "=", int64(previous.LastIndex)),
		}

		var putOp clientv3.Op
		if len(opts) > 0 {
			putOp = clientv3.OpPut(key, string(value), opts...)
		} else {
			putOp = clientv3.OpPut(key, string(value))
		}

		txresp, err = s.client.Txn(ctx).If(cmps...).
			Then(putOp).
			Commit()
		if err != nil {
			return false, nil, err
		}

		if txresp != nil {
			if txresp.Succeeded {
				revision = txresp.Header.GetRevision()
			} else {
				// 事务失败，版本不匹配，但不返回错误，返回success=false
				return false, nil, nil
			}
		}
	}

	if err != nil {
		return false, nil, err
	}

	// 更新本地缓存
	s.mu.Lock()
	s.regItems[key] = RegItem{key: key, value: value, options: options}
	s.mu.Unlock()

	pair := &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: uint64(revision),
	}

	return true, pair, nil
}

// AtomicDelete cas deletes a single value
func (s *EtcdV3Singe) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	deleted := false
	var err error
	var txresp *clientv3.TxnResponse
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	if previous == nil {
		return false, errors.New("key's version info is needed")
	} else {
		cmps := []clientv3.Cmp{
			clientv3.Compare(clientv3.Value(key), "=", string(previous.Value)),
			clientv3.Compare(clientv3.Version(key), "=", int64(previous.LastIndex)),
		}
		txresp, err = s.client.Txn(ctx).If(cmps...).
			Then(clientv3.OpDelete(key)).
			Commit()
		if err != nil {
			return false, err
		}

		deleted = txresp.Succeeded
		// 如果事务失败（版本不匹配），返回success=false，但不返回错误
		if !deleted {
			return false, nil
		}
	}

	if err != nil {
		return false, err
	}

	// 删除成功后更新本地缓存
	if deleted {
		s.mu.Lock()
		delete(s.regItems, key)
		s.mu.Unlock()
	}

	return deleted, nil
}

// Close closes the client connection
func (s *EtcdV3Singe) Close() {
	defer func() {
		if recover() != nil {
			// close of closed channel panic occur
		}
	}()
	close(s.done)
	s.client.Close()
}

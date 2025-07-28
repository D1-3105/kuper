package tests

import (
	"context"
	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"kuper/pkg/etcd_utils"
	"sync"
)

// MockKV implements clientv3.KV interface with testify mock
type MockKV struct {
	mock.Mock
}

func (m *MockKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	args := m.Called(ctx, key)
	return args.Get(0).(*clientv3.GetResponse), args.Error(1)
}

func (m *MockKV) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	args := m.Called(ctx, key)
	return args.Get(0).(*clientv3.DeleteResponse), args.Error(1)
}

func (m *MockKV) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	args := m.Called(ctx, key, val)
	return args.Get(0).(*clientv3.PutResponse), args.Error(1)
}

func (m *MockKV) Compact(
	ctx context.Context, rev int64, opts ...clientv3.CompactOption,
) (*clientv3.CompactResponse, error) {
	args := m.Called(ctx, rev)
	return args.Get(0).(*clientv3.CompactResponse), args.Error(1)
}

func (m *MockKV) Do(ctx context.Context, op clientv3.Op) (clientv3.OpResponse, error) {
	args := m.Called(ctx, op)
	return args.Get(0).(clientv3.OpResponse), args.Error(1)
}

func (m *MockKV) Txn(ctx context.Context) clientv3.Txn {
	return &MockTxn{}
}

// MockTxn implements clientv3.Txn interface for mocking transactions
type MockTxn struct{}

func (m *MockTxn) If(cs ...clientv3.Cmp) clientv3.Txn   { return m }
func (m *MockTxn) Then(ops ...clientv3.Op) clientv3.Txn { return m }
func (m *MockTxn) Else(ops ...clientv3.Op) clientv3.Txn { return m }
func (m *MockTxn) Commit() (*clientv3.TxnResponse, error) {
	return &clientv3.TxnResponse{}, nil
}

// Locker interface abstracts concurrency.Mutex locking for easier mocking
type Locker interface {
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error
}

// Session interface abstracts concurrency.Session Close for mocking
type Session interface {
	Close() error
}

// --- Global factory vars to be overridden in tests ---
var (
	// By default, call real concurrency.NewSession
	NewSessionFunc = func(cli *clientv3.Client, opts ...concurrency.SessionOption) (*concurrency.Session, error) {
		return concurrency.NewSession(cli, opts...)
	}

	// By default, call real concurrency.NewMutex
	NewMutexFunc = func(s *concurrency.Session, pfx string) *concurrency.Mutex {
		return concurrency.NewMutex(s, pfx)
	}
)

// --- Mocks for ETCDSession and Mutex for tests ---

// MockSession implements Session interface
type MockSession struct{}

func (m *MockSession) Close() error {
	return nil
}

type MockMutex struct {
	sync.Mutex
}

func (m *MockMutex) Lock(ctx context.Context) error {
	m.Mutex.Lock()
	return nil
}

func (m *MockMutex) Unlock(ctx context.Context) error {
	m.Mutex.Unlock()
	return nil
}

func MockNewSession(cli *clientv3.Client, opts ...concurrency.SessionOption) (etcd_utils.ETCDSession, error) {
	return &MockSession{}, nil
}

func MockNewMutex(s etcd_utils.ETCDSession, pfx string) etcd_utils.Locker {
	return &MockMutex{
		sync.Mutex{},
	}
}

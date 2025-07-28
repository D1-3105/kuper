package etcd_utils

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Locker interface {
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error
}

type ETCDSession interface {
	Close() error
}

var ETCDSessionFactory = func(cli *clientv3.Client, opts ...concurrency.SessionOption) (ETCDSession, error) {
	// by default, call real concurrency.NewSession and wrap into an adapter
	sess, err := concurrency.NewSession(cli, opts...)
	if err != nil {
		return nil, err
	}
	return sess, nil
}

var ETCDMutexFactory = func(s ETCDSession, pfx string) Locker {
	// type assert to *ETCDSessionWrapper to get underlying *concurrency.ETCDSession
	return concurrency.NewMutex(s.(*concurrency.Session), pfx)
}

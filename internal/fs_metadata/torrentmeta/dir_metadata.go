package torrentmeta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/etcd/client/v3/concurrency"

	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog/v2"
)

// DirMeta remote metadata info
type DirMeta struct {
	Name       string `json:"name,omitempty" name:"name"`
	VolumeName string
}

// NewDirMeta - Create new DirMeta using volume name and name
func NewDirMeta(volumeName string, name string) (*DirMeta, error) {
	meta := &DirMeta{
		Name:       name,
		VolumeName: volumeName,
	}
	return meta, nil
}

// LockPath - lock path for dir context (affects only operations that are performed with all files at once)
func (dmeta *DirMeta) LockPath() (string, error) {
	return fmt.Sprintf(EntriesLockEndpoint, dmeta.VolumeName, dmeta.Name), nil
}

// AtomicOp - atomic op for current DirMeta object
func (dmeta *DirMeta) AtomicOp(
	ctx context.Context,
	callMe func(context.Context, *concurrency.Session) error,
	etcdClient *clientv3.Client,
	ttl uint16,
) error {
	lockPath, err := dmeta.LockPath()
	if err != nil {
		return err
	}

	sessionCtx, cancelSession := context.WithTimeout(ctx, time.Duration(ttl)*time.Second)
	defer cancelSession()

	session, err := concurrency.NewSession(
		etcdClient,
		concurrency.WithTTL(int(ttl)),
		concurrency.WithContext(sessionCtx),
	)
	if err != nil {
		return fmt.Errorf("failed to create etcd session: %w", err)
	}
	defer func() {
		if err := session.Close(); err != nil {
			klog.Errorf("session for >>%s<< close error: %v", lockPath, err)
		}
	}()

	mu := concurrency.NewMutex(session, lockPath)

	lockCtx, cancelLock := context.WithTimeout(ctx, 5*time.Second)
	defer cancelLock()

	if err := mu.Lock(lockCtx); err != nil {
		return fmt.Errorf("failed to acquire lock for %s: %w", lockPath, err)
	}
	defer func() {
		if err := mu.Unlock(context.Background()); err != nil {
			klog.Errorf("failed to unlock mutex for %s: %v", lockPath, err)
		}
	}()

	return callMe(ctx, session)
}

// Touch - create keepfile
func (dmeta *DirMeta) Touch(ctx context.Context, etcdClient *clientv3.Client) error {
	keepEtcd := Entry{dmeta: dmeta, Name: "keep", Size: 0, Hash: nil, Version: nil, Type: Keep}

	putKeep := func(ctx context.Context, _ *concurrency.Session) error {
		fp, err := keepEtcd.EtcdFullPath()
		if err != nil {
			return err
		}
		marshalled, err := json.Marshal(keepEtcd)
		if err != nil {
			return err
		}
		_, err = etcdClient.Put(ctx, fp, string(marshalled))
		return err
	}
	withLockDirMeta := func(ctx context.Context, _ *concurrency.Session) error {
		return keepEtcd.AtomicOp(ctx, putKeep, etcdClient, 15)
	}
	return dmeta.AtomicOp(ctx, withLockDirMeta, etcdClient, 15)
}

// EtcdFullPath - create full etcd key
func (dmeta *DirMeta) EtcdFullPath() (string, error) {
	return fmt.Sprintf("%v/%v", dmeta.VolumeName, dmeta.Name), nil
}

// GetEntries - get metadata entries associated with current volume and dir
func (dmeta *DirMeta) GetEntries(
	ctx context.Context, etcdClient *clientv3.Client, specificKey *string,
) ([]Entry, error) {
	if etcdClient == nil {
		return nil, errors.New("etcd client is nil")
	}
	pth := dmeta.Name
	if specificKey != nil {
		pth += *specificKey
	}
	remoteDirMeta, err := etcdClient.Get(ctx, fmt.Sprintf(EntriesEndpoint, dmeta.VolumeName, pth))
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(remoteDirMeta.Kvs))
	for _, kv := range remoteDirMeta.Kvs {
		var entry Entry
		err := json.Unmarshal(kv.Value, &entry)
		if err != nil {
			klog.Errorf("failed to unmarshal entry from etcd: %v", err)
			continue
		}
		entry.dmeta = dmeta
	}
	return entries, nil
}

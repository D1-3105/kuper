package torrentmeta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"kuper/pkg/etcd_utils"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"k8s.io/klog/v2"
)

// TorrentMeta - metadata for libtorrent
type TorrentMeta struct {
	entry   Entry
	Content string `json:"content"`
}

// Entry - materialized file meta
type Entry struct {
	dmeta   *DirMeta
	Name    string       `json:"name,omitempty"`
	Size    int64        `json:"size,omitempty"`
	Hash    *string      `json:"hash,omitempty"`
	Version *string      `json:"version,omitempty"`
	Type    DirEntryType `json:"type,omitempty"`
}

func (e *Entry) SetDirMeta(dmeta *DirMeta) {
	e.dmeta = dmeta
}

// GetTorrentMeta - if it's a file - return associated torrent metadata for libtorrent (.torrent content)
func (e *Entry) GetTorrentMeta(ctx context.Context, etcdClient *clientv3.Client) (*TorrentMeta, error) {
	if e.Type == Keep {
		return nil, errors.New("keep file cannot have torrent meta")
	}
	if e.Type == Directory {
		return nil, errors.New("directory cannot have torrent meta")
	}
	if etcdClient == nil {
		return nil, errors.New("etcd client is nil")
	}
	if e.dmeta == nil {
		return nil, errors.New("dir meta is nil")
	}
	torrentMetas, err := etcdClient.Get(ctx, fmt.Sprintf(TorrentMetaEndpoint, e.dmeta.VolumeName, e.dmeta.Name))
	if err != nil {
		return nil, err
	}
	torrentMeta := &TorrentMeta{}
	if torrentMetas.Count == 0 {
		return nil, errors.New("no torrent meta found")
	}
	if err = json.Unmarshal(torrentMetas.Kvs[torrentMetas.Count-1].Value, torrentMeta); err != nil {
		return nil, err
	}
	torrentMeta.entry = *e
	return torrentMeta, nil
}

// EtcdFullPath - full etcd key
func (e *Entry) EtcdFullPath() (string, error) {
	if e.dmeta == nil {
		return "", errors.New("etcd meta is nil")
	}
	return fmt.Sprintf(EntriesEndpoint, e.dmeta.VolumeName, e.dmeta.Name+"/"+e.Name), nil
}

// LockPath - lock for this specific object
func (e *Entry) LockPath() (string, error) {
	return fmt.Sprintf(EntriesLockEndpoint, e.dmeta.VolumeName, e.dmeta.Name+"/"+e.Name), nil
}

// AtomicOp callMe with locked entry
func (e *Entry) AtomicOp(
	ctx context.Context,
	callMe func(context.Context, etcd_utils.ETCDSession) error,
	etcdClient *clientv3.Client,
	ttl uint16,
) error {
	lockPath, err := e.LockPath()
	if err != nil {
		return err
	}

	sessionCtx, cancelSession := context.WithTimeout(ctx, time.Duration(ttl)*time.Second)
	defer cancelSession()

	session, err := ETCDSessionFactory(
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

	mu := ETCDMutexFactory(session, lockPath)

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

// ToMeta - if Entry is directory - return DirMeta
func (e *Entry) ToMeta() (*DirMeta, error) {
	path, err := e.EtcdFullPath()
	if err != nil {
		return nil, err
	}
	if e.Type != Directory {
		return nil, fmt.Errorf("entry %s is not a directory", path)
	}
	return &DirMeta{path, e.dmeta.VolumeName}, nil
}

// Delete delete entry
func (e *Entry) Delete(ctx context.Context, etcdClient *clientv3.Client) error {
	if e.dmeta == nil {
		return fmt.Errorf("{%v}.dmeta == nil", e)
	}
	deleteEntry := func(ctx context.Context, _ etcd_utils.ETCDSession) error {
		onDel, err := e.EtcdFullPath()
		if err != nil {
			return err
		}

		if err != nil {
			return err
		}
		switch e.Type {
		case Keep:
			_, err = etcdClient.Delete(ctx, onDel)
			return err
		case Directory:
			meta, err := e.ToMeta()
			if err != nil {
				return err
			}

			if err := meta.Delete(ctx, etcdClient); err != nil {
				return err
			}
			_, err = etcdClient.Delete(ctx, onDel)
			return err
		case File:
			if _, err = etcdClient.Delete(
				ctx, fmt.Sprintf(TorrentMetaEndpoint, e.dmeta.VolumeName, e.dmeta.Name),
			); err != nil {
				return err
			}
			if _, err = etcdClient.Delete(ctx, onDel); err != nil {
				return err
			}
		}
		return err
	}
	withDmetaLock := func(ctx context.Context, _ etcd_utils.ETCDSession) error {
		return e.AtomicOp(ctx, deleteEntry, etcdClient, 1000)
	}
	return e.dmeta.AtomicOp(ctx, withDmetaLock, etcdClient, 1000)
}

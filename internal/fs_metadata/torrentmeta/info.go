package torrentmeta

import (
	"kuper/pkg/etcd_utils"
)

const (
	EntriesEndpoint     string = "/%s/Entry/%s"       // /volume-name/Entry/dir-meta-paths
	TorrentMetaEndpoint string = "/%s/TorrentMeta/%s" // /volume-name/TorrentMeta/full-path
	EntriesLockEndpoint string = "/%s/Lock/Entry/%s"  // /volume-name/Lock/Entry/lock-keys
)

// ETCDSessionFactory Create session for etcd
var ETCDSessionFactory = etcd_utils.ETCDSessionFactory

// ETCDMutexFactory Create mutex
var ETCDMutexFactory = etcd_utils.ETCDMutexFactory

type DirEntryType int

const (
	File      DirEntryType = 0
	Directory DirEntryType = 1
	Keep      DirEntryType = 2
)

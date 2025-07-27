package torrentmeta

const (
	EntriesEndpoint     string = "/%s/Entry/%s"       // /volume-name/Entry/dir-meta-paths
	TorrentMetaEndpoint string = "/%s/TorrentMeta/%s" // /volume-name/TorrentMeta/full-path
	EntriesLockEndpoint string = "/%s/Lock/Entry/%s"  // /volume-name/Lock/Entry/lock-keys
)

type DirEntryType int

const (
	File      DirEntryType = 0
	Directory DirEntryType = 1
	Keep      DirEntryType = 2
)

package fuse_server

import (
	"context"
	"os"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// FS implements the root filesystem interface.
// It is the entry point for all filesystem operations.
type FS struct{}

// Root returns the root directory node of the filesystem.
// This method is called once when the filesystem is mounted,
// and should return the root directory object.
func (f *FS) Root() (fs.Node, error) {
	return &Dir{}, nil
}

// Dir represents a directory node in the filesystem.
// Here it acts as the root directory, but you can extend
// it to represent subdirectories as well.
type Dir struct{}

// Attr fills out attributes of the directory, such as permissions and mode.
// Here, we mark it as a directory with read and execute permissions (0555).
func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0555
	return nil
}

// ReadDirAll is called when the directory contents are read (e.g. ls command).
// It should return a list of directory entries (files and subdirectories).
// TODO: Implement logic to list both local files and files indexed in etcd.
func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return nil, nil
}

// Lookup looks up a specific entry (file or subdirectory) by name within the directory.
// It should return a corresponding node if the file exists, or fuse.ENOENT if not found.
// TODO: Implement lookup to find files locally or via etcd index.
func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	return nil, syscall.ENOENT
}

// File represents a file node in the filesystem.
type File struct{}

// Attr sets file attributes like mode and size.
// Here, we set the file as read-only with mode 0444.
// TODO: Set actual file size.
func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = 0444
	return nil
}

// ReadAll is called to read the entire contents of the file at once.
// TODO: Implement fetching the file content either from local storage or via torrent.
func (f *File) ReadAll(ctx context.Context) ([]byte, error) {
	return nil, nil
}

// Read provides a streaming read interface for the file.
// This method can be implemented to support reading parts of the file as requested.
// Optional: Implement if you want to stream file content rather than read all at once.
func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	return nil
}

// Write is called when the file is written to.
// Since the filesystem is read-only, we return EPERM (operation not permitted).
func (f *File) Write(_ context.Context, _ *fuse.WriteRequest, _ *fuse.WriteResponse) error {
	return fuse.Errno(syscall.EPERM)
}

// Fsync is called to sync file contents to stable storage.
// Since this is a read-only filesystem, we can no-op and return success.
func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

// Rename is called when a file or directory is renamed within the directory.
// This could be used to trigger publishing a new .torrent for the renamed file.
// TODO: Implement logic to handle renames and publish .torrent metadata.
func (d *Dir) Rename(_ context.Context, _ *fuse.RenameRequest, _ fs.Node) error {
	return nil
}

// Link creates a hard link from an existing file to a new name in the directory.
// This could trigger publishing a new .torrent for the linked file.
// TODO: Implement logic to handle link and publish .torrent metadata.
func (d *Dir) Link(_ context.Context, _ *fuse.LinkRequest, _ fs.Node) (fs.Node, error) {
	return nil, nil
}

// Mkdir is called to create a new subdirectory inside this directory.
// Since this filesystem is read-only, we return EROFS (read-only filesystem error).
func (d *Dir) Mkdir(_ context.Context, _ *fuse.MkdirRequest) (fs.Node, error) {
	return nil, fuse.Errno(syscall.EROFS)
}

// Create is called to create a new file inside the directory.
// Since this filesystem is read-only, we return EROFS error.
func (d *Dir) Create(
	_ context.Context,
	_ *fuse.CreateRequest,
	_ *fuse.CreateResponse,
) (fs.Node, fs.Handle, error) {
	return nil, nil, fuse.Errno(syscall.EROFS)
}

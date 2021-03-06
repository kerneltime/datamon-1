package core

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	iradix "github.com/hashicorp/go-immutable-radix"

	"github.com/oneconcern/datamon/pkg/model"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

func (fs *readOnlyFsInternal) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) (err error) {
	return statFS()
}

func (fs *readOnlyFsInternal) LookUpInode(ctx context.Context, op *fuseops.LookUpInodeOp) error {
	log.Print(fmt.Printf("lookup parent id:%d, child: %s ", op.Parent, op.Name))
	lookupKey := formLookupKey(op.Parent, op.Name)
	val, found := fs.lookupTree.Get(lookupKey)
	if found {
		childEntry := val.(fsEntry)
		op.Entry.Attributes = childEntry.attributes
		if fs.isReadOnly {
			op.Entry.AttributesExpiration = time.Now().Add(cacheYearLong)
			op.Entry.EntryExpiration = op.Entry.AttributesExpiration
		}
		op.Entry.Child = childEntry.iNode
		op.Entry.Generation = 1
	} else {
		return fuse.ENOENT
	}
	return nil
}

func (fs *readOnlyFsInternal) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) (err error) {
	log.Print(fmt.Printf("iNode attr id:%d ", op.Inode))
	key := formKey(op.Inode)
	e, found := fs.fsEntryStore.Get(key)
	if !found {
		return fuse.ENOENT
	}
	fe := e.(fsEntry)
	op.AttributesExpiration = time.Now().Add(cacheYearLong)
	op.Attributes = fe.attributes
	return nil
}

func (fs *readOnlyFsInternal) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) (err error) {
	log.Print(fmt.Printf("SetInodeAttributes iNode id:%d ", op.Inode))
	err = fuse.ENOSYS
	return
}

func (fs *readOnlyFsInternal) ForgetInode(
	ctx context.Context,
	op *fuseops.ForgetInodeOp) (err error) {
	log.Print(fmt.Printf("ForgetInode iNode id:%d ", op.Inode))
	return
}

func (fs *readOnlyFsInternal) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) (err error) {

	log.Print(fmt.Printf("Mkdir parent iNode id:%d ", op.Parent))
	err = fuse.ENOSYS
	return
}

func (fs *readOnlyFsInternal) MkNode(
	ctx context.Context,
	op *fuseops.MkNodeOp) (err error) {
	log.Print(fmt.Printf("MkNode parent iNode id:%d ", op.Parent))
	err = fuse.ENOSYS
	return
}

func (fs *readOnlyFsInternal) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) (err error) {
	log.Print(fmt.Printf("CreateFile parent iNode id:%d name: %s ", op.Parent, op.Name))
	// Take RW lock.
	// Check if the child exists
	// Create child
	// Open Child
	return
}

func (fs *readOnlyFsInternal) CreateSymlink(
	ctx context.Context,
	op *fuseops.CreateSymlinkOp) (err error) {
	log.Print(fmt.Printf("CreateSymLink"))
	err = fuse.ENOSYS
	return
}

// Hard links are not supported in datamon.
func (fs *readOnlyFsInternal) CreateLink(
	ctx context.Context,
	op *fuseops.CreateLinkOp) (err error) {
	log.Print(fmt.Printf("CreateLink"))
	err = fuse.ENOSYS
	return
}

func (fs *readOnlyFsInternal) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) (err error) {
	log.Print(fmt.Printf("Rename new name:"+op.NewName+" oldname:"+op.OldName+" new parent %d, old parent %d ", op.NewParent, op.OldParent))
	err = fuse.ENOSYS
	return
}

func (fs *readOnlyFsInternal) RmDir(
	ctx context.Context,
	op *fuseops.RmDirOp) (err error) {
	log.Print(fmt.Printf("RmDir iNode id:%d ", op.Parent))
	err = fuse.ENOSYS
	return
}

func (fs *readOnlyFsInternal) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) (err error) {
	log.Print(fmt.Printf("Unlink child: "+op.Name+" parent: %d ", op.Parent))
	err = fuse.ENOSYS
	return
}

func (fs *readOnlyFsInternal) OpenDir(ctx context.Context, openDirOp *fuseops.OpenDirOp) error {
	log.Print(fmt.Printf("openDir iNode id:%d ", openDirOp.Inode))
	p, found := fs.fsEntryStore.Get(formKey(openDirOp.Inode))
	if !found {
		return fuse.ENOENT
	}
	fe := p.(fsEntry)
	if isDir(fe) {
		return fuse.ENOTDIR
	}
	return nil
}

func (fs *readOnlyFsInternal) ReadDir(ctx context.Context, readDirOp *fuseops.ReadDirOp) error {

	offset := int(readDirOp.Offset)
	iNode := readDirOp.Inode

	children, found := fs.readDirMap[iNode]

	if !found {
		return fuse.ENOENT
	}

	if offset > len(children) {
		return fuse.EIO
	}

	for i := offset; i < len(children); i++ {
		n := fuseutil.WriteDirent(readDirOp.Dst[readDirOp.BytesRead:], children[i])
		if n == 0 {
			break
		}
		readDirOp.BytesRead += n
	}
	log.Print(fmt.Printf("readDir iNode id:%d offset: %d bytes: %d ", readDirOp.Inode, readDirOp.Offset, readDirOp.BytesRead))
	return nil
}

func (fs *readOnlyFsInternal) ReleaseDirHandle(
	ctx context.Context,
	op *fuseops.ReleaseDirHandleOp) (err error) {
	log.Print(fmt.Printf("ReleaseDirHandle iNode id:%d ", op.Handle))
	return
}

func (fs *readOnlyFsInternal) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) (err error) {
	log.Print(fmt.Printf("OpenFile iNode id:%d handle:%d ", op.Inode, op.Handle))
	return
}

func (fs *readOnlyFsInternal) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) (err error) {
	log.Print(fmt.Printf("ReadFile iNode id:%d, offset: %d ", op.Inode, op.Offset))

	// If file has not been mutated.
	p, found := fs.fsEntryStore.Get(formKey(op.Inode))
	if !found {
		return fuse.ENOENT
	}
	fe := p.(fsEntry)
	reader, err := fs.bundle.ConsumableStore.GetAt(context.Background(), fe.fullPath)
	if err != nil {
		log.Print(err)
		return fuse.EIO
	}
	n, err := reader.ReadAt(op.Dst, op.Offset)
	if err != nil {
		log.Print(err)
		return fuse.EIO
	}
	log.Print(fmt.Printf("Read: %d of %s ", n, fe.fullPath))
	op.BytesRead = n
	return nil
}

func (fs *readOnlyFsInternal) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) (err error) {
	log.Print(fmt.Printf("WriteFile iNode id:%d ", op.Inode))
	err = fuse.ENOSYS
	return
}

func (fs *readOnlyFsInternal) SyncFile(
	ctx context.Context,
	op *fuseops.SyncFileOp) (err error) {
	log.Print(fmt.Printf("SyncFile iNode id:%d ", op.Inode))
	err = fuse.ENOSYS
	return
}

func (fs *readOnlyFsInternal) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) (err error) {
	log.Print(fmt.Printf("FlushFile iNode id:%d ", op.Inode))
	err = fuse.ENOSYS
	return
}

func (fs *readOnlyFsInternal) ReleaseFileHandle(
	ctx context.Context,
	op *fuseops.ReleaseFileHandleOp) (err error) {
	log.Print(fmt.Printf("ReleaseFileHandle iNode id:%d ", op.Handle))
	return
}

func (fs *readOnlyFsInternal) ReadSymlink(
	ctx context.Context,
	op *fuseops.ReadSymlinkOp) (err error) {
	log.Print(fmt.Printf("ReadSymlink iNode id:%d ", op.Inode))
	err = fuse.ENOSYS
	return
}

func (fs *readOnlyFsInternal) RemoveXattr(
	ctx context.Context,
	op *fuseops.RemoveXattrOp) (err error) {
	log.Print(fmt.Printf("RemoveXattr iNode id:%d ", op.Inode))
	err = fuse.ENOSYS
	return
}

func (fs *readOnlyFsInternal) GetXattr(
	ctx context.Context,
	op *fuseops.GetXattrOp) (err error) {
	log.Print(fmt.Printf("GetXattr iNode id:%d ", op.Inode))
	err = fuse.ENOSYS
	return
}

func (fs *readOnlyFsInternal) ListXattr(
	ctx context.Context,
	op *fuseops.ListXattrOp) (err error) {
	log.Print(fmt.Printf("ListXattr iNode id:%d ", op.Inode))
	err = fuse.ENOSYS
	return
}

func (fs *readOnlyFsInternal) SetXattr(
	ctx context.Context,
	op *fuseops.SetXattrOp) (err error) {
	log.Print(fmt.Printf("SetXattr iNode id:%d ", op.Inode))
	err = fuse.ENOSYS
	return
}

func (fs *readOnlyFsInternal) Destroy() {
	log.Print(fmt.Printf("Destroy"))
}

func isDir(fsEntry fsEntry) bool {
	return fsEntry.hash != ""
}

func newDatamonFSEntry(bundleEntry *model.BundleEntry, time time.Time, id fuseops.InodeID, linkCount uint32) *fsEntry {
	var mode os.FileMode = fileReadOnlyMode
	if bundleEntry.Hash == "" {
		mode = dirReadOnlyMode
	}
	return &fsEntry{
		fullPath: bundleEntry.NameWithPath,
		hash:     bundleEntry.Hash,
		iNode:    id,
		attributes: fuseops.InodeAttributes{
			Size:   bundleEntry.Size,
			Nlink:  linkCount,
			Mode:   mode,
			Atime:  time,
			Mtime:  time,
			Ctime:  time,
			Crtime: time,
			Uid:    0, // TODO: Set to uid gid usable by container..
			Gid:    0, // TODO: Same as above
		},
	}
}

func generateBundleDirEntry(nameWithPath string) *model.BundleEntry {
	return &model.BundleEntry{
		Hash:         "", // Directories do not have datamon backed hash
		NameWithPath: nameWithPath,
		FileMode:     dirReadOnlyMode,
		Size:         2048, // TODO: Increase size of directory with file count when mount is mutable.
	}
}

func (fs *readOnlyFsInternal) populateFS(bundle *Bundle) (*ReadOnlyFS, error) {
	dirStoreTxn := fs.fsDirStore.Txn()
	lookupTreeTxn := fs.lookupTree.Txn()
	fsEntryStoreTxn := fs.fsEntryStore.Txn()

	// Add root.
	dirFsEntry := newDatamonFSEntry(generateBundleDirEntry(rootPath), bundle.BundleDescriptor.Timestamp, fuseops.RootInodeID, dirLinkCount)
	err := fs.insertDatamonFSDirEntry(
		dirStoreTxn,
		lookupTreeTxn,
		fsEntryStoreTxn,
		fuseops.RootInodeID, // Root points to itself
		*dirFsEntry)
	if err != nil {
		return nil, err
	}

	// For a Bundle Entry there might be intermediate directories that need adding.
	var nodesToAdd []fsNodeToAdd
	// iNode for fs entries
	var iNode = firstINode

	generateNextINode := func(iNode *fuseops.InodeID) fuseops.InodeID {
		*iNode++
		return *iNode
	}

	for _, bundleEntry := range fs.bundle.GetBundleEntries() {
		bundleEntry := bundleEntry
		// Generate the fsEntry
		newFsEntry := newDatamonFSEntry(&bundleEntry, bundle.BundleDescriptor.Timestamp, generateNextINode(&iNode), fileLinkCount)

		// Add parents if first visit
		// If a parent has been visited, all the parent's parents in the path have been visited
		nameWithPath := bundleEntry.NameWithPath
		for {
			parentPath := path.Dir(nameWithPath)
			// entry under root
			if parentPath == "" || parentPath == "." || parentPath == "/" {
				nodesToAdd = append(nodesToAdd, fsNodeToAdd{
					parentINode: fuseops.RootInodeID,
					fsEntry:     *newFsEntry,
				})
				if len(nodesToAdd) > 1 {
					// If more than one node is to be added populate the parent iNode.
					nodesToAdd[len(nodesToAdd)-2].parentINode = nodesToAdd[len(nodesToAdd)-1].fsEntry.iNode
				}
				break
			}

			// Copy into queue
			nodesToAdd = append(nodesToAdd, fsNodeToAdd{
				parentINode: 0, // undefined
				fsEntry:     *newFsEntry,
			})

			if len(nodesToAdd) > 1 {
				// If more than one node is to be added populate the parent iNode.
				nodesToAdd[len(nodesToAdd)-2].parentINode = nodesToAdd[len(nodesToAdd)-1].fsEntry.iNode
			}

			p, found := dirStoreTxn.Get([]byte(parentPath))
			if !found {

				newFsEntry = newDatamonFSEntry(generateBundleDirEntry(parentPath), bundle.BundleDescriptor.Timestamp, generateNextINode(&iNode), dirLinkCount)

				// Continue till we hit root or found
				nameWithPath = parentPath
				continue
			} else {
				parentDirEntry := p.(fsEntry)
				if len(nodesToAdd) == 1 {
					nodesToAdd[len(nodesToAdd)-1].parentINode = parentDirEntry.iNode
				}
			}
			break
		}

		for _, nodeToAdd := range nodesToAdd {
			if nodeToAdd.fsEntry.attributes.Nlink == dirLinkCount {
				err = fs.insertDatamonFSDirEntry(
					dirStoreTxn,
					lookupTreeTxn,
					fsEntryStoreTxn,
					nodeToAdd.parentINode,
					nodeToAdd.fsEntry,
				)

			} else {
				err = fs.insertDatamonFSEntry(
					lookupTreeTxn,
					fsEntryStoreTxn,
					nodeToAdd.parentINode,
					nodeToAdd.fsEntry,
				)
			}
			if err != nil {
				return nil, err
			}
			nodesToAdd = nodesToAdd[:0]
		}
	} // End walking bundle entries q2

	fs.fsEntryStore = fsEntryStoreTxn.Commit()
	fs.lookupTree = lookupTreeTxn.Commit()
	fs.fsDirStore = dirStoreTxn.Commit()
	fs.isReadOnly = true
	return &ReadOnlyFS{
		fsInternal: fs,
		server:     fuseutil.NewFileSystemServer(fs),
	}, nil
}

func (fs *readOnlyFsInternal) insertDatamonFSDirEntry(
	dirStoreTxn *iradix.Txn,
	lookupTreeTxn *iradix.Txn,
	fsEntryStoreTxn *iradix.Txn,
	parentInode fuseops.InodeID,
	dirFsEntry fsEntry) error {

	_, update := dirStoreTxn.Insert([]byte(dirFsEntry.fullPath), dirFsEntry)

	if update {
		return errors.New("dirStore updates are not expected: /" + dirFsEntry.fullPath)
	}

	key := formKey(dirFsEntry.iNode)

	_, update = fsEntryStoreTxn.Insert(key, dirFsEntry)
	if update {
		return errors.New("fsEntryStore updates are not expected: /")
	}

	if dirFsEntry.iNode != fuseops.RootInodeID {
		key = formLookupKey(parentInode, path.Base(dirFsEntry.fullPath))

		_, update = lookupTreeTxn.Insert(key, dirFsEntry)
		if update {
			return errors.New("lookupTree updates are not expected: " + dirFsEntry.fullPath)
		}

		childEntries := fs.readDirMap[parentInode]
		childEntries = append(childEntries, fuseutil.Dirent{
			Offset: fuseops.DirOffset(len(childEntries) + 1),
			Inode:  dirFsEntry.iNode,
			Name:   path.Base(dirFsEntry.fullPath),
			Type:   fuseutil.DT_Directory,
		})
		fs.readDirMap[parentInode] = childEntries
	}

	return nil
}

func (fs *readOnlyFsInternal) insertDatamonFSEntry(
	lookupTreeTxn *iradix.Txn,
	fsEntryStoreTxn *iradix.Txn,
	parentInode fuseops.InodeID,
	fsEntry fsEntry) error {

	key := formKey(fsEntry.iNode)

	_, update := fsEntryStoreTxn.Insert(key, fsEntry)
	if update {
		return errors.New("fsEntryStore updates are not expected: " + fsEntry.fullPath)
	}

	key = formLookupKey(parentInode, path.Base(fsEntry.fullPath))

	_, update = lookupTreeTxn.Insert(key, fsEntry)
	if update {
		return errors.New("lookupTree updates are not expected: " + fsEntry.fullPath)
	}

	childEntries := fs.readDirMap[parentInode]
	childEntries = append(childEntries, fuseutil.Dirent{
		Offset: fuseops.DirOffset(len(childEntries) + 1),
		Inode:  fsEntry.iNode,
		Name:   path.Base(fsEntry.fullPath),
		Type:   fuseutil.DT_File,
	})
	fs.readDirMap[parentInode] = childEntries

	return nil
}

type readOnlyFsInternal struct {

	// Backing bundle for this FS.
	bundle *Bundle

	// Get iNode for path. This is needed to generate directory entries without imposing a strict order of traversal.
	fsDirStore *iradix.Tree

	// Get fsEntry for an iNode. Speed up stat and other calls keyed by iNode
	fsEntryStore *iradix.Tree

	// Fast lookup of parent iNode id + child name, returns iNode of child. This is a common operation and it's speed is
	// important.
	lookupTree *iradix.Tree

	// List of children for a given iNode. Maps inode id to list of children. This stitches the fuse FS together.
	readDirMap map[fuseops.InodeID][]fuseutil.Dirent

	// readonly
	isReadOnly bool
}

// fsEntry is a node in the filesystem.
type fsEntry struct {
	hash string // Set for files, empty for directories

	// iNode ID is generated on the fly for a bundle that is committed. Since the file list
	// for a bundle is static and the list of files is frozen, multiple mounts of the same
	// bundle will preserve a fixed iNode for a file provided the order of reading the files
	// remains fixed.
	iNode      fuseops.InodeID         // Unique ID for Fuse
	attributes fuseops.InodeAttributes // Fuse Attributes
	fullPath   string
}

type fsNodeToAdd struct {
	parentINode fuseops.InodeID
	fsEntry     fsEntry
}

package cafs

import (
	"bytes"
	"context"
	"hash/crc32"
	"io"
	"log"
	"sync"

	"go.uber.org/zap"

	"github.com/oneconcern/datamon/pkg/storage"
	"github.com/oneconcern/datamon/pkg/storage/localfs"

	"github.com/docker/go-units"
)

const (
	DefaultLeafSize = 2 * 1024 * 1024
)

func Backend(store storage.Store) Option {
	return func(w *defaultFs) {
		w.fs = store
	}
}

// LeafSize configuration for the blake2b hashes
func LeafSize(sz uint32) Option {
	return func(w *defaultFs) {
		w.leafSize = sz
	}
}

type HasOption func(*hasOpts)

func HasOnlyRoots() HasOption {
	return func(opts *hasOpts) {
		opts.OnlyRoots = true
	}
}

func HasGatherIncomplete() HasOption {
	return func(opts *hasOpts) {
		opts.OnlyRoots = true
		opts.GatherIncomplete = true
	}
}

func LeafTruncation(a bool) Option {
	return func(w *defaultFs) {
		w.leafTruncation = a
	}
}

func Prefix(prefix string) Option {
	return func(w *defaultFs) {
		w.prefix = prefix
	}
}

type hasOpts struct {
	OnlyRoots, GatherIncomplete bool
	_                           struct{} // disallow unkeyed usage
}

// Option to configure content addressable FS components
type Option func(*defaultFs)

// Fs implementations provide content-addressable filesystem operations
type Fs interface {
	Get(context.Context, Key) (io.ReadCloser, error)
	Put(context.Context, io.Reader) (int64, Key, []byte, bool, error)
	Delete(context.Context, Key) error
	Clear(context.Context) error
	Keys(context.Context) ([]Key, error)
	RootKeys(context.Context) ([]Key, error)
	Has(context.Context, Key, ...HasOption) (bool, []Key, error)
}

// New creates a new file system operations instance for a repository
func New(opts ...Option) (Fs, error) {
	f := &defaultFs{
		fs:       localfs.New(nil),
		leafSize: uint32(5 * units.MiB),
	}

	for _, apply := range opts {
		apply(f)
	}
	return f, nil
}

type defaultFs struct {
	fs             storage.Store
	leafSize       uint32
	prefix         string
	zl             zap.Logger //nolint:structcheck,unused
	l              log.Logger //nolint:structcheck,unused
	leafTruncation bool
}

func (d *defaultFs) Put(ctx context.Context, src io.Reader) (int64, Key, []byte, bool, error) {
	w := d.writer(d.prefix)
	defer w.Close()
	written, err := io.Copy(w, src)
	if err != nil {
		return 0, Key{}, nil, false, err
	}
	key, keys, err := w.Flush()
	if err != nil {
		return 0, Key{}, nil, false, err
	}
	if err = w.Close(); err != nil {
		return 0, Key{}, nil, false, err
	}
	found, _ := d.fs.Has(context.TODO(), d.prefix+key.String())
	if !found {
		crcFS, ok := d.fs.(storage.StoreCRC)
		if ok {
			buffer := append(keys, key[:]...)
			crc := crc32.Checksum(buffer, crc32.MakeTable(crc32.Castagnoli))
			err = crcFS.PutCRC(context.TODO(), d.prefix+key.String(), bytes.NewReader(buffer), storage.OverWrite, crc)
		} else {
			err = d.fs.Put(ctx, d.prefix+key.String(), bytes.NewReader(append(keys, key[:]...)), storage.OverWrite)
		}
		if err != nil {
			return 0, Key{}, nil, found, err
		}
	}

	return written, key, keys, found, nil
}

func (d *defaultFs) Get(ctx context.Context, hash Key) (io.ReadCloser, error) {
	return newReader(d.fs, hash, d.leafSize, d.prefix, TruncateLeaf(d.leafTruncation))
}

func (d *defaultFs) writer(prefix string) Writer {
	return &fsWriter{
		fs:            d.fs,
		leafSize:      d.leafSize,
		leafs:         nil,
		buf:           make([]byte, d.leafSize),
		offset:        0,
		flushed:       0,
		pather:        nil,
		prefix:        prefix,
		count:         0,
		flushChan:     make(chan blobFlush, 100000),
		errC:          make(chan error, 1000000),
		maxGoRoutines: make(chan struct{}, maxGoRoutinesPerPut),
		wg:            sync.WaitGroup{},
	}
}

func (d *defaultFs) Delete(ctx context.Context, hash Key) error {
	keys, err := LeafsForHash(d.fs, hash, d.leafSize, d.prefix)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err = d.fs.Delete(ctx, key.String()); err != nil {
			return err
		}
	}

	return d.fs.Delete(ctx, hash.String())
}

func (d *defaultFs) Clear(ctx context.Context) error {
	return d.fs.Clear(ctx)
}

func (d *defaultFs) Keys(ctx context.Context) ([]Key, error) {
	return d.keys(ctx, matchAnyKey)
}

func (d *defaultFs) keys(ctx context.Context, matches func(Key) bool) ([]Key, error) {
	v, err := d.fs.Keys(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]Key, 0, len(v))
	for _, k := range v {
		kk, err := KeyFromString(k)
		if err != nil {
			return nil, err
		}

		if matches(kk) {
			result = append(result, kk)
		}
	}
	return result, nil
}

func (d *defaultFs) RootKeys(ctx context.Context) ([]Key, error) {
	return d.keys(ctx, d.matchOnlyObjectRoots)
}

func (d *defaultFs) matchOnlyObjectRoots(key Key) bool {
	return IsRootKey(d.fs, key, d.leafSize)
}

func (d *defaultFs) Has(ctx context.Context, key Key, cfgs ...HasOption) (bool, []Key, error) {
	var opts hasOpts
	for _, apply := range cfgs {
		apply(&opts)
	}

	has, err := d.fs.Has(ctx, key.String())
	if err != nil {
		return false, nil, err
	}

	if !has {
		return false, nil, nil
	}

	if !opts.GatherIncomplete && !opts.OnlyRoots {
		return has, nil, nil
	}

	ks, err := LeafsForHash(d.fs, key, d.leafSize, d.prefix)
	if err != nil {
		return false, nil, nil
	}
	if len(ks) == 0 {
		return false, nil, nil
	}

	var keys []Key
	if opts.GatherIncomplete {
		for _, k := range ks {
			if ok, err := d.fs.Has(ctx, k.String()); err != nil || !ok {
				keys = append(keys, k)
			}
		}
	}
	return true, keys, nil
}

func matchAnyKey(_ Key) bool { return true }

func IsRootKey(fs storage.Store, key Key, leafSize uint32) bool {
	keys, err := LeafsForHash(fs, key, leafSize, "")
	if err != nil {
		return false
	}
	return len(keys) > 0
}

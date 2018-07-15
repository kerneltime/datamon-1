package goofys

import (
	"fmt"
	"os"
	"os/exec"
	"time"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/sirupsen/logrus"
)

type Config struct {
	// File system
	MountOptions  map[string]string
	MountPoint    string
	MountPointArg string

	Cache    []string
	DirMode  os.FileMode
	FileMode os.FileMode
	Uid      uint32
	Gid      uint32

	// S3
	Endpoint       string
	Region         string
	RegionSet      bool
	StorageClass   string
	Profile        string
	UseContentType bool
	UseSSE         bool
	UseKMS         bool
	KMSKeyID       string
	ACL            string

	// Tuning
	Cheap        bool
	ExplicitDir  bool
	StatCacheTTL time.Duration
	TypeCacheTTL time.Duration

	// Debugging
	DebugFuse  bool
	DebugS3    bool
	Foreground bool
}

func Mount(
	ctx context.Context,
	bucketName string,
	config *Config) (fs *Goofys, mfs *fuse.MountedFileSystem, err error) {

	awsConfig := &aws.Config{
		Region: aws.String("us-west-2"),
		Logger: s3Log,
		//LogLevel: aws.LogLevel(aws.LogDebug),
	}

	if len(config.Profile) > 0 {
		awsConfig.Credentials = credentials.NewSharedCredentials("", config.Profile)
	}

	if len(config.Endpoint) > 0 {
		awsConfig.Endpoint = &config.Endpoint
	}

	awsConfig.S3ForcePathStyle = aws.Bool(true)

	fs = NewGoofys(ctx, bucketName, awsConfig, config)
	if fs == nil {
		err = fmt.Errorf("Mount: initialization failed")
		return
	}
	server := fuseutil.NewFileSystemServer(fs)

	// Mount the file system.
	mountCfg := &fuse.MountConfig{
		FSName:                  bucketName,
		Options:                 config.MountOptions,
		ErrorLogger:             GetStdLogger(NewLogger("fuse"), logrus.ErrorLevel),
		DisableWritebackCaching: true,
	}

	if config.DebugFuse {
		fuseLog.Level = logrus.DebugLevel
		log.Level = logrus.DebugLevel
		mountCfg.DebugLogger = GetStdLogger(fuseLog, logrus.DebugLevel)
	}

	mfs, err = fuse.Mount(config.MountPoint, server, mountCfg)
	if err != nil {
		err = fmt.Errorf("Mount: %v", err)
		return
	}

	if len(config.Cache) != 0 {
		log.Infof("Starting catfs %v", config.Cache)
		catfs := exec.Command("catfs", config.Cache...)
		lvl := logrus.InfoLevel
		if config.DebugFuse {
			lvl = logrus.DebugLevel
			catfs.Env = append(catfs.Env, "RUST_LOG=debug")
		} else {
			catfs.Env = append(catfs.Env, "RUST_LOG=info")
		}
		catfsLog := GetLogger("catfs")
		catfsLog.Formatter.(*LogHandle).Lvl = &lvl
		catfs.Stderr = catfsLog.Writer()
		err = catfs.Start()
		if err != nil {
			err = fmt.Errorf("Failed to start catfs: %v", err)

			// sleep a bit otherwise can't unmount right away
			time.Sleep(time.Second)
			err2 := TryUnmount(config.MountPoint)
			if err2 != nil {
				err = fmt.Errorf("%v. Failed to unmount: %v", err, err2)
			}
		}

		go func() {
			err := catfs.Wait()
			log.Errorf("catfs exited: %v", err)

			if err != nil {
				// if catfs terminated cleanly, it
				// should have unmounted this,
				// otherwise we will do it ourselves
				err2 := TryUnmount(config.MountPointArg)
				if err2 != nil {
					log.Errorf("Failed to unmount: %v", err2)
				}
			}

			if config.MountPointArg != config.MountPoint {
				err2 := TryUnmount(config.MountPoint)
				if err2 != nil {
					log.Errorf("Failed to unmount: %v", err2)
				}
			}

			if err != nil {
				os.Exit(1)
			}
		}()
	}

	return
}

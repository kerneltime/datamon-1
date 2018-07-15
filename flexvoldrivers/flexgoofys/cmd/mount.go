// Copyright © 2018 One Concern

package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"

	"github.com/jacobsa/fuse"
	"github.com/json-iterator/go"
	"github.com/kardianos/osext"
	daemon "github.com/sevlyar/go-daemon"

	"github.com/oneconcern/trumpet/pkg/goofys"
	"github.com/spf13/cobra"
)

type mountOpts struct {
	Bucket         string `json:"bucket"`
	SubPath        string `json:"subPath"`
	DirMode        string `json:"dirMode"`
	FileMode       string `json:"fileMode"`
	UID            int    `json:"uid"`
	GID            int    `json:"gid"`
	Region         string `json:"region"`
	PodName        string `json:"kubernetes.io/pod.name"`
	PodNamespace   string `json:"kubernetes.io/pod.namespace"`
	PodUID         string `json:"kubernetes.io/pod.uid"`
	PvOrVolumeName string `json:"kubernetes.io/pvOrVolumeName"`
	ServiceAccount string `json:"kubernetes.io/serviceAccount.name"`
	AccessMode     string `json:"kubernetes.io/readwrite"`
	AccessKeyID    string `json:"kubernetes.io/secret/aws_access_key_id"`
	SecretKey      string `json:"kubernetes.io/secret/aws_secret_access_key"`
}

// mountCmd represents the mount command
var mountCmd = &cobra.Command{
	Use:   "mount [MOUNT DIR] [JSON OPTIONS]",
	Short: "mounts a S3 bucket to a local folder",
	Long:  `mounts a S3 bucket to a local folder`,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		mpath := args[0]
		if mpath == "" {
			respond(dsFailure, "mount path is required")
		}
		var opts mountOpts
		if err := jsoniter.UnmarshalFromString(args[1], &opts); err != nil {
			log.Println("options unmarshal", err)
			respond(dsFailure, err.Error())
		}

		mountPoint := filepath.Join("/mnt/goofys", opts.Bucket)
		os.MkdirAll(mountPoint, 0755)

		cfg, err := goofysConfig(mountPoint, &opts)
		if err != nil {
			log.Println("build goofys config", err)
			respond(dsFailure, err.Error())
		}

		child, err := daemonize()
		if err != nil {
			log.Println("daemonize", err)
			respond(dsFailure, err.Error())
		}

		if child != nil {
			if err = os.RemoveAll(mpath); err != nil {
				log.Println("parent remove previous path", err)
				respond(dsFailure, err.Error())
				return
			}
			if err = os.Symlink(mountPoint, mpath); err != nil {
				log.Println("parent symlink", mountPoint, "to", mpath, err)
				respond(dsFailure, err.Error())
				return
			}
			log.Printf("bucket %s was mounted for %s", opts.Bucket, mpath)
			respond(dsSuccess, "Bucket was mounted.")
			return
		}

		log.Printf("mounting goofys with: %#v", *cfg)
		fs, mfs, err := goofys.Mount(context.Background(), opts.Bucket, cfg)
		if err != nil {
			kill(os.Getppid(), syscall.SIGUSR1)
			log.Printf("mount %s: %v", mpath, err)
			respond(dsFailure, err.Error())
		}

		if err = kill(os.Getppid(), syscall.SIGUSR1); err != nil {
			log.Printf("kill ppid: %v", err)
			err = nil
		}
		registerSIGINTHandler(fs, mountPoint)
		err = mfs.Join(context.Background())
		if err != nil {
			log.Println("join fuse fs", err)
			respond(dsFailure, fmt.Sprintf("mounted filesystem join: %v", err))
		}
		log.Printf("bucket %s was unmounted for %s", opts.Bucket, mpath)
		respond(dsSuccess, "Bucket was mounted.")
	},
}

func init() {
	rootCmd.AddCommand(mountCmd)
}

func daemonize() (child *os.Process, err error) {
	var wg sync.WaitGroup
	waitForSignal(&wg)

	massageArg0()

	ctx := new(daemon.Context)
	child, err = ctx.Reborn()

	if err != nil {
		err = fmt.Errorf("unable to daemonize: %v", err)
		return
	}

	if child != nil {
		// attempt to wait for child to notify parent
		wg.Wait()
		if waitedForSignal == syscall.SIGUSR1 {
			log.Println("received signal from child")
			return
		}
		err = fuse.EINVAL
		return
	} else {
		// kill our own waiting goroutine
		kill(os.Getpid(), syscall.SIGUSR1)
		wg.Wait()
		defer ctx.Release()
		log.Println("parent process done")
	}
	return
}

type goofysFs interface {
	SigUsr1()
}

func registerSIGINTHandler(fs goofysFs, mpath string) {
	// Register for SIGINT.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM, syscall.SIGUSR1)

	// Start a goroutine that will unmount when the signal is received.
	go func() {
		for {
			s := <-signalChan
			if s == syscall.SIGUSR1 {
				log.Printf("Received %v", s)
				fs.SigUsr1()
				continue
			}

			log.Printf("Received %v, attempting to unmount...", s)

			err := goofys.TryUnmount(mpath)
			if err != nil {
				log.Printf("Failed to unmount in response to %v: %v", s, err)
			} else {
				log.Printf("Successfully unmounted %v in response to %v", mpath, s)
				return
			}

		}
	}()
}

var waitedForSignal os.Signal

func waitForSignal(wg *sync.WaitGroup) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGUSR1, syscall.SIGUSR2)

	wg.Add(1)
	go func() {
		waitedForSignal = <-signalChan
		wg.Done()
	}()
}

func kill(pid int, s os.Signal) error {
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	defer p.Release()

	if err := p.Signal(s); err != nil {
		return err
	}
	return p.Release()
}

func massageArg0() {
	var err error
	os.Args[0], err = osext.Executable()
	if err != nil {
		panic(fmt.Sprintf("Unable to discover current executable: %v", err))
	}
}

func goofysConfig(mpath string, opts *mountOpts) (*goofys.Config, error) {
	var res goofys.Config
	res.MountPoint = mpath
	// res.DebugFuse = true

	res.MountOptions = map[string]string{
		"allow_other": "",
	}

	uid, gid := goofys.MyUserAndGroup()
	res.Uid = uint32(uid)
	res.Gid = uint32(gid)
	if opts.UID > 0 {
		res.Uid = uint32(opts.UID)
	}
	if opts.GID > 0 {
		res.Gid = uint32(opts.GID)
	}

	dm, err := dirMode(opts.DirMode)
	if err != nil {
		return nil, err
	}
	res.DirMode = dm

	fm, err := fileMode(opts.FileMode)
	if err != nil {
		return nil, err
	}
	res.FileMode = fm

	return &res, nil
}

func dirMode(dm string) (os.FileMode, error) {
	if dm == "" {
		return 0755, nil
	}
	res, err := strconv.ParseUint(dm, 8, 32)
	if err != nil {
		return 0, err
	}
	return os.FileMode(res), nil
}

func fileMode(dm string) (os.FileMode, error) {
	if dm == "" {
		return 0644, nil
	}
	res, err := strconv.ParseUint(dm, 8, 32)
	if err != nil {
		return 0, err
	}
	return os.FileMode(res), nil
}

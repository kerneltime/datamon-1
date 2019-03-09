// Copyright © 2018 One Concern

package cmd

import (
	"log"

	"github.com/oneconcern/datamon/pkg/core"
	"github.com/oneconcern/datamon/pkg/storage/gcs"
	"github.com/oneconcern/datamon/pkg/storage/localfs"
	"github.com/spf13/afero"

	"github.com/spf13/cobra"
)

// Mount a read only view of a bundle
var mountBundleCmd = &cobra.Command{
	Use:   "mount",
	Short: "Mount a bundle",
	Long:  "Mount a readonly, non-interactive view of the entire data that is part of a bundle",
	Run: func(cmd *cobra.Command, args []string) {

		DieIfNotAccessible(bundleOptions.DataPath)

		metadataSource, err := gcs.New(repoParams.MetadataBucket, config.Credential)
		if err != nil {
			log.Fatalln(err)
		}
		blobStore, err := gcs.New(repoParams.BlobBucket, config.Credential)
		if err != nil {
			log.Fatalln(err)
		}
		consumableStore := localfs.New(afero.NewBasePathFs(afero.NewOsFs(), bundleOptions.DataPath))

		bd := core.NewBDescriptor()
		bundle := core.New(bd,
			core.Repo(repoParams.RepoName),
			core.BundleID(bundleID),
			core.BlobStore(blobStore),
			core.ConsumableStore(consumableStore),
			core.MetaStore(metadataSource),
		)

		fs, err := core.NewReadOnlyFS(bundle)
		if err != nil {
			log.Fatalln(err)
		}
		err = fs.MountReadOnly(bundleOptions.DataPath)
		if err != nil {
			log.Fatalln(err)
		}
	},
}

func init() {

	requiredFlags := []string{addRepoNameOptionFlag(mountBundleCmd)}
	addBucketNameFlag(mountBundleCmd)
	addBlobBucket(mountBundleCmd)
	requiredFlags = append(requiredFlags, addBundleFlag(mountBundleCmd))
	requiredFlags = append(requiredFlags, addDataPathFlag(mountBundleCmd))

	for _, flag := range requiredFlags {
		err := mountBundleCmd.MarkFlagRequired(flag)
		if err != nil {
			log.Fatalln(err)
		}
	}

	bundleCmd.AddCommand(mountBundleCmd)
}

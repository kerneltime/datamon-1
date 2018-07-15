// Copyright © 2018 One Concern

package cmd

import (
	"fmt"
	"io"
	"log"
	"log/syslog"
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "flexgoofys",
	Short: "A flexvolume plugin for goofys S3 FUSE",
	Long:  `A flexvolume plugin for goofys S3 Fuse`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	w, err := syslog.New(syslog.LOG_DEBUG, "")
	if err != nil {
		panic(err)
	}
	log.SetFlags(0)
	log.SetOutput(io.MultiWriter(os.Stderr, w))
}

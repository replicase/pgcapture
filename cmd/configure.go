package cmd

import (
	"context"
	"log"
	"time"

	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	AgentAddr           string
	AgentCommand        string
	ConfigPGConnURL     string
	ConfigPGReplURL     string
	ConfigPulsarURL     string
	ConfigPulsarTopic   string
	ConfigPGLogPath     string
	ConfigStartLSN      string
	ConfigPulsarTracker string
)

func init() {
	rootCmd.AddCommand(configure)
	configure.Flags().StringVarP(&AgentAddr, "AgentAddr", "", "", "connection addr to pgcapture agent")
	configure.Flags().StringVarP(&AgentCommand, "AgentCommand", "", "", "agent command to configure")
	configure.Flags().StringVarP(&ConfigPGConnURL, "PGConnURL", "", "", "connection url to install pg extension and fetching schema information")
	configure.Flags().StringVarP(&ConfigPGReplURL, "PGReplURL", "", "", "connection url to fetching logs from logical replication slot")
	configure.Flags().StringVarP(&ConfigPulsarURL, "PulsarURL", "", "", "connection url to sink pulsar cluster")
	configure.Flags().StringVarP(&ConfigPulsarTopic, "PulsarTopic", "", "", "the sink pulsar topic name and as well as the logical replication slot name")
	configure.Flags().StringVarP(&ConfigPGLogPath, "PGLogPath", "", "", "pg log path for finding last checkpoint lsn")
	configure.Flags().StringVarP(&ConfigStartLSN, "StartLSN", "", "", "the LSN position to start the pg2pulsar process, optional")
	configure.Flags().StringVarP(&ConfigPulsarTracker, "PulsarTracker", "", "", "the tracker type for pg2pulsar, optional")
	configure.MarkFlagRequired("AgentAddr")
	configure.MarkFlagRequired("AgentCommand")
	configure.MarkFlagRequired("PGConnURL")
	configure.MarkFlagRequired("PulsarURL")
	configure.MarkFlagRequired("PulsarTopic")
}

var configure = &cobra.Command{
	Use:   "configure",
	Short: "Poke agent's Configure endpoint repeatedly",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		params, err := structpb.NewStruct(map[string]interface{}{
			"Command":       AgentCommand,
			"PGConnURL":     ConfigPGConnURL,
			"PGReplURL":     ConfigPGReplURL,
			"PulsarURL":     ConfigPulsarURL,
			"PulsarTopic":   ConfigPulsarTopic,
			"PGLogPath":     ConfigPGLogPath,
			"StartLSN":      ConfigStartLSN,
			"PulsarTracker": ConfigPulsarTracker,
		})
		if err != nil {
			panic(err)
		}

		for {
			if err := poke(AgentAddr, params); err != nil {
				log.Println("Err", err)
			}

			time.Sleep(5 * time.Second)
		}
	},
}

func poke(addr string, params *structpb.Struct) error {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewAgentClient(conn)

	resp, err := client.Configure(context.Background(), &pb.AgentConfigRequest{Parameters: params})
	if err != nil {
		return err
	}

	log.Println("Success", resp.String())
	return nil
}

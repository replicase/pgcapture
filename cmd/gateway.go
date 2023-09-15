package main

import (
	"encoding/json"

	"github.com/replicase/pgcapture/pkg/dblog"
	"github.com/replicase/pgcapture/pkg/pb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	GatewayListenAddr string
	ControllerAddr    string
	ResolverConfig    string
)

func init() {
	rootCmd.AddCommand(gateway)
	gateway.Flags().StringVarP(&GatewayListenAddr, "ListenAddr", "", ":10001", "the tcp address for grpc server to listen")
	gateway.Flags().StringVarP(&ControllerAddr, "ControllerAddr", "", "127.0.0.1:10000", "the tcp address of dblog controller")
	gateway.Flags().StringVarP(&ResolverConfig, "ResolverConfig", "", "", "json config for resolving where is the source pulsar and where is the dump source")
	gateway.MarkFlagRequired("ResolverConfig")
}

var gateway = &cobra.Command{
	Use:   "gateway",
	Short: "grpc api for downstream to consume changes and dumps",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		resolverConfig := map[string]dblog.StaticAgentPulsarURIConfig{}
		if err = json.Unmarshal([]byte(ResolverConfig), &resolverConfig); err != nil {
			return err
		}
		controlConn, err := grpc.Dial(ControllerAddr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		gateway := &dblog.Gateway{
			SourceResolver: dblog.NewStaticAgentPulsarResolver(resolverConfig),
			DumpInfoPuller: &dblog.GRPCDumpInfoPuller{Client: pb.NewDBLogControllerClient(controlConn)},
		}
		return serveGRPC(&pb.DBLogGateway_ServiceDesc, GatewayListenAddr, gateway, func() {})
	},
}

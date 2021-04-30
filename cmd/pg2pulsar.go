package cmd

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rueian/pgcapture/pkg/sink"
	"github.com/rueian/pgcapture/pkg/source"
	"github.com/spf13/cobra"
)

var (
	SourcePGConnURL string
	SourcePGReplURL string
	SinkPulsarURL   string
	SinkPulsarTopic string
)

func init() {
	rootCmd.AddCommand(pg2pulsar)
	pg2pulsar.Flags().StringVarP(&SourcePGConnURL, "PGConnURL", "", "", "connection url to install pg extension and fetching schema information")
	pg2pulsar.Flags().StringVarP(&SourcePGReplURL, "PGReplURL", "", "", "connection url to fetching logs from logical replication slot")
	pg2pulsar.Flags().StringVarP(&SinkPulsarURL, "PulsarURL", "", "", "connection url to sink pulsar cluster")
	pg2pulsar.Flags().StringVarP(&SinkPulsarTopic, "PulsarTopic", "", "", "the sink pulsar topic name and as well as the logical replication slot name")
	pg2pulsar.MarkFlagRequired("PGConnURL")
	pg2pulsar.MarkFlagRequired("PGReplURL")
	pg2pulsar.MarkFlagRequired("PulsarURL")
	pg2pulsar.MarkFlagRequired("PulsarTopic")
}

var pg2pulsar = &cobra.Command{
	Use:   "pg2pulsar",
	Short: "Capture logical replication logs to a Pulsar Topic from a PostgreSQL logical replication slot",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		pgSrc := &source.PGXSource{SetupConnStr: SourcePGConnURL, ReplConnStr: SourcePGReplURL, ReplSlot: trimSlot(SinkPulsarTopic), CreateSlot: true}
		pulsarSink := &sink.PulsarSink{PulsarOption: pulsar.ClientOptions{URL: SinkPulsarURL}, PulsarTopic: SinkPulsarTopic}
		return sourceToSink(pgSrc, pulsarSink)
	},
}

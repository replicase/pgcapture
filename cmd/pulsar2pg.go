package cmd

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rueian/pgcapture/pkg/sink"
	"github.com/rueian/pgcapture/pkg/source"
	"github.com/spf13/cobra"
)

var (
	SinkPGConnURL     string
	SourcePulsarURL   string
	SourcePulsarTopic string
)

func init() {
	rootCmd.AddCommand(pulsar2pg)
	pulsar2pg.Flags().StringVarP(&SinkPGConnURL, "PGConnURL", "", "", "connection url to install pg extension and fetching schema information")
	pulsar2pg.Flags().StringVarP(&SourcePulsarURL, "PulsarURL", "", "", "connection url to sink pulsar cluster")
	pulsar2pg.Flags().StringVarP(&SourcePulsarTopic, "PulsarTopic", "", "", "the sink pulsar topic name and as well as the logical replication slot name")
	pulsar2pg.MarkFlagRequired("PGConnURL")
	pulsar2pg.MarkFlagRequired("PulsarURL")
	pulsar2pg.MarkFlagRequired("PulsarTopic")
}

var pulsar2pg = &cobra.Command{
	Use:   "pulsar2pg",
	Short: "Apply logical replication logs to a PostgreSQL from a Pulsar Topic",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		pulsarSrc := &source.PulsarReaderSource{PulsarOption: pulsar.ClientOptions{URL: SourcePulsarURL}, PulsarTopic: SourcePulsarTopic}
		pgSink := &sink.PGXSink{ConnStr: SinkPGConnURL, SourceID: SourcePulsarTopic}
		return sourceToSink(pulsarSrc, pgSink)
	},
}

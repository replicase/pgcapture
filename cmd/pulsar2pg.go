package main

import (
	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/replicase/pgcapture/pkg/sink"
	"github.com/replicase/pgcapture/pkg/source"
	"github.com/spf13/cobra"
)

var (
	SinkPGConnURL       string
	SinkPGLogPath       string
	SourcePulsarURL     string
	SourcePulsarTopic   string
	SourceReaderPrefix  string
	Renice              int64
	BatchTXSize         int
)

func init() {
	rootCmd.AddCommand(pulsar2pg)
	pulsar2pg.Flags().StringVarP(&SinkPGConnURL, "PGConnURL", "", "", "connection url to install pg extension and fetching schema information")
	pulsar2pg.Flags().StringVarP(&SinkPGLogPath, "PGLogPath", "", "", "pg log path for finding last checkpoint lsn")
	pulsar2pg.Flags().StringVarP(&SourcePulsarURL, "PulsarURL", "", "", "connection url to sink pulsar cluster")
	pulsar2pg.Flags().StringVarP(&SourcePulsarTopic, "PulsarTopic", "", "", "the sink pulsar topic name and as well as the logical replication slot name")
	pulsar2pg.Flags().StringVarP(&SourceReaderPrefix, "ReaderPrefix", "", "", "subscription role prefix for pulsar reader")
	pulsar2pg.Flags().Int64VarP(&Renice, "Renice", "", -10, "try renice the sink pg process")
	pulsar2pg.Flags().IntVarP(&BatchTXSize, "BatchTxSize", "", 100, "the max number of tx in a pipeline")
	pulsar2pg.MarkFlagRequired("PGConnURL")
	pulsar2pg.MarkFlagRequired("PulsarURL")
	pulsar2pg.MarkFlagRequired("PulsarTopic")
}

var pulsar2pg = &cobra.Command{
	Use:   "pulsar2pg",
	Short: "Apply logical replication logs to a PostgreSQL from a Pulsar Topic",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		pgSink := &sink.PGXSink{ConnStr: SinkPGConnURL, SourceID: trimSlot(SourcePulsarTopic), Renice: Renice, LogReader: nil, BatchTXSize: BatchTXSize}
		if SinkPGLogPath != "" {
			pgLog, err := os.Open(SinkPGLogPath)
			if err != nil {
				return err
			}
			defer pgLog.Close()
			pgSink.LogReader = pgLog
		}
		pulsarSrc := &source.PulsarReaderSource{
			PulsarOption: pulsar.ClientOptions{URL: SourcePulsarURL},
			PulsarTopic:  SourcePulsarTopic,
			ReaderPrefix: SourceReaderPrefix,
		}
		return sourceToSink(pulsarSrc, pgSink)
	},
}

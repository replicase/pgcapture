package cursor

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/streamnative/pulsarctl/pkg/pulsar"
	"github.com/streamnative/pulsarctl/pkg/pulsar/common"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
)

func NewAdminClient() (pulsar.Client, error) {
	return pulsar.New(&common.Config{WebServiceURL: "http://localhost:8080"})
}

func nextMessageID(cursor utils.CursorStats) string {
	comps := strings.Split(cursor.MarkDeletePosition, ":")
	e, _ := strconv.ParseInt(comps[1], 10, 64)
	return comps[0] + ":" + strconv.FormatInt(e+1, 10)
}

func CheckSubscriptionCursor(client pulsar.Client, topicName string, subscriptionName string) (string, error) {
	topic, err := utils.GetTopicName("public/default/" + topicName)
	if err != nil {
		return "", err
	}

	stats, err := client.Topics().GetInternalStats(*topic)
	if err != nil {
		return "", err
	}

	for sub, c := range stats.Cursors {
		if sub == subscriptionName {
			return nextMessageID(c), nil
		}
	}
	return "", errors.New("subscription not found")
}

func GetCheckpointByMessageID(topicName string, messageID string) (cp Checkpoint, err error) {
	mid, err := utils.ParseMessageID(messageID)
	if err != nil {
		return cp, err
	}

	resp, err := http.Get(
		"http://localhost:8080/admin/v2/persistent/public/default/" +
			topicName + "/ledger/" +
			strconv.FormatInt(mid.LedgerID, 10) +
			"/entry/" +
			strconv.FormatInt(mid.EntryID, 10))
	if err != nil {
		return cp, err
	}
	defer io.Copy(ioutil.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		return cp, errors.New(fmt.Sprintf("error response:\n %s", string(b)))
	}

	buf32 := make([]byte, 4)
	if _, err := io.ReadFull(resp.Body, buf32); err != nil {
		return cp, err
	}

	metaSize := binary.BigEndian.Uint32(buf32)
	metaBuf := make([]byte, metaSize)
	if _, err := io.ReadFull(resp.Body, metaBuf); err != nil {
		return cp, err
	}

	meta := new(utils.SingleMessageMetadata)
	if err := proto.Unmarshal(metaBuf, meta); err != nil {
		return cp, err
	}

	msgKey := *meta.PartitionKey
	err = cp.FromKey(msgKey)
	return cp, err
}

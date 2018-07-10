package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/niwho/utils"
)

func main() {
	utils.NewKafkaConsumer([]string{"10.66.130.240:9292", "10.66.130.207:9292", "10.66.130.245:9292"}, "user_event", "test_kafka", 1, hand{}).Run()
	//utils.NewKafkaConsumer([]string{"10.116.76.148:9292", "10.116.76.193:9292"}, "miveshow_error_log", "test_kafka", 1, hand{}).Run()
	//utils.NewKafkaConsumer([]string{"10.116.76.148:9292", "10.116.76.193:9292"}, "miveshow_bd_alert", "test_kafka", 1, hand{}).Run()
	select {}
}

type hand struct {
}

// {"event_name": "http_api_push_token", "timestamp": 1531211604.646207, "params": {"token": "dVFbuSD5pRw:APA91bGHCkSXptwaCo7bfSOIboWs0RM84tZu8hB-OWlSMXCYo3Ok-CxkMZEJEVos59SSnvsIYzVP4XqykITxRzSc5alZxKIqqbPe5o5k8wKu7rHng6PfLEyTmh1jCYr1T3MhEEp2bOIPvev-th_f9HMA2t2bRppBxw", "enabled": 1}, "user": {"uid": "u2714661524656517100001252"}, "header": {"did": "mgd_5ae068bfee20dc03750c7133", "os": 2, "ver": "165", "app_id": 10}}
func (hand) Consume(msg *sarama.ConsumerMessage) error {

	if strings.Contains(string(msg.Value), "token_submit") || true {
		pushData := map[string]interface{}{}
		json.Unmarshal((msg.Value), &pushData)
		fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
	}
	return nil
}

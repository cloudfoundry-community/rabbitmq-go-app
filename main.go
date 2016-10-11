package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/cloudfoundry-community/go-cfenv"
	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	Name       string `json:"name"`
	Vhost      string `json:"vhost"`
	Durable    bool   `json:"durable"`
	AutoDelete bool   `json:"auto_delete"`
	Exclusive  bool   `json:"exclusive"`
	Arguments  struct {
	} `json:"arguments"`
	Node               string        `json:"node"`
	ConsumerDetails    []interface{} `json:"consumer_details"`
	Deliveries         []interface{} `json:"deliveries"`
	Incoming           []interface{} `json:"incoming"`
	BackingQueueStatus struct {
		Mode              string        `json:"mode"`
		Q1                int           `json:"q1"`
		Q2                int           `json:"q2"`
		Delta             []interface{} `json:"delta"`
		Q3                int           `json:"q3"`
		Q4                int           `json:"q4"`
		Len               int           `json:"len"`
		TargetRAMCount    string        `json:"target_ram_count"`
		NextSeqID         int           `json:"next_seq_id"`
		AvgIngressRate    float64       `json:"avg_ingress_rate"`
		AvgEgressRate     float64       `json:"avg_egress_rate"`
		AvgAckIngressRate float64       `json:"avg_ack_ingress_rate"`
		AvgAckEgressRate  float64       `json:"avg_ack_egress_rate"`
		MirrorSeen        int           `json:"mirror_seen"`
		MirrorSenders     int           `json:"mirror_senders"`
	} `json:"backing_queue_status"`
	DiskWrites                 int         `json:"disk_writes"`
	DiskReads                  int         `json:"disk_reads"`
	HeadMessageTimestamp       interface{} `json:"head_message_timestamp"`
	MessageBytesPersistent     int         `json:"message_bytes_persistent"`
	MessageBytesRAM            int         `json:"message_bytes_ram"`
	MessageBytesUnacknowledged int         `json:"message_bytes_unacknowledged"`
	MessageBytesReady          int         `json:"message_bytes_ready"`
	MessageBytes               int         `json:"message_bytes"`
	MessagesPersistent         int         `json:"messages_persistent"`
	MessagesUnacknowledgedRAM  int         `json:"messages_unacknowledged_ram"`
	MessagesReadyRAM           int         `json:"messages_ready_ram"`
	MessagesRAM                int         `json:"messages_ram"`
	GarbageCollection          struct {
		MinBinVheapSize int `json:"min_bin_vheap_size"`
		MinHeapSize     int `json:"min_heap_size"`
		FullsweepAfter  int `json:"fullsweep_after"`
		MinorGcs        int `json:"minor_gcs"`
	} `json:"garbage_collection"`
	Reductions                    int         `json:"reductions"`
	State                         string      `json:"state"`
	RecoverableSlaves             interface{} `json:"recoverable_slaves"`
	SynchronisedSlaveNodes        []string    `json:"synchronised_slave_nodes"`
	SlaveNodes                    []string    `json:"slave_nodes"`
	Consumers                     int         `json:"consumers"`
	ExclusiveConsumerTag          interface{} `json:"exclusive_consumer_tag"`
	Policy                        string      `json:"policy"`
	ConsumerUtilisation           interface{} `json:"consumer_utilisation"`
	IdleSince                     string      `json:"idle_since"`
	MessagesUnacknowledgedDetails struct {
		Rate float64 `json:"rate"`
	} `json:"messages_unacknowledged_details"`
	MessagesUnacknowledged int `json:"messages_unacknowledged"`
	MessagesReadyDetails   struct {
		Rate float64 `json:"rate"`
	} `json:"messages_ready_details"`
	MessagesReady   int `json:"messages_ready"`
	MessagesDetails struct {
		Rate float64 `json:"rate"`
	} `json:"messages_details"`
	Messages          int `json:"messages"`
	ReductionsDetails struct {
		Rate float64 `json:"rate"`
	} `json:"reductions_details"`
	MessageStats struct {
		Deliver        int `json:"deliver"`
		DeliverDetails struct {
			Rate float64 `json:"rate"`
		} `json:"deliver_details"`
		DeliverNoAck        int `json:"deliver_no_ack"`
		DeliverNoAckDetails struct {
			Rate float64 `json:"rate"`
		} `json:"deliver_no_ack_details"`
		Get        int `json:"get"`
		GetDetails struct {
			Rate float64 `json:"rate"`
		} `json:"get_details"`
		GetNoAck        int `json:"get_no_ack"`
		GetNoAckDetails struct {
			Rate float64 `json:"rate"`
		} `json:"get_no_ack_details"`
		Publish        int `json:"publish"`
		PublishDetails struct {
			Rate float64 `json:"rate"`
		} `json:"publish_details"`
		PublishIn        int `json:"publish_in"`
		PublishInDetails struct {
			Rate float64 `json:"rate"`
		} `json:"publish_in_details"`
		PublishOut        int `json:"publish_out"`
		PublishOutDetails struct {
			Rate float64 `json:"rate"`
		} `json:"publish_out_details"`
		Ack        int `json:"ack"`
		AckDetails struct {
			Rate float64 `json:"rate"`
		} `json:"ack_details"`
		DeliverGet        int `json:"deliver_get"`
		DeliverGetDetails struct {
			Rate float64 `json:"rate"`
		} `json:"deliver_get_details"`
		Confirm        int `json:"confirm"`
		ConfirmDetails struct {
			Rate float64 `json:"rate"`
		} `json:"confirm_details"`
		ReturnUnroutable        int `json:"return_unroutable"`
		ReturnUnroutableDetails struct {
			Rate float64 `json:"rate"`
		} `json:"return_unroutable_details"`
		Redeliver        int `json:"redeliver"`
		RedeliverDetails struct {
			Rate float64 `json:"rate"`
		} `json:"redeliver_details"`
	} `json:"message_stats"`
	Memory int `json:"memory"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func sendMessage(body string, uri string) {
	conn, err := amqp.Dial(uri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"golang", // name
		false,    // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")

}

func receiveMessage(uri string) <-chan amqp.Delivery {
	conn, err := amqp.Dial(uri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"golang", // name
		false,    // durable
		false,    // delete when usused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	ch.Close()
	conn.Close()
	return msgs
}

func checkQueueSize(uri string) {
	resp, err := http.Get(uri)
	failOnError(err, "Failed to curl")

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	var temp []RabbitMQ
	json.Unmarshal(body, &temp)
	currentSize := temp[1].MessageBytes
	if currentSize >= 9500000 {
		log.Printf("Queue is full! Messages will be dropped when publishing to full queue")
	}
}

func main() {
	http.HandleFunc("/", testRabbit)
	fmt.Println("listening...")
	err := http.ListenAndServe(":"+os.Getenv("PORT"), nil)
	if err != nil {
		panic(err)
	}
}

func testRabbit(res http.ResponseWriter, req *http.Request) {
	//Getting Uri for RabbitMQ
	appEnv, _ := cfenv.Current()
	service, _ := appEnv.Services.WithName(os.Getenv("SERVICE_NAME"))
	protocols := service.Credentials["protocols"]
	uri := protocols.(map[string]interface{})["amqp"].(map[string]interface{})["uri"].(string)
	reqURL := service.Credentials["http_api_uri"].(string) + "queues/" + service.Credentials["vhost"].(string)

	checkQueueSize(reqURL)
	sendMessage("OK", uri)
	msgs := receiveMessage(uri)
	//printing messages
	for d := range msgs {
		fmt.Fprintln(res, fmt.Sprintf("%s", d.Body))
	}

}

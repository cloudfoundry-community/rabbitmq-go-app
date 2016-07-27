package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/cloudfoundry-community/go-cfenv"
	"github.com/streadway/amqp"
)

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

	sendMessage("OK", uri)
	msgs := receiveMessage(uri)

	//printing messages
	for d := range msgs {
		fmt.Fprintln(res, fmt.Sprintf("%s", d.Body))
	}

}

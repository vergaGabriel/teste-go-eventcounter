package main


import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Estrutura da mensagem
type Message struct {
	ID string `json:"id"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	// Pega a URL do Rabbit via env (ou usa padrão)
	rabbitURL := os.Getenv("RABBIT_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}

	// Conexão
	conn, err := amqp.Dial(rabbitURL)
	failOnError(err, "Falha ao conectar no RabbitMQ")
	defer conn.Close()

	// Canal
	ch, err := conn.Channel()
	failOnError(err, "Falha ao abrir canal")
	defer ch.Close()

	// Declara a fila (precisa existir)
	q, err := ch.QueueDeclare(
		"eventcountertest", // nome
		true,               // durável
		false,              // auto-delete
		false,              // exclusiva
		false,              // no-wait
		nil,                // argumentos
	)
	failOnError(err, "Falha ao declarar fila")

	// Consome mensagens
	msgs, err := ch.Consume(
		q.Name, // fila
		"",     // consumer
		true,   // auto-ack (pode mudar para false se quiser confirmar manualmente)
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Falha ao registrar consumidor")

	fmt.Println(" [*] Esperando mensagens... CTRL+C para sair")

	// Loop de mensagens
	forever := make(chan bool)

	go func() {
		for d := range msgs {
			// Routing key (contém userID e tipo do evento)
			routingKey := d.RoutingKey

			// Decodifica o corpo JSON
			var m Message
			if err := json.Unmarshal(d.Body, &m); err != nil {
				log.Printf("Erro ao decodificar mensagem: %v", err)
				continue
			}

			fmt.Printf("Mensagem recebida | ID: %s | RoutingKey: %s\n", m.ID, routingKey)
		}
	}()

	<-forever
}
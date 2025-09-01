package consumer


import (
	"encoding/json"
	"context"
	"log"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	eventcounter "github.com/reb-felipe/eventcounter/pkg"
)

const (
	queueName   = "eventcountertest"
	consumerTag = "eventcountertest-consumer"
)

type Message struct {
	ID string `json:"id"`
}

type RabbitConn struct {
	conn 	*amqp.Connection
	channel *amqp.Channel
	event 	eventcounter.Consumer
}

func ConnectRabbit(rabbitURL string, event eventcounter.Consumer) (*RabbitConn, error) {
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &RabbitConn{
		conn, 
		ch, 
		event,
	}, nil
}

func (c *RabbitConn) ConsumeMessages(ctx context.Context) error {
	msgs, err := c.channel.Consume(
		queueName,
		consumerTag,
		true,  
		false, 
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	processed := make(map[string]bool) 

	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-msgs:
			if !ok {
				return nil
			}
			c.ProcessMessage(ctx, d, processed)
		case <-time.After(5 * time.Second):
			log.Println("Nenhuma mensagem recebida em 5 segundos, finalizando.")
			return nil
		}
		
	}
}

func (c *RabbitConn) ProcessMessage(ctx context.Context, d amqp.Delivery, processed map[string]bool) {
	var m Message
	if err := json.Unmarshal(d.Body, &m); err != nil {
		log.Printf("Erro ao decodificar mensagem: %v", err)
		return
	}

	// Verifica duplicado
	if processed[m.ID] {
		return
	}
	processed[m.ID] = true

	// Extrai userID e eventType da routing key
	parts := strings.Split(d.RoutingKey, ".")
	if len(parts) != 3 {
		log.Printf("Routing key inválida: %s", d.RoutingKey)
		return
	}
	userID := parts[0]
	eventType := parts[2]

	log.Printf("Mensagem processada: userID=%s, eventType=%s", userID, eventType)

	msgCtx := context.WithValue(ctx, "messageID", m.ID)

	var err error
	switch eventcounter.EventType(eventType) {
	case eventcounter.EventCreated:
		err = c.event.Created(msgCtx, userID)
	case eventcounter.EventUpdated:
		err = c.event.Updated(msgCtx, userID)
	case eventcounter.EventDeleted:
		err = c.event.Deleted(msgCtx, userID)
	default:
		log.Printf("Tipo de evento desconhecido: %s", eventType)
		return
	}

	if err != nil {
		log.Printf("Mensagem para usuário '%s' não processada: %v", userID, err)
	}
}

func (c *RabbitConn) Close() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
	log.Println("Conexão com RabbitMQ fechada.")
}
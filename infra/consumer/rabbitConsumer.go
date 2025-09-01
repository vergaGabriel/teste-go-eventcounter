package consumer


import (
	"encoding/json"
	"context"
	"log"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	eventcounter "github.com/reb-felipe/eventcounter/pkg"
	service "github.com/reb-felipe/eventcounter/infra/service"
)

const (
	queueName   = "eventcountertest"
	consumerTag = "eventcountertest-consumer"
)

type Message struct {
	ID string `json:"id"`
}

type RabbitConsumer struct {
	Conn 	*amqp.Connection
	Channel *amqp.Channel
	Event 	eventcounter.Consumer
}

func ConnectRabbit(rabbitURL string, event eventcounter.Consumer) (*RabbitConsumer, error) {
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &RabbitConsumer{
		conn, 
		ch, 
		event,
	}, nil
}

func (c *RabbitConsumer) ConsumeMessages(ctx context.Context, shutdownService *service.ShutdownService) error {
	msgs, err := c.Channel.Consume(
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

	// Canais por tipo de evento
	createdCh := make(chan string)
	updatedCh := make(chan string)
	deletedCh := make(chan string)

	var wg sync.WaitGroup

	// Goroutines consumidoras
	wg.Add(3)
	go c.consumeChannel(ctx, &wg, createdCh, c.Event.Created)
	go c.consumeChannel(ctx, &wg, updatedCh, c.Event.Updated)
	go c.consumeChannel(ctx, &wg, deletedCh, c.Event.Deleted)

	inactivity := time.NewTimer(shutdownService.Timeout)
    defer inactivity.Stop()

	// Loop principal de consumo
	for {
		select {
		case <-ctx.Done():
			close(createdCh)
			close(updatedCh)
			close(deletedCh)
			wg.Wait()
			return nil
		case d, ok := <-msgs:
			if !ok {
				close(createdCh)
				close(updatedCh)
				close(deletedCh)
				wg.Wait()
				return nil
			}

			// Reset do timer porque chegou mensagem
            if !inactivity.Stop() {
                <-inactivity.C
            }
            inactivity.Reset(shutdownService.Timeout)

            c.ProcessMessage(d, processed, createdCh, updatedCh, deletedCh, shutdownService)

        case <-inactivity.C:
            log.Println("Timeout de inatividade atingido. Encerrando consumer...")
            close(createdCh)
            close(updatedCh)
            close(deletedCh)
            wg.Wait()
            return nil
		}
	}
}

func (c *RabbitConsumer) ProcessMessage(
	d amqp.Delivery,
	processed map[string]bool,
	createdCh, updatedCh, deletedCh chan string,
	shutdownService *service.ShutdownService,
) {
	var m Message
	if err := json.Unmarshal(d.Body, &m); err != nil {
		log.Printf("Erro ao decodificar mensagem: %v", err)
		return
	}

	if processed[m.ID] {
		return
	}
	processed[m.ID] = true

	parts := strings.Split(d.RoutingKey, ".")
	if len(parts) != 3 {
		log.Printf("Routing key inválida: %s", d.RoutingKey)
		return
	}

	userID := parts[0]
	eventType := parts[2]

	// Envia para canal correto
	switch eventType {
	case "created":
		createdCh <- userID
	case "updated":
		updatedCh <- userID
	case "deleted":
		deletedCh <- userID
	default:
		log.Printf("Tipo de evento desconhecido: %s", eventType)
		return
	}

	log.Printf("Mensagem enviada para canal: userID=%s, eventType=%s", userID, eventType)

	if shutdownService != nil {
		shutdownService.UpdateLastMessage()
	}
}

func (c *RabbitConsumer) consumeChannel(ctx context.Context, wg *sync.WaitGroup, ch <-chan string, handler func(context.Context, string) error) {
	defer wg.Done()
	for userID := range ch {
		//handler(ctx, userID)

		log.Printf(">> Worker %p começou a processar %s", ch, userID)

        // simula processamento pesado
        time.Sleep(2 * time.Second)

        if err := handler(ctx, userID); err != nil {
            log.Printf("Erro ao processar %s: %v", userID, err)
        }

        log.Printf(">> Worker %p terminou de processar %s", ch, userID)
	}
}

func (rc *RabbitConsumer) Close() {
	if rc.Channel != nil {
		rc.Channel.Close()
	}
	if rc.Conn != nil {
		rc.Conn.Close()
	}
	log.Println("Conexão com RabbitMQ fechada.")
}
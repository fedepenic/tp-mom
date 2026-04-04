package exchange

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

var _ m.Middleware = (*ExchangeMiddleware)(nil)

type ExchangeMiddleware struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	exchange  string
	keys      []string
	queueName string
	stopCh    chan struct{}
}

func NewExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	connStr := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(connStr)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", m.ErrMessageMiddlewareDisconnected, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("%w: %v", m.ErrMessageMiddlewareDisconnected, err)
	}

	if err := ch.ExchangeDeclare(
		exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("%w: %v", m.ErrMessageMiddlewareMessage, err)
	}

	q, err := ch.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("%w: %v", m.ErrMessageMiddlewareMessage, err)
	}

	for _, key := range keys {
		if err := ch.QueueBind(q.Name, key, exchange, false, nil); err != nil {
			ch.Close()
			conn.Close()
			return nil, fmt.Errorf("%w: %v", m.ErrMessageMiddlewareMessage, err)
		}
	}

	return &ExchangeMiddleware{
		conn:      conn,
		ch:        ch,
		exchange:  exchange,
		keys:      keys,
		queueName: q.Name,
		stopCh:    make(chan struct{}),
	}, nil
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	deliveries, err := em.ch.Consume(
		em.queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("%w: %v", m.ErrMessageMiddlewareDisconnected, err)
	}

	for {
		select {
		case <-em.stopCh:
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return m.ErrMessageMiddlewareDisconnected
			}
			msg := m.Message{Body: string(delivery.Body)}
			ack := func() { delivery.Ack(false) }
			nack := func() { delivery.Nack(false, true) }
			callbackFunc(msg, ack, nack)
		}
	}
}

func (em *ExchangeMiddleware) StopConsuming() error {
	select {
	case <-em.stopCh:
	default:
		close(em.stopCh)
	}
	return nil
}

func (em *ExchangeMiddleware) Send(msg m.Message) error {
	if len(em.keys) == 0 {
		return m.ErrMessageMiddlewareMessage
	}
	for _, key := range em.keys {
		if err := em.ch.PublishWithContext(
			context.Background(),
			em.exchange,
			key,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			},
		); err != nil {
			return fmt.Errorf("%w: %v", m.ErrMessageMiddlewareMessage, err)
		}
	}
	return nil
}

func (em *ExchangeMiddleware) Close() error {
	if err := em.ch.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}
	if err := em.conn.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}

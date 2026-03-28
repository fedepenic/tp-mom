package factory

import (
	ex "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/exchange"
	q "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/queue"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return q.NewQueueMiddleware(queueName, connectionSettings)
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return ex.NewExchangeMiddleware(exchange, keys, connectionSettings)
}

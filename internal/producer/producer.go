package producer

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

type producer struct {
	conn *kafka.Conn
}

func NewProducer(brokerAddr, topic string, partition int) (*producer, error) {
	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("producer.go/NewProducer Error First connection %v:", err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return nil, fmt.Errorf("producer.go/NewProducer Error get controller %v:", err)
	}

	connTopicPart, err := kafka.DialLeader(
		context.Background(),
		"tcp",
		net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)),
		topic,
		partition,
	)
	if err != nil {
		return nil, fmt.Errorf("producer.go/NewProducer Error get Concret Topic Partition connection: %v", err)
	}
	return &producer{
		conn: connTopicPart,
	}, err
}

func (p *producer) SendMessages(messages []kafka.Message) error {
	_, err := p.conn.WriteMessages(messages...)
	if err != nil {
		return fmt.Errorf("producer.go/SendMessage Error write message:%v", err)
	}
	return nil
}

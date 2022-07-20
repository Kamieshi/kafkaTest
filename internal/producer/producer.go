package producer

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type producer struct {
	conn *kafka.Conn
}

func NewProducer(brokerAddr, topic string, partition int) (*producer, error) {

	connTopicPart, err := kafka.DialLeader(
		context.Background(),
		"tcp",
		brokerAddr,
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

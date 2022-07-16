package consumer

import (
	"KafkaWriterReader/internal/models"
	"KafkaWriterReader/internal/repository"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
	"time"
)

type StreamConsumer struct {
	hostPort  string
	topic     string
	partition int
	Conn      *kafka.Conn
	batch     *kafka.Batch
}

func NewStreamConsumer(brokerAddr, topic string, partition int, offset int64) (*StreamConsumer, error) {
	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("producer.go/NewStreamConsumer Error First connection %v:", err)
	}
	controller, err := conn.Controller()
	if err != nil {
		return nil, fmt.Errorf("producer.go/NewStreamConsumer Error get controller %v:", err)
	}

	connTopicPart, err := kafka.DialLeader(
		context.Background(),
		"tcp",
		net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)),
		topic,
		partition,
	)
	if err != nil {
		return nil, fmt.Errorf("producer.go/NewStreamConsumer Error get Concret Topic Partition connection: %v", err)
	}
	_, err = connTopicPart.Seek(offset, 0)
	if err != nil {
		return nil, fmt.Errorf("producer.go/NewStreamConsumer Error Set Offset: %v", err)
	}

	connTopicPart.SetReadDeadline(time.Now().Add(1 * time.Minute))
	batch := connTopicPart.ReadBatchWith(kafka.ReadBatchConfig{
		MinBytes:       2e3,
		MaxBytes:       2e6,
		IsolationLevel: kafka.IsolationLevel(0),
		MaxWait:        1 * time.Minute,
	})

	return &StreamConsumer{
		Conn:      connTopicPart,
		batch:     batch,
		hostPort:  brokerAddr,
		topic:     topic,
		partition: partition,
	}, err
}

func (s *StreamConsumer) ReInitBatch(newOffset int64) error {

	s.batch.Close()
	s.Conn.Close()
	newStreamer, err := NewStreamConsumer(s.hostPort, s.topic, s.partition, newOffset)
	if err != nil {
		return fmt.Errorf("producer.go/ReInitBatch Create new Streamer: %v", err)
	}

	s.Conn = newStreamer.Conn
	s.batch = newStreamer.batch
	return nil
}

func (s *StreamConsumer) ListenAndWriteToRep(ctx context.Context, rep repository.MessageRepository) {
	batchBuffer := make([]byte, 10e3) // 10KB max per message
	bt := pgx.Batch{}
	var mess models.Message
	for {
		n, err := s.batch.Read(batchBuffer)

		if err != nil {
			err = s.ReInitBatch(s.batch.Offset())
			if err != nil {
				log.Fatal().Err(err)
				break
			}
			continue
		}
		//fmt.Println(string(batchBuffer[:n]))
		err = json.Unmarshal(batchBuffer[:n], &mess)
		if err != nil {
			break
		}
		bt.Queue("INSERT INTO messages(id,pay_load) values ($1,$2)", mess.ID, mess.PayLoad)
		if bt.Len() > 100 {
			err := rep.BatchQuery(ctx, &bt)
			if err != nil {
				log.Info().Err(err)
			}
			bt = pgx.Batch{}
		}
	}

}

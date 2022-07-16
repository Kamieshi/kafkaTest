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

type SimpleReader struct {
	repMessage repository.MessageRepository
	read       *kafka.Reader
}

func NewSimpleReader(rep *repository.MessageRepository, brokers []string, groupId, topic string) *SimpleReader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupId,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	return &SimpleReader{
		repMessage: *rep,
		read:       r,
	}
}

func (r *SimpleReader) ListenMessageAndWriteToDB(ctx context.Context) {
	var mess models.Message
	for {
		m, err := r.read.ReadMessage(context.Background())
		if err != nil {
			break
		}
		err = json.Unmarshal(m.Value, &mess)
		if err != nil {
			log.Info().Err(err)
		}
		err = r.repMessage.Write(ctx, &mess)
		if err != nil {
			log.Info().Err(err)
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}

type BatchReader struct {
	repMessage repository.MessageRepository
	Conn       *kafka.Conn
}

func NewBatchReader(repMess repository.MessageRepository, brokerAddr, topic string, partition int) (*BatchReader, error) {
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
	return &BatchReader{
		Conn:       connTopicPart,
		repMessage: repMess,
	}, err
}

func (r *BatchReader) ListenMessageAndWriteToDB(ctx context.Context) {
	r.Conn.SetReadDeadline(time.Now().Add(1 * time.Minute))
	r.Conn.Seek(5, 2)
	batch := r.Conn.ReadBatchWith(kafka.ReadBatchConfig{
		MinBytes:       2e3,
		MaxBytes:       2e6,
		IsolationLevel: kafka.IsolationLevel(0),
		MaxWait:        1 * time.Minute,
	})

	defer func() {
		err := batch.Close()
		log.Info().Err(err)
	}()
	batchBuffer := make([]byte, 10e3) // 10KB max per message
	bt := pgx.Batch{}
	var mess models.Message
	for {
		n, err := batch.Read(batchBuffer)
		fmt.Println(string(batchBuffer[:n]))
		if err != nil {
			fmt.Println(batch.Offset())
			continue
		}
		err = json.Unmarshal(batchBuffer[:n], &mess)
		if err != nil {
			break

		}
		bt.Queue("INSERT INTO messages(id,pay_load) values ($1,$2)", mess.ID, mess.PayLoad)
	}
	r.repMessage.BatchQuery(ctx, &bt)

	if err := batch.Close(); err != nil {
		log.Fatal().Err(err)
	}

	if err := r.Conn.Close(); err != nil {
		log.Fatal().Err(err)
	}

}

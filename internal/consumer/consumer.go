package consumer

import (
	"KafkaWriterReader/internal/models"
	"KafkaWriterReader/internal/repository"
	"context"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

type EasyReader struct {
	repMessage repository.MessageRepository
	read       *kafka.Reader
}

func NewReader(rep *repository.MessageRepository, brokers []string, groupId, topic string) *EasyReader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupId,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	return &EasyReader{
		repMessage: *rep,
		read:       r,
	}
}

func (r *EasyReader) ListenMessageAndWriteToDB(ctx context.Context) {
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

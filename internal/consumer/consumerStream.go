package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"

	"KafkaWriterReader/internal/models"
	"KafkaWriterReader/internal/repository"
)

type StreamConsumer struct {
	hostPort  string
	topic     string
	partition int
	Conn      *kafka.Conn
	batch     *kafka.Batch
}

func NewStreamConsumer(brokerAddr, topic string, partition int, offset int64) (*StreamConsumer, error) {
	connTopicPart, err := kafka.DialLeader(
		context.Background(),
		"tcp",
		brokerAddr,
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
	connTopicPart.SetReadDeadline(time.Now().Add(20 * time.Second))
	batch := connTopicPart.ReadBatchWith(kafka.ReadBatchConfig{
		MinBytes:       2e3,
		MaxBytes:       2e6,
		IsolationLevel: kafka.IsolationLevel(1),
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

func GetStreamConsumers(ctx context.Context, brokerAddr, topic string) ([]*StreamConsumer, error) {
	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("consumerStream.go/GetAddrPartitions Make first connection with cluster:%v", err)
	}
	topicPartitions, err := conn.ReadPartitions("test_topic")
	if err != nil {
		return nil, fmt.Errorf("consumerStream.go/GetAddrPartitions Get all partition for topic %s :%v", topic, err)
	}
	var streamConsumers = make([]*StreamConsumer, 0, len(topicPartitions))

	for _, part := range topicPartitions {
		strCons, errN := NewStreamConsumer(fmt.Sprintf("%s:%d", part.Leader.Host, part.Leader.Port), topic, part.ID, 0)
		if errN != nil {
			continue
		}
		streamConsumers = append(streamConsumers, strCons)
	}
	return streamConsumers, nil
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
		if bt.Len() > 20 {
			err := rep.BatchQuery(ctx, &bt)
			if err != nil {
				log.Info().Err(err)
			}
			bt = pgx.Batch{}
		}
	}

}

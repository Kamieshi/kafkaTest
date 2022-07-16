package consumer

import (
	"KafkaWriterReader/internal/conf"
	"KafkaWriterReader/internal/repository"
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog/log"
	"os"
	"testing"
)

var consumerEasyInstance *SimpleReader
var ctx = context.Background()
var repMessage repository.MessageRepository
var consStream *StreamConsumer

func TestMain(m *testing.M) {

	var err error
	config, err := conf.GetConfig()
	if err != nil {
		panic(fmt.Errorf("repository.MainTest: %v", err))
	}

	pool, err := pgxpool.Connect(ctx, fmt.Sprintf("postgres://%v:%v@%v:%v/%v", config.POSTGRES_USER, config.POSTGRES_PASSWORD, config.POSTGRES_HOST, config.POSTGRES_PORT, config.POSTGRES_DB))
	if err != nil {
		panic(fmt.Errorf("repository.MainTest poolconnection: %v", err))
	}
	repMessage = repository.NewMessageRepositoryPostgres(pool)
	consumerEasyInstance = NewSimpleReader(&repMessage, []string{"127.0.0.1:9092"}, "test_group", "test_topic")

	consStream, err = NewStreamConsumer("127.0.0.1:9092", "test_topic", 0, 0)
	if err != nil {
		log.Fatal().Err(err)
	}
	code := m.Run()
	os.Exit(code)
}

func TestEasyReader_SingleListenMessageAndWriteToDB(t *testing.T) {
	consumerEasyInstance.ListenMessageAndWriteToDB(ctx)
}

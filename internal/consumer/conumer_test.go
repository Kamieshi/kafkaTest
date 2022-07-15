package consumer

import (
	"KafkaWriterReader/internal/conf"
	"KafkaWriterReader/internal/repository"
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"os"
	"testing"
	"time"
)

var consumerEasyInstance *EasyReader
var ctx = context.Background()
var repMessage repository.MessageRepository

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
	consumerEasyInstance = NewReader(&repMessage, []string{"127.0.0.1:9092"}, "test_group", "test_topic")
	code := m.Run()
	os.Exit(code)
}

func TestEasyReader_ListenMessageAndWriteToDB(t *testing.T) {
	go consumerEasyInstance.ListenMessageAndWriteToDB(ctx)
	go consumerEasyInstance.ListenMessageAndWriteToDB(ctx)
	time.Sleep(5 * time.Minute)
}

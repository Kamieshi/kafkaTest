package repository

import (
	"KafkaWriterReader/internal/conf"
	"KafkaWriterReader/internal/models"
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var pool *pgxpool.Pool
var ctx = context.Background()
var repMessage *MessageRepositoryPostgres

func TestMain(m *testing.M) {
	var err error
	config, err := conf.GetConfig()
	if err != nil {
		panic(fmt.Errorf("repository.MainTest: %v", err))
	}

	pool, err = pgxpool.Connect(ctx, fmt.Sprintf("postgres://%v:%v@%v:%v/%v", config.POSTGRES_USER, config.POSTGRES_PASSWORD, config.POSTGRES_HOST, config.POSTGRES_PORT, config.POSTGRES_DB))
	if err != nil {
		panic(fmt.Errorf("repository.MainTest poolconnection: %v", err))
	}
	repMessage = NewMessageRepositoryPostgres(pool)
	code := m.Run()
	os.Exit(code)
}

func TestMessageRepositoryPostgres_Write(t *testing.T) {
	message := models.Message{
		PayLoad: "Test",
	}
	err := repMessage.Write(ctx, &message)
	assert.Nil(t, err)
	t.Cleanup(func() {
		repMessage.Delete(ctx, message.ID)
	})
}

func TestMessageRepositoryPostgres_Get(t *testing.T) {
	message := models.Message{
		PayLoad: "Test1",
	}
	err := repMessage.Write(ctx, &message)

	assert.Nil(t, err)
	t.Cleanup(func() {
		repMessage.Delete(ctx, message.ID)
	})
	actualMessage, err := repMessage.Get(ctx, message.ID)
	assert.Nil(t, err)
	assert.Equal(t, message, *actualMessage)
}

func TestMessageRepositoryPostgres_Delete(t *testing.T) {
	message := models.Message{
		PayLoad: "Test",
	}
	err := repMessage.Write(ctx, &message)
	assert.Nil(t, err)
	err = repMessage.Delete(ctx, message.ID)
	assert.Nil(t, err)
	actualMessage, err := repMessage.Get(ctx, message.ID)
	assert.Error(t, err)
	assert.Nil(t, actualMessage)
}

func TestMessageRepositoryPostgres_BatchQuery(t *testing.T) {
	sizeQuerySet := 70000
	bt_update := pgx.Batch{}
	bt_delete := pgx.Batch{}
	var id uuid.UUID
	for i := 0; i < sizeQuerySet; i++ {
		id = uuid.New()
		bt_update.Queue("INSERT INTO messages(id,pay_load) values ($1,$2)", id, fmt.Sprintf("Data %d", i+1))
		bt_delete.Queue("DELETE FROM messages WHERE id=$1", id)
	}
	var beforeBatchCountRows int
	err := repMessage.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM messages").Scan(&beforeBatchCountRows)
	assert.Nil(t, err, "Get current count rows in table messages")
	err = repMessage.BatchQuery(ctx, &bt_update)
	assert.Nil(t, err)
	var afterBatchCountRows int
	err = repMessage.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM messages").Scan(&afterBatchCountRows)
	assert.Nil(t, err, "Get current count rows in table messages")
	assert.Equal(t, afterBatchCountRows-sizeQuerySet, beforeBatchCountRows)

	err = repMessage.BatchQuery(ctx, &bt_delete)
	assert.Nil(t, err)
	err = repMessage.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM messages").Scan(&afterBatchCountRows)
	assert.Nil(t, err, "Get current count rows in table messages")
	assert.Equal(t, afterBatchCountRows, beforeBatchCountRows)

}

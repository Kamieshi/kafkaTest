package repository

import (
	"KafkaWriterReader/internal/models"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type MessageRepositoryPostgres struct {
	Pool *pgxpool.Pool
}

// NewMessageRepositoryPostgres return lint to instance MessageRepositoryPostgres
func NewMessageRepositoryPostgres(p *pgxpool.Pool) *MessageRepositoryPostgres {
	return &MessageRepositoryPostgres{Pool: p}
}

func (m *MessageRepositoryPostgres) Get(ctx context.Context, id uuid.UUID) (*models.Message, error) {
	var message models.Message
	query := "SELECT id,pay_load FROM messages WHERE id=$1"
	err := m.Pool.QueryRow(ctx, query, id).Scan(&message.ID, &message.PayLoad)
	if err != nil {
		return nil, fmt.Errorf("messagePostgres.go/Get :%v", err)
	}
	return &message, err
}

func (m *MessageRepositoryPostgres) Delete(ctx context.Context, id uuid.UUID) error {
	query := "DELETE FROM messages WHERE id=$1"
	res, err := m.Pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("messagePostgres.go/Delete Error: %v", err)
	}
	if !res.Delete() {
		return fmt.Errorf("messagePostgres.go/Delete DElete unsuccess:%v", res.String())
	}
	return err
}

func (m *MessageRepositoryPostgres) Write(ctx context.Context, message *models.Message) error {
	message.ID = uuid.New()
	query := "INSERT INTO messages(id,pay_load) values ($1,$2)"
	res, err := m.Pool.Exec(ctx, query, message.ID, message.PayLoad)
	if err != nil {
		return fmt.Errorf("messagePostgres.go/Write Error: %v", err)
	}
	if !res.Insert() {
		return fmt.Errorf("messagePostgres.go/Write Insert unsuccess:%v", res.String())
	}
	return err
}

func (m *MessageRepositoryPostgres) BatchQuery(ctx context.Context, bt *pgx.Batch) error {
	br := m.Pool.SendBatch(ctx, bt)

	if err := br.Close(); err != nil {
		return fmt.Errorf("messagePostgres.go/Write Close Batch Postgres unsuccess:%v", err)
	}
	return nil
	res, err := br.Exec()
	defer br.Close()
	if err != nil {
		return err
		return fmt.Errorf("messagePostgres.go/BranchWrite :%v", err)
	}
	if int64(bt.Len()) == res.RowsAffected() {
		return fmt.Errorf("messagePostgres.go/BranchWrite :%v", errors.New("Count query in branch not equal like resoult"))
	}

	if err != nil {
		return fmt.Errorf("messagePostgres.go/BranchWrite :%v", err)
	}
	var afterBatchCountRows int
	err = m.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM messages").Scan(&afterBatchCountRows)
	fmt.Println(afterBatchCountRows)
	return err
}

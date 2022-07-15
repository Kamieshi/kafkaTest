package producer

import (
	"KafkaWriterReader/internal/models"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

var prod *producer

func TestMain(m *testing.M) {
	var err error
	prod, err = NewProducer("127.0.0.1:9092", "test_topic", 0)
	if err != nil {
		panic(err)
	}
	defer prod.conn.Close()
	code := m.Run()

	os.Exit(code)
}

func TestProducer_SendMessages(t *testing.T) {

	mess := []models.Message{
		{PayLoad: "kek 4", ID: uuid.New()},
		{PayLoad: "kek 3", ID: uuid.New()},
	}

	messFromKafka := make([]kafka.Message, len(mess))

	for i, data := range mess {
		dataTempBuffer, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		messFromKafka[i] = kafka.Message{Value: dataTempBuffer, Key: []byte(data.ID.String())}
	}

	err := prod.SendMessages(messFromKafka)
	assert.Nil(t, err)

}

func TestHighLoad(t *testing.T) {
	//countMessagesInSeccond := 20
	//timePause := 1000000 / countMessagesInSeccond
	countSendMessage := 0
	countMessageInPack := 10
	messPack := make([]models.Message, countMessageInPack)
	messFromKafka := make([]kafka.Message, countMessageInPack)
	checkTime := time.Now().Add(1 * time.Second)
	countInsecond := 0
	for {

		for i, _ := range messPack {
			messPack[i] = models.Message{ID: uuid.New(), PayLoad: fmt.Sprintf("Mesage :%d", countSendMessage+i)}
			dataTempBuffer, err := json.Marshal(messPack[i])
			if err != nil {
				t.Fatal(err)
			}
			messFromKafka[i] = kafka.Message{Value: dataTempBuffer, Key: []byte(messPack[i].ID.String())}
		}
		err := prod.SendMessages(messFromKafka)
		countSendMessage += countMessageInPack
		countInsecond += countMessageInPack
		assert.Nil(t, err)
		if err != nil {
			break
		}
		if time.Now().After(checkTime) {
			checkTime = time.Now().Add(1 * time.Second)
			fmt.Printf("Count in seccond : %d", countInsecond)
			fmt.Println(countInsecond)
			countInsecond = 0

		}
		time.Sleep(500 * time.Millisecond)
	}
}

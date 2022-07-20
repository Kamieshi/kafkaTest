package producer

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"

	"KafkaWriterReader/internal/models"
)

var prod *producer
var produsers []*producer

func TestMain(m *testing.M) {
	var err error
	conn, err := kafka.Dial("tcp", "127.0.0.1:9092")
	if err != nil {
		fmt.Println(err)
	}
	topicPartitions, err := conn.ReadPartitions("test_topic")
	if err != nil {
		fmt.Println(err)
	}
	produsers = make([]*producer, 0, len(topicPartitions))
	for _, part := range topicPartitions {
		pr, errC := NewProducer(fmt.Sprintf("%s:%d", part.Leader.Host, part.Leader.Port), "test_topic", part.ID)
		if errC != nil {
			continue
		}
		produsers = append(produsers, pr)
	}

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
	countMessagesInSeccond := 20
	timePause := float32(10000000) / float32(countMessagesInSeccond)
	countSendMessage := 0
	countMessageInPack := 1
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
			fmt.Printf("Count in seccond : %d\n", countInsecond)
			countInsecond = 0
		}
		if countInsecond > countMessagesInSeccond {
			time.Sleep(time.Duration(timePause) * time.Microsecond)
		}

	}
}

func TestStreamHighLoadCluster(t *testing.T) {
	f := func(n string) {
		countMessagesInSeccond := 20000
		timePause := float32(10000000) / float32(countMessagesInSeccond)
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

			produsers[countSendMessage%3].SendMessages(messFromKafka)

			countSendMessage += countMessageInPack
			countInsecond += countMessageInPack

			if time.Now().After(checkTime) {
				checkTime = time.Now().Add(1 * time.Second)
				fmt.Printf("%s Gorutine: Count in seccond : %d\n", n, countInsecond)
				countInsecond = 0
			}
			if countInsecond > countMessagesInSeccond {
				time.Sleep(time.Duration(timePause) * time.Microsecond)
			}
		}
	}
	go f("1")
	go f("2")
	go f("3")
	time.Sleep(5 * time.Minute)
}

package consumer

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestListenAndWriteToRep(t *testing.T) {
	consStream.ListenAndWriteToRep(context.Background(), repMessage)
}

func TestGetAddrPartitions(t *testing.T) {
	streamConsumers, err := GetStreamConsumers(ctx, "localhost:9093", "test_topic")
	fmt.Println(streamConsumers, err)
	for _, cons := range streamConsumers {
		go cons.ListenAndWriteToRep(ctx, repMessage)
	}
	time.Sleep(10 * time.Minute)
}

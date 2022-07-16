package consumer

import (
	"context"
	"testing"
)

func TestStreamConsumer_ListenAndWriteToRep(t *testing.T) {
	consStream.ListenAndWriteToRep(context.Background(), repMessage)
}

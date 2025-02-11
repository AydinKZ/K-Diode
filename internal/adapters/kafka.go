package adapters

import (
	"context"
	"github.com/AydinKZ/K-Diode-Caster/internal/domain"
	"github.com/segmentio/kafka-go"
	"time"
)

type KafkaReader struct {
	reader *kafka.Reader
}

func NewKafkaReader(brokers []string, topics []string, groupID string, maxWait time.Duration) *KafkaReader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupTopics: topics,
		GroupID:     groupID,
		MinBytes:    10e3,
		MaxBytes:    10e6,
		MaxWait:     maxWait * time.Millisecond,
	})
	return &KafkaReader{reader: reader}
}

func (kr *KafkaReader) ReadMessage() (*domain.Message, error) {
	msg, err := kr.reader.ReadMessage(context.Background())
	if err != nil {
		return &domain.Message{}, err
	}

	return &domain.Message{
		Topic: msg.Topic,
		Key:   string(msg.Key),
		Value: string(msg.Value),
	}, nil
}

func (kl *KafkaReader) Close() {
	kl.reader.Close()
}

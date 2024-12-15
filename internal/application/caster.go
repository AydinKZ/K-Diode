package application

import (
	"fmt"
	"github.com/AydinKZ/K-Diode-Caster/internal/adapters"
	"github.com/AydinKZ/K-Diode-Caster/internal/domain"
	"github.com/AydinKZ/K-Diode-Caster/internal/ports"
	"sync"
	"time"
)

type CasterService struct {
	KafkaReader    *adapters.KafkaReader
	UDPSender      *adapters.UDPSender
	HashCalculator ports.MessageHashCalculator
	Duplicator     ports.MessageDuplicator
	Copies         int
	EnableHash     bool
	Logger         *adapters.KafkaLogger
}

func NewCasterService(kafkaReader *adapters.KafkaReader, udpSender *adapters.UDPSender,
	hashCalculator ports.MessageHashCalculator,
	duplicator ports.MessageDuplicator, copies int, enableHash bool, logger *adapters.KafkaLogger) *CasterService {

	return &CasterService{
		KafkaReader:    kafkaReader,
		UDPSender:      udpSender,
		HashCalculator: hashCalculator,
		Duplicator:     duplicator,
		Copies:         copies,
		EnableHash:     enableHash,
		Logger:         logger,
	}
}

func (c *CasterService) ProcessAndSendMessages(workers int, messageCount int) error {
	messageChan := make(chan *domain.Message, messageCount)

	go func() {
		defer close(messageChan)
		for {
			msg, err := c.KafkaReader.ReadMessage()
			if err != nil {
				c.Logger.Log(fmt.Sprintf("[%v][Error Reading Message] %v", time.Now(), err.Error()))
				continue
			}
			messageChan <- msg
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range messageChan {
				c.processMessage(msg)
			}
		}()
	}

	wg.Wait()
	return nil
}

func (c *CasterService) processMessage(msg *domain.Message) {
	timeStart := time.Now()

	if c.EnableHash {
		hash := c.HashCalculator.Calculate(msg.Value)
		msg.Hash = hash
	}

	duplicatedMessages := c.Duplicator.Duplicate(msg, c.Copies)

	var wg sync.WaitGroup
	for _, duplicate := range duplicatedMessages {
		wg.Add(1)
		go func(dup *domain.Message) {
			defer wg.Done()
			err := c.UDPSender.Send(dup)
			if err != nil {
				c.Logger.Log(fmt.Sprintf("[%v][Error Sending Duplicate] %v", time.Now(), err.Error()))
			}
		}(duplicate)
	}
	wg.Wait()

	adapters.BroadcastStatus(0, msg.Topic, "SUCCESS", time.Since(timeStart))
	c.Logger.Log(fmt.Sprintf("[%v][Message Processed] Topic: %s, Duration: %v", time.Now(), msg.Topic, time.Since(timeStart)))
	c.Logger.SendMetricsToKafka()
}

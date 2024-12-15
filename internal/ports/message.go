package ports

import "github.com/AydinKZ/K-Diode-Caster/internal/domain"

// MessageHashCalculator - интерфейс для вычисления хэша сообщения
type MessageHashCalculator interface {
	Calculate(data string) string
}

// MessageDuplicator - интерфейс для реализации дублирования сообщений
type MessageDuplicator interface {
	Duplicate(message *domain.Message, copies int) []*domain.Message
}

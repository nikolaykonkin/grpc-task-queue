// Пакет storage хранит задачи очереди в памяти
package storage

import "errors"

// Status это статус задачи
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusRunning   Status = "RUNNING"
	StatusDone      Status = "DONE"
	StatusFailed    Status = "FAILED"
	StatusCancelled Status = "CANCELLED"
)

// Job это задача в очереди
// Время хранится как Unix-секунды, как и в proto-контракте
type Job struct {
	ID        string
	Kind      string
	Payload   string
	Status    Status
	Result    string
	Error     string
	CreatedAt int64
	UpdatedAt int64
}

var (
	// ErrNotFound возвращается, если задачи с таким id нет
	ErrNotFound = errors.New("job not found")

	// ErrInvalidTransition возвращается, если переход между статусами запрещен
	ErrInvalidTransition = errors.New("invalid status transition")
)

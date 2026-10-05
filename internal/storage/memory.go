package storage

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// transitions описывает разрешенные переходы: из какого статуса в какие
// Статусов DONE, FAILED и CANCELLED здесь нет: из них выхода нет
var transitions = map[Status]map[Status]bool{
	StatusPending: {StatusRunning: true, StatusCancelled: true},
	StatusRunning: {StatusDone: true, StatusFailed: true},
}

// Memory это потокобезопасное хранилище задач в памяти
// Копировать структуру нельзя (внутри мьютекс), поэтому везде *Memory
type Memory struct {
	mu    sync.RWMutex
	jobs  map[string]*Job
	order []string // id задач в порядке создания (для FIFO и стабильных страниц)
}

// NewMemory создает пустое хранилище
func NewMemory() *Memory {
	return &Memory{jobs: make(map[string]*Job)}
}

// Create создает задачу в статусе PENDING
// Проверка kind и payload выполняется выше, на уровне сервера
func (m *Memory) Create(kind, payload string) (Job, error) {
	// id генерируем до блокировки: чтение случайных байт не требует мьютекса
	id, err := newID()
	if err != nil {
		return Job{}, fmt.Errorf("generate id: %w", err)
	}

	now := time.Now().Unix()
	job := &Job{
		ID:        id,
		Kind:      kind,
		Payload:   payload,
		Status:    StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.jobs[id] = job
	m.order = append(m.order, id)

	// Возвращаем копию, а не указатель на объект внутри map
	return *job, nil
}

// Get возвращает копию задачи или ErrNotFound
func (m *Memory) Get(id string) (Job, error) {
	// Только читаем, поэтому достаточно RLock: читатели не мешают друг другу
	m.mu.RLock()
	defer m.mu.RUnlock()

	job, ok := m.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	return *job, nil
}

// List возвращает страницу задач в порядке создания и общее число задач, подходящих под фильтр:
//   - status - пустая строка означает "без фильтра"
//   - limit <= 0 означает "без ограничения" (значения по умолчанию задает сервер)
//   - offset < 0 считается нулем
func (m *Memory) List(status Status, limit, offset int) ([]Job, int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Сначала отбираем подходящие задачи по фильтру (в порядке создания)
	matched := make([]Job, 0)
	for _, id := range m.order {
		job := m.jobs[id]
		if status == "" || job.Status == status {
			matched = append(matched, *job)
		}
	}

	// total считаем до применения пагинации
	total := len(matched)

	if offset < 0 {
		offset = 0
	}
	if offset >= total {
		return []Job{}, total
	}
	matched = matched[offset:]

	if limit > 0 && limit < len(matched) {
		matched = matched[:limit]
	}
	return matched, total
}

// UpdateStatus переводит задачу в новый статус и записывает result и errMsg
// Проверка перехода и запись выполняются под одной блокировкой, поэтому
// два воркера не смогут одновременно взять одну и ту же задачу
func (m *Memory) UpdateStatus(id string, status Status, result, errMsg string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, ok := m.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}

	if !transitions[job.Status][status] {
		return Job{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, job.Status, status)
	}

	job.Status = status
	job.Result = result
	job.Error = errMsg
	job.UpdatedAt = time.Now().Unix()

	return *job, nil
}

// newID возвращает случайный id: 16 байт в виде hex-строки из 32 символов
func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

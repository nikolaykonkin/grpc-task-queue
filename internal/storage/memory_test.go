package storage_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/nikolaykonkin/grpc-task-queue/internal/storage"
)

func TestMemory_Create(t *testing.T) {
	t.Parallel()

	s := storage.NewMemory()

	first, err := s.Create("email", "hello")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	second, err := s.Create("email", "world")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if len(first.ID) != 32 {
		t.Errorf("длина id = %d, хотим 32", len(first.ID))
	}
	if first.ID == second.ID {
		t.Error("id двух задач совпали")
	}
	if first.Status != storage.StatusPending {
		t.Errorf("status = %s, хотим PENDING", first.Status)
	}
	if first.Kind != "email" || first.Payload != "hello" {
		t.Errorf("kind/payload не совпали: %+v", first)
	}
	if first.CreatedAt == 0 || first.UpdatedAt == 0 {
		t.Errorf("время не заполнено: %+v", first)
	}
}

func TestMemory_Get(t *testing.T) {
	t.Parallel()

	s := storage.NewMemory()
	created, err := s.Create("email", "hello")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	tests := []struct {
		name    string
		id      string
		wantErr error
	}{
		{name: "существующая задача", id: created.ID, wantErr: nil},
		{name: "несуществующая задача", id: "no-such-id", wantErr: storage.ErrNotFound},
		{name: "пустой id", id: "", wantErr: storage.ErrNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := s.Get(tt.id)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, хотим %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && got.ID != created.ID {
				t.Errorf("id = %s, хотим %s", got.ID, created.ID)
			}
		})
	}
}

func TestMemory_UpdateStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		before  []storage.Status // как довести задачу до нужного статуса
		target  storage.Status
		wantErr error
	}{
		{name: "PENDING -> RUNNING", target: storage.StatusRunning},
		{name: "PENDING -> CANCELLED", target: storage.StatusCancelled},
		{name: "PENDING -> DONE запрещено", target: storage.StatusDone, wantErr: storage.ErrInvalidTransition},
		{name: "RUNNING -> DONE", before: []storage.Status{storage.StatusRunning}, target: storage.StatusDone},
		{name: "RUNNING -> FAILED", before: []storage.Status{storage.StatusRunning}, target: storage.StatusFailed},
		{name: "RUNNING -> CANCELLED запрещено", before: []storage.Status{storage.StatusRunning}, target: storage.StatusCancelled, wantErr: storage.ErrInvalidTransition},
		{name: "DONE -> RUNNING запрещено", before: []storage.Status{storage.StatusRunning, storage.StatusDone}, target: storage.StatusRunning, wantErr: storage.ErrInvalidTransition},
		{name: "CANCELLED -> RUNNING запрещено", before: []storage.Status{storage.StatusCancelled}, target: storage.StatusRunning, wantErr: storage.ErrInvalidTransition},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// У каждого подтеста свое хранилище, поэтому они не мешают друг другу
			s := storage.NewMemory()
			job, err := s.Create("email", "hello")
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			for _, st := range tt.before {
				if _, err := s.UpdateStatus(job.ID, st, "", ""); err != nil {
					t.Fatalf("подготовка, переход в %s: %v", st, err)
				}
			}

			got, err := s.UpdateStatus(job.ID, tt.target, "res", "err")
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, хотим %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if got.Status != tt.target {
				t.Errorf("status = %s, хотим %s", got.Status, tt.target)
			}
			if got.Result != "res" || got.Error != "err" {
				t.Errorf("result/error не записались: %+v", got)
			}
		})
	}
}

func TestMemory_UpdateStatus_NotFound(t *testing.T) {
	t.Parallel()

	s := storage.NewMemory()
	_, err := s.UpdateStatus("no-such-id", storage.StatusRunning, "", "")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("err = %v, хотим ErrNotFound", err)
	}
}

func TestMemory_List(t *testing.T) {
	t.Parallel()

	// Создаем 5 задач и переводим часть из них в другие статусы:
	// 0 PENDING, 1 RUNNING, 2 DONE, 3 CANCELLED, 4 PENDING
	s := storage.NewMemory()
	ids := make([]string, 5)
	for i := range ids {
		job, err := s.Create("email", "payload")
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		ids[i] = job.ID
	}
	steps := []struct {
		idx    int
		status storage.Status
	}{
		{1, storage.StatusRunning},
		{2, storage.StatusRunning},
		{2, storage.StatusDone},
		{3, storage.StatusCancelled},
	}
	for _, st := range steps {
		if _, err := s.UpdateStatus(ids[st.idx], st.status, "", ""); err != nil {
			t.Fatalf("подготовка: %v", err)
		}
	}

	tests := []struct {
		name      string
		status    storage.Status
		limit     int
		offset    int
		wantIdx   []int // индексы ожидаемых задач в порядке создания
		wantTotal int
	}{
		{name: "без фильтра и без лимита", wantIdx: []int{0, 1, 2, 3, 4}, wantTotal: 5},
		{name: "фильтр PENDING", status: storage.StatusPending, wantIdx: []int{0, 4}, wantTotal: 2},
		{name: "фильтр DONE", status: storage.StatusDone, wantIdx: []int{2}, wantTotal: 1},
		{name: "первая страница", limit: 2, wantIdx: []int{0, 1}, wantTotal: 5},
		{name: "вторая страница", limit: 2, offset: 2, wantIdx: []int{2, 3}, wantTotal: 5},
		{name: "неполная последняя страница", limit: 2, offset: 4, wantIdx: []int{4}, wantTotal: 5},
		{name: "offset за пределами списка", offset: 10, wantIdx: []int{}, wantTotal: 5},
		{name: "фильтр и пагинация вместе", status: storage.StatusPending, limit: 1, offset: 1, wantIdx: []int{4}, wantTotal: 2},
		{name: "фильтр без совпадений", status: storage.StatusFailed, wantIdx: []int{}, wantTotal: 0},
		{name: "отрицательный offset как ноль", offset: -3, limit: 1, wantIdx: []int{0}, wantTotal: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, total := s.List(tt.status, tt.limit, tt.offset)
			if total != tt.wantTotal {
				t.Errorf("total = %d, хотим %d", total, tt.wantTotal)
			}
			if len(got) != len(tt.wantIdx) {
				t.Fatalf("получили %d задач, хотим %d", len(got), len(tt.wantIdx))
			}
			for i, idx := range tt.wantIdx {
				if got[i].ID != ids[idx] {
					t.Errorf("позиция %d: id = %s, хотим задачу #%d", i, got[i].ID, idx)
				}
			}
		})
	}
}

func TestMemory_ReturnsCopies(t *testing.T) {
	t.Parallel()

	s := storage.NewMemory()
	created, err := s.Create("email", "hello")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Меняем возвращенную копию: хранилище от этого меняться не должно
	created.Status = storage.StatusDone
	created.Payload = "changed"

	got, err := s.Get(created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != storage.StatusPending || got.Payload != "hello" {
		t.Errorf("хранилище изменилось через копию: %+v", got)
	}
}

// Много горутин одновременно создают, читают и листают задачи
// Тест осмыслен только вместе с -race: детектор найдет гонку, если где-то забыли блокировку
func TestMemory_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	s := storage.NewMemory()
	const n = 50

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			job, err := s.Create("email", "payload")
			if err != nil {
				t.Errorf("Create: %v", err)
				return
			}
			if _, err := s.Get(job.ID); err != nil {
				t.Errorf("Get: %v", err)
			}
			s.List("", 10, 0)
		}()
	}
	wg.Wait()

	_, total := s.List("", 0, 0)
	if total != n {
		t.Errorf("total = %d, хотим %d", total, n)
	}
}

// Двадцать воркеров одновременно пытаются взять одну задачу
// Должен победить ровно один, остальные получают ErrInvalidTransition
func TestMemory_UpdateStatus_OnlyOneWins(t *testing.T) {
	t.Parallel()

	s := storage.NewMemory()
	job, err := s.Create("email", "hello")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	const workers = 20
	errs := make([]error, workers) // каждая горутина пишет только в свою ячейку

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = s.UpdateStatus(job.ID, storage.StatusRunning, "", "")
		}()
	}
	wg.Wait()

	wins := 0
	for _, err := range errs {
		switch {
		case err == nil:
			wins++
		case !errors.Is(err, storage.ErrInvalidTransition):
			t.Errorf("неожиданная ошибка: %v", err)
		}
	}
	if wins != 1 {
		t.Errorf("задачу взяли %d воркеров, хотим ровно 1", wins)
	}
}
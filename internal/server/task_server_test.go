package server_test

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	taskv1 "github.com/nikolaykonkin/grpc-task-queue/gen/task/v1"
	"github.com/nikolaykonkin/grpc-task-queue/internal/server"
	"github.com/nikolaykonkin/grpc-task-queue/internal/storage"
)

// newClient поднимает gRPC-сервер в памяти (bufconn) и возвращает клиента
// Все запросы проходят через настоящий gRPC (сериализация, коды статусов), но без портов и сети
func newClient(t *testing.T) taskv1.TaskServiceClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)

	srv := grpc.NewServer()
	taskv1.RegisterTaskServiceServer(srv, server.NewTaskServer(storage.NewMemory(), slog.New(slog.DiscardHandler)))

	go func() {
		// Serve вернет ошибку после Stop, в тесте она не интересна
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)

	// Обычно клиент gRPC сам открывает сетевое соединение по адресу (сначала DNS переводит имя в IP-адрес)
	// У нас сети нет, сервер живет в памяти, поэтому мы сами говорим клиенту, как подключиться:
	// WithContextDialer задает функцию, которая возвращает соединение из bufconn
	// Схема passthrough отключает DNS: адрес bufnet не ищется в сети, а передается функции как есть
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	// Cleanup выполняются в обратном порядке: сначала закроется клиент, потом сервер
	t.Cleanup(func() { _ = conn.Close() })

	return taskv1.NewTaskServiceClient(conn)
}

// testCtx возвращает контекст с таймаутом, чтобы тест не завис навсегда
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// submit создает задачу и падает, если не получилось
func submit(t *testing.T, c taskv1.TaskServiceClient, kind, payload string) *taskv1.Job {
	t.Helper()
	resp, err := c.SubmitJob(testCtx(t), &taskv1.SubmitJobRequest{Kind: kind, Payload: payload})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	return resp.GetJob()
}

// setStatus переводит задачу в статус и падает, если не получилось
func setStatus(t *testing.T, c taskv1.TaskServiceClient, id string, st taskv1.JobStatus) {
	t.Helper()
	_, err := c.UpdateJobStatus(testCtx(t), &taskv1.UpdateJobStatusRequest{Id: id, Status: st})
	if err != nil {
		t.Fatalf("UpdateJobStatus(%s): %v", st, err)
	}
}

func TestSubmitJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		kind     string
		payload  string
		wantCode codes.Code
	}{
		{name: "валидная задача", kind: "email", payload: "hello", wantCode: codes.OK},
		{name: "пустой kind", kind: "", payload: "hello", wantCode: codes.InvalidArgument},
		{name: "пустой payload", kind: "email", payload: "", wantCode: codes.InvalidArgument},
		{name: "kind ровно 50 символов", kind: strings.Repeat("a", 50), payload: "x", wantCode: codes.OK},
		{name: "kind 51 символ", kind: strings.Repeat("a", 51), payload: "x", wantCode: codes.InvalidArgument},
		{name: "kind 50 русских букв (считаем символы, не байты)", kind: strings.Repeat("я", 50), payload: "x", wantCode: codes.OK},
		{name: "kind 51 русская буква", kind: strings.Repeat("я", 51), payload: "x", wantCode: codes.InvalidArgument},
		{name: "payload ровно 10000 символов", kind: "email", payload: strings.Repeat("a", 10000), wantCode: codes.OK},
		{name: "payload 10001 символ", kind: "email", payload: strings.Repeat("a", 10001), wantCode: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := newClient(t)

			resp, err := client.SubmitJob(testCtx(t), &taskv1.SubmitJobRequest{Kind: tt.kind, Payload: tt.payload})
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("код = %s, хотим %s (ошибка: %v)", got, tt.wantCode, err)
			}
			if tt.wantCode != codes.OK {
				return
			}

			job := resp.GetJob()
			if job.GetId() == "" {
				t.Error("id пустой")
			}
			if job.GetStatus() != taskv1.JobStatus_JOB_STATUS_PENDING {
				t.Errorf("status = %s, хотим PENDING", job.GetStatus())
			}
			if job.GetKind() != tt.kind || job.GetPayload() != tt.payload {
				t.Errorf("kind/payload не совпали: %v", job)
			}
		})
	}
}

func TestGetJob(t *testing.T) {
	t.Parallel()
	client := newClient(t)
	created := submit(t, client, "email", "hello")

	resp, err := client.GetJob(testCtx(t), &taskv1.GetJobRequest{Id: created.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if resp.GetJob().GetId() != created.GetId() {
		t.Errorf("id = %s, хотим %s", resp.GetJob().GetId(), created.GetId())
	}
}

// Три метода, которые принимают id, одинаково реагируют на плохой id
func TestMethodsWithBadID(t *testing.T) {
	t.Parallel()
	client := newClient(t)

	calls := []struct {
		name string
		call func(ctx context.Context, id string) error
	}{
		{name: "GetJob", call: func(ctx context.Context, id string) error {
			_, err := client.GetJob(ctx, &taskv1.GetJobRequest{Id: id})
			return err
		}},
		{name: "CancelJob", call: func(ctx context.Context, id string) error {
			_, err := client.CancelJob(ctx, &taskv1.CancelJobRequest{Id: id})
			return err
		}},
		{name: "UpdateJobStatus", call: func(ctx context.Context, id string) error {
			_, err := client.UpdateJobStatus(ctx, &taskv1.UpdateJobStatusRequest{
				Id: id, Status: taskv1.JobStatus_JOB_STATUS_RUNNING,
			})
			return err
		}},
	}
	ids := []struct {
		name     string
		id       string
		wantCode codes.Code
	}{
		{name: "пустой id", id: "", wantCode: codes.InvalidArgument},
		{name: "несуществующий id", id: "no-such-id", wantCode: codes.NotFound},
	}

	for _, c := range calls {
		for _, tc := range ids {
			t.Run(c.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				err := c.call(testCtx(t), tc.id)
				if got := status.Code(err); got != tc.wantCode {
					t.Errorf("код = %s, хотим %s (ошибка: %v)", got, tc.wantCode, err)
				}
			})
		}
	}
}

func TestListJobs(t *testing.T) {
	t.Parallel()
	client := newClient(t)

	// Статусы: 0 PENDING, 1 RUNNING, 2 DONE, 3 PENDING, 4 PENDING
	ids := make([]string, 5)
	for i := range ids {
		ids[i] = submit(t, client, "email", "payload").GetId()
	}
	setStatus(t, client, ids[1], taskv1.JobStatus_JOB_STATUS_RUNNING)
	setStatus(t, client, ids[2], taskv1.JobStatus_JOB_STATUS_RUNNING)
	setStatus(t, client, ids[2], taskv1.JobStatus_JOB_STATUS_DONE)

	tests := []struct {
		name      string
		status    taskv1.JobStatus
		limit     int32
		offset    int32
		wantCode  codes.Code
		wantIdx   []int // индексы ожидаемых задач в порядке создания
		wantTotal int32
	}{
		{name: "без фильтра, лимит по умолчанию", wantIdx: []int{0, 1, 2, 3, 4}, wantTotal: 5},
		{name: "фильтр PENDING", status: taskv1.JobStatus_JOB_STATUS_PENDING, wantIdx: []int{0, 3, 4}, wantTotal: 3},
		{name: "фильтр RUNNING", status: taskv1.JobStatus_JOB_STATUS_RUNNING, wantIdx: []int{1}, wantTotal: 1},
		{name: "страница", limit: 2, offset: 1, wantIdx: []int{1, 2}, wantTotal: 5},
		{name: "слишком большой limit ограничивается", limit: 1000, wantIdx: []int{0, 1, 2, 3, 4}, wantTotal: 5},
		{name: "offset за пределами списка", offset: 100, wantIdx: []int{}, wantTotal: 5},
		{name: "неизвестный статус", status: taskv1.JobStatus(99), wantCode: codes.InvalidArgument},
		{name: "отрицательный limit", limit: -1, wantCode: codes.InvalidArgument},
		{name: "отрицательный offset", offset: -1, wantCode: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := client.ListJobs(testCtx(t), &taskv1.ListJobsRequest{
				Status: tt.status, Limit: tt.limit, Offset: tt.offset,
			})
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("код = %s, хотим %s (ошибка: %v)", got, tt.wantCode, err)
			}
			if tt.wantCode != codes.OK {
				return
			}

			if resp.GetTotal() != tt.wantTotal {
				t.Errorf("total = %d, хотим %d", resp.GetTotal(), tt.wantTotal)
			}
			jobs := resp.GetJobs()
			if len(jobs) != len(tt.wantIdx) {
				t.Fatalf("получили %d задач, хотим %d", len(jobs), len(tt.wantIdx))
			}
			for i, idx := range tt.wantIdx {
				if jobs[i].GetId() != ids[idx] {
					t.Errorf("позиция %d: id = %s, хотим задачу #%d", i, jobs[i].GetId(), idx)
				}
			}
		})
	}
}

func TestCancelJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		steps    []taskv1.JobStatus // как довести задачу до нужного статуса
		wantCode codes.Code
	}{
		{name: "PENDING отменяется", wantCode: codes.OK},
		{
			name:     "RUNNING отменить нельзя",
			steps:    []taskv1.JobStatus{taskv1.JobStatus_JOB_STATUS_RUNNING},
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "DONE отменить нельзя",
			steps:    []taskv1.JobStatus{taskv1.JobStatus_JOB_STATUS_RUNNING, taskv1.JobStatus_JOB_STATUS_DONE},
			wantCode: codes.FailedPrecondition,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := newClient(t)

			job := submit(t, client, "email", "hello")
			for _, st := range tt.steps {
				setStatus(t, client, job.GetId(), st)
			}

			resp, err := client.CancelJob(testCtx(t), &taskv1.CancelJobRequest{Id: job.GetId()})
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("код = %s, хотим %s (ошибка: %v)", got, tt.wantCode, err)
			}
			if tt.wantCode == codes.OK && resp.GetJob().GetStatus() != taskv1.JobStatus_JOB_STATUS_CANCELLED {
				t.Errorf("status = %s, хотим CANCELLED", resp.GetJob().GetStatus())
			}
		})
	}
}

func TestCancelJob_Twice(t *testing.T) {
	t.Parallel()
	client := newClient(t)
	job := submit(t, client, "email", "hello")

	if _, err := client.CancelJob(testCtx(t), &taskv1.CancelJobRequest{Id: job.GetId()}); err != nil {
		t.Fatalf("первая отмена: %v", err)
	}
	_, err := client.CancelJob(testCtx(t), &taskv1.CancelJobRequest{Id: job.GetId()})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("повторная отмена: код = %s, хотим FailedPrecondition", got)
	}
}

func TestUpdateJobStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		steps    []taskv1.JobStatus // как довести задачу до нужного статуса
		target   taskv1.JobStatus
		result   string
		errMsg   string
		wantCode codes.Code
	}{
		{name: "PENDING -> RUNNING", target: taskv1.JobStatus_JOB_STATUS_RUNNING, wantCode: codes.OK},
		{
			name:     "RUNNING -> DONE с результатом",
			steps:    []taskv1.JobStatus{taskv1.JobStatus_JOB_STATUS_RUNNING},
			target:   taskv1.JobStatus_JOB_STATUS_DONE,
			result:   "ok",
			wantCode: codes.OK,
		},
		{
			name:     "RUNNING -> FAILED с ошибкой",
			steps:    []taskv1.JobStatus{taskv1.JobStatus_JOB_STATUS_RUNNING},
			target:   taskv1.JobStatus_JOB_STATUS_FAILED,
			errMsg:   "boom",
			wantCode: codes.OK,
		},
		{name: "PENDING -> DONE запрещено", target: taskv1.JobStatus_JOB_STATUS_DONE, wantCode: codes.FailedPrecondition},
		{
			name:     "DONE -> RUNNING запрещено",
			steps:    []taskv1.JobStatus{taskv1.JobStatus_JOB_STATUS_RUNNING, taskv1.JobStatus_JOB_STATUS_DONE},
			target:   taskv1.JobStatus_JOB_STATUS_RUNNING,
			wantCode: codes.FailedPrecondition,
		},
		{name: "воркеру нельзя ставить PENDING", target: taskv1.JobStatus_JOB_STATUS_PENDING, wantCode: codes.InvalidArgument},
		{name: "воркеру нельзя ставить CANCELLED", target: taskv1.JobStatus_JOB_STATUS_CANCELLED, wantCode: codes.InvalidArgument},
		{name: "статус не задан", target: taskv1.JobStatus_JOB_STATUS_UNSPECIFIED, wantCode: codes.InvalidArgument},
		{name: "неизвестный статус", target: taskv1.JobStatus(99), wantCode: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := newClient(t)

			job := submit(t, client, "email", "hello")
			for _, st := range tt.steps {
				setStatus(t, client, job.GetId(), st)
			}

			resp, err := client.UpdateJobStatus(testCtx(t), &taskv1.UpdateJobStatusRequest{
				Id: job.GetId(), Status: tt.target, Result: tt.result, Error: tt.errMsg,
			})
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("код = %s, хотим %s (ошибка: %v)", got, tt.wantCode, err)
			}
			if tt.wantCode != codes.OK {
				return
			}

			got := resp.GetJob()
			if got.GetStatus() != tt.target {
				t.Errorf("status = %s, хотим %s", got.GetStatus(), tt.target)
			}
			if got.GetResult() != tt.result || got.GetError() != tt.errMsg {
				t.Errorf("result/error не совпали: %v", got)
			}
		})
	}
}

// Десять воркеров одновременно пытаются взять одну задачу
// Ровно один должен победить, остальные получают FailedPrecondition
func TestUpdateJobStatus_OnlyOneWorkerWins(t *testing.T) {
	t.Parallel()
	client := newClient(t)
	ctx := testCtx(t)
	job := submit(t, client, "email", "hello")

	const workers = 10
	got := make([]codes.Code, workers) // каждая горутина пишет только в свою ячейку

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.UpdateJobStatus(ctx, &taskv1.UpdateJobStatusRequest{
				Id: job.GetId(), Status: taskv1.JobStatus_JOB_STATUS_RUNNING,
			})
			got[i] = status.Code(err)
		}()
	}
	wg.Wait()

	wins, rejected := 0, 0
	for _, c := range got {
		switch c {
		case codes.OK:
			wins++
		case codes.FailedPrecondition:
			rejected++
		}
	}
	if wins != 1 || rejected != workers-1 {
		t.Errorf("победителей %d, отказов %d; хотим 1 и %d", wins, rejected, workers-1)
	}
}

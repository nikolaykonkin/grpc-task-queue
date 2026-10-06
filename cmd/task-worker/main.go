// Команда task-worker забирает задачи у task-api по gRPC и выполняет их
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	taskv1 "github.com/nikolaykonkin/grpc-task-queue/gen/task/v1"
)

const (
	pollInterval  = time.Second     // пауза между проверками очереди
	workDuration  = 2 * time.Second // сколько "выполняется" задача
	callTimeout   = 5 * time.Second // таймаут на вызовы ListJobs и UpdateJobStatus(RUNNING)
	reportTimeout = 5 * time.Second // таймаут на отправку результата
)

func main() {
	if err := run(); err != nil {
		slog.Error("task-worker failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", "localhost:50051", "адрес task-api")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// ctx отменится при Ctrl+C (SIGINT) или SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// NewClient не подключается сразу: соединение откроется при первом вызове
	// Шифрование (TLS) не используем: это учебный проект на localhost
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("create grpc client: %w", err)
	}
	defer conn.Close()

	client := taskv1.NewTaskServiceClient(conn)
	log.Info("task-worker started", "addr", *addr)

	for {
		// Если пришел сигнал остановки, ошибки вызовов ожидаемы: не шумим в логе
		if err := processOne(ctx, client, log); err != nil && ctx.Err() == nil {
			log.Error("process job failed", "error", err)
		}

		// Ждем секунду или сигнал остановки: что наступит раньше
		select {
		case <-ctx.Done():
			log.Info("task-worker stopped")
			return nil
		case <-time.After(pollInterval):
		}
	}
}

// processOne обрабатывает не более одной задачи: занять, выполнить, доложить
func processOne(ctx context.Context, client taskv1.TaskServiceClient, log *slog.Logger) error {
	job, claimed, err := claimJob(ctx, client, log)
	if err != nil {
		return err
	}
	if !claimed {
		return nil // брать нечего
	}

	log.Info("job started", "id", job.GetId(), "kind", job.GetKind())
	result, execErr := execute(job)

	return reportResult(client, log, job, result, execErr)
}

// claimJob ищет самую старую PENDING-задачу и пытается ее занять (PENDING -> RUNNING)
// claimed равен false, если очередь пуста или задачу успел взять другой воркер
func claimJob(ctx context.Context, client taskv1.TaskServiceClient, log *slog.Logger) (job *taskv1.Job, claimed bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	resp, err := client.ListJobs(ctx, &taskv1.ListJobsRequest{
		Status: taskv1.JobStatus_JOB_STATUS_PENDING,
		Limit:  1,
	})
	if err != nil {
		return nil, false, fmt.Errorf("list jobs: %w", err)
	}
	if len(resp.GetJobs()) == 0 {
		return nil, false, nil
	}
	job = resp.GetJobs()[0]

	_, err = client.UpdateJobStatus(ctx, &taskv1.UpdateJobStatusRequest{
		Id:     job.GetId(),
		Status: taskv1.JobStatus_JOB_STATUS_RUNNING,
	})
	switch {
	case err == nil:
		return job, true, nil
	case status.Code(err) == codes.FailedPrecondition:
		// Пока мы выбирали, задачу взял другой воркер или ее отменили
		// Это нормальная ситуация, просто пропускаем
		log.Info("job is no longer available, skipping", "id", job.GetId())
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("mark job running: %w", err)
	}
}

// execute "выполняет" задачу: просто ждет
// Для демонстрации статуса FAILED задача с payload "fail" завершается ошибкой
func execute(job *taskv1.Job) (string, error) {
	time.Sleep(workDuration)

	if job.GetPayload() == "fail" {
		return "", errors.New("job failed on purpose (payload is \"fail\")")
	}
	return fmt.Sprintf("processed %s job", job.GetKind()), nil
}

// reportResult отправляет результат: DONE с результатом или FAILED с ошибкой
func reportResult(client taskv1.TaskServiceClient, log *slog.Logger, job *taskv1.Job, result string, execErr error) error {
	req := &taskv1.UpdateJobStatusRequest{Id: job.GetId()}
	if execErr != nil {
		req.Status = taskv1.JobStatus_JOB_STATUS_FAILED
		req.Error = execErr.Error()
	} else {
		req.Status = taskv1.JobStatus_JOB_STATUS_DONE
		req.Result = result
	}

	// Свой контекст, не связанный с сигналом остановки: если Ctrl+C нажат во время работы,
	// результат все равно нужно доложить, иначе задача навсегда останется в статусе RUNNING
	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()

	if _, err := client.UpdateJobStatus(ctx, req); err != nil {
		return fmt.Errorf("report result: %w", err)
	}
	log.Info("job finished", "id", job.GetId(), "status", req.GetStatus().String())
	return nil
}

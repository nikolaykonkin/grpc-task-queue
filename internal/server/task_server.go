// Пакет server реализует gRPC-сервис TaskService поверх хранилища в памяти
package server

import (
	"context"
	"errors"
	"log/slog"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	taskv1 "github.com/nikolaykonkin/grpc-task-queue/gen/task/v1"
	"github.com/nikolaykonkin/grpc-task-queue/internal/storage"
)

// Ограничения на вход
const (
	maxKindLen    = 50    // символов
	maxPayloadLen = 10000 // символов
	defaultLimit  = 50    // размер страницы по умолчанию в ListJobs
	maxLimit      = 100   // максимальный размер страницы в ListJobs
)

var errEmptyID = status.Error(codes.InvalidArgument, "id is required")

// TaskServer реализует интерфейс taskv1.TaskServiceServer
type TaskServer struct {
	// Встраиваем заглушку: если в proto появятся новые методы, сервер
	// продолжит компилироваться, а новые методы вернут Unimplemented
	taskv1.UnimplementedTaskServiceServer

	store *storage.Memory
	log   *slog.Logger
}

// NewTaskServer создает сервер с переданным хранилищем и логгером
func NewTaskServer(store *storage.Memory, log *slog.Logger) *TaskServer {
	return &TaskServer{store: store, log: log}
}

// SubmitJob создает задачу в статусе PENDING
func (s *TaskServer) SubmitJob(_ context.Context, req *taskv1.SubmitJobRequest) (*taskv1.SubmitJobResponse, error) {
	// Используем геттеры (GetKind и т.д.): они безопасны, даже если req равен nil.
	if err := validateSubmit(req.GetKind(), req.GetPayload()); err != nil {
		return nil, err
	}

	job, err := s.store.Create(req.GetKind(), req.GetPayload())
	if err != nil {
		return nil, s.toStatusError(err)
	}
	return &taskv1.SubmitJobResponse{Job: toProto(job)}, nil
}

// GetJob возвращает задачу по id
func (s *TaskServer) GetJob(_ context.Context, req *taskv1.GetJobRequest) (*taskv1.GetJobResponse, error) {
	if req.GetId() == "" {
		return nil, errEmptyID
	}

	job, err := s.store.Get(req.GetId())
	if err != nil {
		return nil, s.toStatusError(err)
	}
	return &taskv1.GetJobResponse{Job: toProto(job)}, nil
}

// ListJobs возвращает страницу задач с фильтром по статусу
func (s *TaskServer) ListJobs(_ context.Context, req *taskv1.ListJobsRequest) (*taskv1.ListJobsResponse, error) {
	// UNSPECIFIED превращается в пустой фильтр, то есть "все статусы"
	filter, ok := statusFromProto(req.GetStatus())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "unknown status")
	}

	limit := int(req.GetLimit())
	offset := int(req.GetOffset())
	if limit < 0 {
		return nil, status.Error(codes.InvalidArgument, "limit must not be negative")
	}
	if offset < 0 {
		return nil, status.Error(codes.InvalidArgument, "offset must not be negative")
	}
	if limit == 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}

	page, total := s.store.List(filter, limit, offset)

	jobs := make([]*taskv1.Job, 0, len(page))
	for _, j := range page {
		jobs = append(jobs, toProto(j))
	}
	return &taskv1.ListJobsResponse{Jobs: jobs, Total: int32(total)}, nil
}

// CancelJob отменяет задачу, пока она в статусе PENDING
func (s *TaskServer) CancelJob(_ context.Context, req *taskv1.CancelJobRequest) (*taskv1.CancelJobResponse, error) {
	if req.GetId() == "" {
		return nil, errEmptyID
	}

	// Допустимость перехода проверяет хранилище: отменить можно только PENDING
	job, err := s.store.UpdateStatus(req.GetId(), storage.StatusCancelled, "", "")
	if err != nil {
		return nil, s.toStatusError(err)
	}
	return &taskv1.CancelJobResponse{Job: toProto(job)}, nil
}

// UpdateJobStatus нужен воркеру: перевести задачу в RUNNING, DONE или FAILED
func (s *TaskServer) UpdateJobStatus(_ context.Context, req *taskv1.UpdateJobStatusRequest) (*taskv1.UpdateJobStatusResponse, error) {
	if req.GetId() == "" {
		return nil, errEmptyID
	}

	// Воркеру разрешены только три статуса
	// PENDING и CANCELLED ему не нужны: отмена делается отдельным методом CancelJob
	st, ok := statusFromProto(req.GetStatus())
	if !ok || (st != storage.StatusRunning && st != storage.StatusDone && st != storage.StatusFailed) {
		return nil, status.Error(codes.InvalidArgument, "status must be RUNNING, DONE or FAILED")
	}

	job, err := s.store.UpdateStatus(req.GetId(), st, req.GetResult(), req.GetError())
	if err != nil {
		return nil, s.toStatusError(err)
	}
	return &taskv1.UpdateJobStatusResponse{Job: toProto(job)}, nil
}

// validateSubmit проверяет kind и payload
// Длину считаем в символах (рунах), а не в байтах: русская буква это 2 байта
func validateSubmit(kind, payload string) error {
	switch {
	case kind == "":
		return status.Error(codes.InvalidArgument, "kind is required")
	case utf8.RuneCountInString(kind) > maxKindLen:
		return status.Errorf(codes.InvalidArgument, "kind must be at most %d characters", maxKindLen)
	case payload == "":
		return status.Error(codes.InvalidArgument, "payload is required")
	case utf8.RuneCountInString(payload) > maxPayloadLen:
		return status.Errorf(codes.InvalidArgument, "payload must be at most %d characters", maxPayloadLen)
	}
	return nil
}

// toStatusError переводит ошибку хранилища в gRPC-ошибку с нужным кодом
func (s *TaskServer) toStatusError(err error) error {
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return status.Error(codes.NotFound, "job not found")
	case errors.Is(err, storage.ErrInvalidTransition):
		// Запрос корректный, но состояние задачи не позволяет его выполнить
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		// Подробности только в лог, клиенту отдаем общий текст
		s.log.Error("storage error", "error", err)
		return status.Error(codes.Internal, "internal error")
	}
}

// toProto переводит задачу из хранилища в protobuf-сообщение
func toProto(j storage.Job) *taskv1.Job {
	return &taskv1.Job{
		Id:        j.ID,
		Kind:      j.Kind,
		Payload:   j.Payload,
		Status:    statusToProto(j.Status),
		Result:    j.Result,
		Error:     j.Error,
		CreatedAt: j.CreatedAt,
		UpdatedAt: j.UpdatedAt,
	}
}

func statusToProto(st storage.Status) taskv1.JobStatus {
	switch st {
	case storage.StatusPending:
		return taskv1.JobStatus_JOB_STATUS_PENDING
	case storage.StatusRunning:
		return taskv1.JobStatus_JOB_STATUS_RUNNING
	case storage.StatusDone:
		return taskv1.JobStatus_JOB_STATUS_DONE
	case storage.StatusFailed:
		return taskv1.JobStatus_JOB_STATUS_FAILED
	case storage.StatusCancelled:
		return taskv1.JobStatus_JOB_STATUS_CANCELLED
	default:
		return taskv1.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}

// statusFromProto переводит enum из proto в статус хранилища
// Для UNSPECIFIED возвращает пустой статус (true),
// для неизвестного числа, которое мог прислать клиент, возвращает false
func statusFromProto(p taskv1.JobStatus) (storage.Status, bool) {
	switch p {
	case taskv1.JobStatus_JOB_STATUS_UNSPECIFIED:
		return "", true
	case taskv1.JobStatus_JOB_STATUS_PENDING:
		return storage.StatusPending, true
	case taskv1.JobStatus_JOB_STATUS_RUNNING:
		return storage.StatusRunning, true
	case taskv1.JobStatus_JOB_STATUS_DONE:
		return storage.StatusDone, true
	case taskv1.JobStatus_JOB_STATUS_FAILED:
		return storage.StatusFailed, true
	case taskv1.JobStatus_JOB_STATUS_CANCELLED:
		return storage.StatusCancelled, true
	default:
		return "", false
	}
}

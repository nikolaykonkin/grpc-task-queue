// Команда task-api запускает gRPC-сервер очереди задач
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	taskv1 "github.com/nikolaykonkin/grpc-task-queue/gen/task/v1"
	"github.com/nikolaykonkin/grpc-task-queue/internal/server"
	"github.com/nikolaykonkin/grpc-task-queue/internal/storage"
)

// Сколько ждем завершения текущих запросов, прежде чем остановить сервер принудительно
const shutdownTimeout = 10 * time.Second

func main() {
	// Вся работа в run, чтобы defer внутри успевали выполниться:
	// os.Exit завершает программу сразу и пропускает все defer
	if err := run(); err != nil {
		slog.Error("task-api failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	// По умолчанию слушаем только localhost: аутентификации у нас нет,
	// поэтому наружу сервер без необходимости не открываем
	addr := flag.String("addr", "localhost:50051", "адрес, на котором слушает gRPC-сервер")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// ctx отменится, когда придет Ctrl+C (SIGINT) или SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", *addr, err)
	}

	srv := grpc.NewServer()
	taskv1.RegisterTaskServiceServer(srv, server.NewTaskServer(storage.NewMemory(), log))

	// Serve блокирует выполнение, поэтому запускаем его в горутине
	// Канал с буфером 1: горутина не зависнет на записи, даже если мы уже вышли из select и никто не читает
	serveErr := make(chan error, 1)
	go func() {
		log.Info("task-api started", "addr", lis.Addr().String())
		serveErr <- srv.Serve(lis)
	}()

	// Ждем одно из двух: ошибку сервера или сигнал остановки
	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
		log.Info("shutdown signal received")
	}

	// Возвращаем стандартное поведение сигналов: если остановка зависнет,
	// второй Ctrl+C завершит процесс сразу
	stop()

	// GracefulStop ждет завершения текущих запросов
	// Запускаем его в горутине, чтобы ограничить ожидание таймаутом
	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		log.Info("task-api stopped gracefully")
	case <-time.After(shutdownTimeout):
		log.Warn("graceful shutdown timed out, forcing stop")
		srv.Stop()
	}
	return nil
}

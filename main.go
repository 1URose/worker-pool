package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Job struct {
	Entity string
}

func NewJob(entity string) Job {
	return Job{Entity: entity}
}

type WorkerPool struct {
	nextWorkerId int
	jobs         chan Job
	quitChans    map[int]chan bool
	mu           sync.Mutex
	wg           sync.WaitGroup
}

func NewWorkerPool(jobsChanSize int) *WorkerPool {
	nextWorkerId := 1
	jobs := make(chan Job, jobsChanSize)
	quitChans := make(map[int]chan bool)

	return &WorkerPool{
		nextWorkerId: nextWorkerId,
		jobs:         jobs,
		quitChans:    quitChans,
	}
}

func (wp *WorkerPool) AddJob(job Job) {
	wp.mu.Lock()
	available := len(wp.quitChans)
	wp.mu.Unlock()

	if available == 0 {
		fmt.Printf("Нет доступных работников для обработки задания %q\n", job.Entity)
		return
	}
	wp.jobs <- job
}

func (wp *WorkerPool) AddWorker() int {
	wp.mu.Lock()
	id := wp.nextWorkerId
	wp.nextWorkerId++
	quit := make(chan bool)
	wp.quitChans[id] = quit
	wp.wg.Add(1)
	wp.mu.Unlock()

	go func(id int, quit chan bool) {
		defer wp.wg.Done()
		for {
			select {
			case job := <-wp.jobs:
				fmt.Printf("Работник %d обрабатывает задание %q\n", id, job.Entity)
			case <-quit:
				fmt.Printf("Работник %d завершает работу\n", id)
				return
			}
		}
	}(id, quit)

	fmt.Printf("Работник %d добавлен\n", id)
	return id
}

func (wp *WorkerPool) RemoveWorker(id int) bool {
	wp.mu.Lock()
	quit, exists := wp.quitChans[id]
	if exists {
		close(quit)
		delete(wp.quitChans, id)
		fmt.Printf("Работник %d получил сигнал на завершение\n", id)
	}
	wp.mu.Unlock()
	return exists
}

func (wp *WorkerPool) Shutdown() {
	wp.mu.Lock()
	for id, quit := range wp.quitChans {
		close(quit)
		fmt.Printf("Работник %d получил сигнал на завершение\n", id)
	}
	wp.quitChans = make(map[int]chan bool)
	wp.mu.Unlock()

	wp.wg.Wait()
	close(wp.jobs)
}

func main() {
	const jobsChanSize = 10
	wp := NewWorkerPool(jobsChanSize)

	for i := 0; i < 2; i++ {
		wp.AddWorker()
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		switch parts[0] {
		case "add":
			wp.AddWorker()
		case "remove":
			if len(parts) != 2 {
				fmt.Println("Использование: remove <ID работника>")
				continue
			}
			id, err := strconv.Atoi(parts[1])
			if err != nil || !wp.RemoveWorker(id) {
				fmt.Println("Неверный ID или работник не найден:", parts[1])
			}
		case "quit":
			wp.Shutdown()
			fmt.Println("Пул остановлен, выход.")
			return
		default:
			wp.AddJob(NewJob(line))
		}
	}
	wp.Shutdown()
}

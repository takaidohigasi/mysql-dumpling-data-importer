package pimp

import (
	"math/rand"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Job struct {
	Task       func(string, *ImportData) error
	Length     int
	ResourceId string
	Data       *ImportData
}

type WorkerPool interface {
	Run()
	AddTask(Job)
	Wait()
	Progress() (int, int)
}

type workerPool struct {
	maxWorker          int
	maxCPU             int
	maxThreadPerWorker int
	queuedTaskC        chan Job
	progress           Progress
	wg                 sync.WaitGroup
}

type Progress struct {
	mutex       sync.Mutex
	concurrency int
	completed   int
}

func (p *Progress) start(thread int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.concurrency += thread
}

func (p *Progress) finish(thread int, file int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.concurrency -= thread
	p.completed += file
}

func (wp *workerPool) Run() {
	wp.run()
}

func (wp *workerPool) AddTask(task Job) {
	wp.wg.Add(1)
	wp.queuedTaskC <- task
}

func (wp *workerPool) Wait() {
	wp.wg.Wait()
}

func (wp *workerPool) run() {
	wp.progress.concurrency = 0
	for i := 0; i < wp.maxWorker; i++ {
		wID := i + 1
		go func(workerID int, wp *workerPool) {
			for task := range wp.queuedTaskC {
				taskThread := wp.maxThreadPerWorker
				if task.Data.FileNum < taskThread {
					taskThread = task.Data.FileNum
				}
				if wp.maxCPU-wp.progress.concurrency >= taskThread {
					wp.progress.start(taskThread)
					if err := task.Task(task.ResourceId, task.Data); err != nil {
						log.Errorln("error", err)
					}
					wp.progress.finish(taskThread, task.Length)
					wp.wg.Done()
				} else {
					// re-enqueue
					wp.queuedTaskC <- task
					time.Sleep(time.Duration(rand.Intn(60)) * time.Second)
					log.Infoln("re-enqueued:", task.ResourceId, "required thread: ", taskThread, "available thread: ", wp.maxCPU-wp.progress.concurrency)
				}
			}
		}(wID, wp)
	}
}

func (wp *workerPool) Progress() (concurrency int, completed int) {
	return wp.progress.concurrency, wp.progress.completed
}

func NewWorkerPool(maxWorker int, maxCPU int, maxThreadPerWorker int) WorkerPool {
	wp := &workerPool{
		maxWorker:          maxWorker,
		maxCPU:             maxCPU,
		maxThreadPerWorker: maxThreadPerWorker,
		queuedTaskC:        make(chan Job),
		progress:           Progress{},
		wg:                 sync.WaitGroup{},
	}
	return wp
}

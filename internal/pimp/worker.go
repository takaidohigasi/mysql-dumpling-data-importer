package pimp

import (
	"sync"
        log "github.com/sirupsen/logrus"
)

type Job struct {
        Task   func(string, *ImportData) error
        Thread int
        Length int
        ResourceId      string
        Data            *ImportData
}

type WorkerPool interface {
        Run()
        AddTask(Job)
        Wait()
}

type workerPool struct {
        maxWorker   int
        maxCPU      int
        queuedTaskC chan Job
        progress    Progress
        wg          sync.WaitGroup
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
                go func(workerID int) {
                        for task := range wp.queuedTaskC {
                                if wp.maxCPU-wp.progress.concurrency >= task.Thread {
                                        wp.progress.start(task.Thread)
                                        if err := task.Task(task.ResourceId, task.Data); err != nil {
                                                log.Infoln("error", err)
                                        }
                                        wp.progress.finish(task.Thread, task.Length)
                                        wp.wg.Done()
                                }
                        }
                }(wID)
        }
}

func NewWorkerPool(maxWorker int, maxCPU int) WorkerPool {
        wp := &workerPool{
                maxWorker:   maxWorker,
                maxCPU:      maxCPU,
                queuedTaskC: make(chan Job),
                progress:    Progress{},
                wg:          sync.WaitGroup{},
        }
        return wp
}
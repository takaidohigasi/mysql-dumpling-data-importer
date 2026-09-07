package pimp

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

type Job struct {
	Task       func(string, *ImportData) error
	ResourceId string
	Data       *ImportData
}

type WorkerPool interface {
	Run()
	AddTask(Job)
	Wait()
	Progress() (int, int)
}

// workerPool runs one data file per task, each imported with --threads=1, so
// the worker count IS the thread count: there is no separate thread budget to
// reserve against, and a worker simply runs whatever it pulls next.
type workerPool struct {
	maxWorker   int
	queuedTaskC chan Job
	progress    Progress
	wg          sync.WaitGroup
}

type Progress struct {
	mutex     sync.Mutex
	running   int
	completed int
}

func (p *Progress) start() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.running++
}

func (p *Progress) finish() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.running--
	p.completed++
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
	for i := 0; i < wp.maxWorker; i++ {
		go func(wp *workerPool) {
			for task := range wp.queuedTaskC {
				wp.progress.start()
				if err := task.Task(task.ResourceId, task.Data); err != nil {
					log.Errorln("error", err)
				}
				wp.progress.finish()
				wp.wg.Done()
			}
		}(wp)
	}
}

func (wp *workerPool) Progress() (running int, completed int) {
	wp.progress.mutex.Lock()
	defer wp.progress.mutex.Unlock()
	return wp.progress.running, wp.progress.completed
}

func NewWorkerPool(maxWorker int) WorkerPool {
	wp := &workerPool{
		maxWorker:   maxWorker,
		queuedTaskC: make(chan Job, 1024),
		progress:    Progress{},
		wg:          sync.WaitGroup{},
	}
	return wp
}

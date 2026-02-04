import os
import threading
from queue import Queue
from threading import Thread


class ThreadPool:
    ENV_TP_NUM_OF_THREADS = 'TP_NUM_OF_THREADS'

    def __init__(self):
        self.nr_threads = self.get_max_nr_of_threads()
        self.tasks = Queue()
        self.shutdown_event = threading.Event()
        self.runners = self.init_runners()
        self.job_id = 0
        self.job_id_lock = threading.Lock()
        self.all_tasks = {}

    def init_runners(self):
        runners = []
        for _ in range(self.nr_threads):
            runner = TaskRunner(self.tasks, self.shutdown_event)
            runners.append(runner)
            runner.start()
        return runners

    def get_max_nr_of_threads(self):
        return os.cpu_count() if self.ENV_TP_NUM_OF_THREADS not in os.environ \
            else os.environ.get(self.ENV_TP_NUM_OF_THREADS)

    def add_task(self, task):
        with self.job_id_lock:
            self.job_id += 1
            task.task_id = self.job_id
            self.all_tasks[self.job_id] = task
            self.tasks.put(task)
            return self.job_id

    def is_task_done(self, job_id):
        return self.all_tasks[job_id].is_done

    def graceful_shutdown(self):
        self.shutdown_event.set()
        for runner in self.runners:
            runner.join()

    def jobs(self):
        jobs = []

        for t in self.all_tasks.values():
            if t.is_done:
                res = "done"
            else:
                res = "running"
            jobs.append({t.task_id: res})

        return jobs

    def num_jobs(self):
        cnt = 0
        for t in self.all_tasks.values():
            if not t.is_done:
                cnt += 1
        return cnt

class TaskRunner(Thread):
    def __init__(self, tasks, shutdown_event):
        super().__init__()
        self.tasks = tasks
        self.shutdown_event = shutdown_event

    def run(self):
        while True:
            task = self.tasks.get()
            self.run_task(task)

    def run_task(self, task):
        task.execute_task()

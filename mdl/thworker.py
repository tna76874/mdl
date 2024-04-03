#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
multi thread worker
"""
import threading
import time
from queue import Queue
from tqdm import tqdm

class ThreadedWorker:
    def __init__(self, data, worker_function, threads=10, info=None):
        self.data = data
        self.worker_function = worker_function
        self.total_tasks = len(data)
        self.queue = Queue()
        self.threads = min([threads, self.total_tasks])
        self.info = info

    def _worker(self):
        while True:
            item = self.queue.get()
            if item is None:
                break
            self.worker_function(**item)
            self.queue.task_done()

    # Funktion zur Aktualisierung der Fortschrittsanzeige mit der Länge der Warteschlange
    def update_progress_bar(self):
        with tqdm(total=self.total_tasks) as pbar:
            while True:
                pbar.update(self.total_tasks - self.queue.qsize() - pbar.n)

                # Wenn alle Aufgaben erledigt sind, breche die Schleife ab
                if self.queue.empty():
                    break
                time.sleep(1)

    def start_processing(self):
        if self.info:
            print(self.info)
        # Starte Worker-Threads
        workers = []
        for _ in range(self.threads):
            worker = threading.Thread(target=self._worker)
            worker.start()
            workers.append(worker)

        # Fülle die Warteschlange mit Aufgaben
        for item in self.data:
            self.queue.put(item)

        progress_thread = threading.Thread(target=self.update_progress_bar)
        progress_thread.start()

        # Warte darauf, dass alle Aufgaben bearbeitet werden
        self.queue.join()

        # Stoppe den Thread zur Aktualisierung der Fortschrittsanzeige
        progress_thread.join()

        # Beende die Worker-Threads
        for _ in range(self.threads):
            self.queue.put(None)

        for worker in workers:
            worker.join()
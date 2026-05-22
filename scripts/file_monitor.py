import time
import subprocess
import queue
import threading
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

MAX_WORKERS = 5
WATCH_PATH = "data/raw"

file_queue = queue.Queue()

def process_worker():
    """Worker thread to process files from the queue."""
    while True:
        file_path = file_queue.get()
        print(f"⚙️ Processing: {os.path.basename(file_path)}")
        try:
            subprocess.run([
                "python", "scripts/run_pipeline.py",
                "--input", file_path
            ], check=True)
            print(f"✅ Finished: {file_path}")
        except Exception as e:
            print(f"❌ Failed: {file_path} - {e}")
        finally:
            file_queue.task_done()

class NewFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            print(f"🔍 Detected: {event.src_path}")
            file_queue.put(event.src_path)

if __name__ == "__main__":
    os.makedirs(WATCH_PATH, exist_ok=True)
    
    # Start workers
    for _ in range(MAX_WORKERS):
        threading.Thread(target=process_worker, daemon=True).start()
    
    # Start monitoring
    observer = Observer()
    observer.schedule(NewFileHandler(), path=WATCH_PATH, recursive=False)
    observer.start()
    
    print(f"👀 Monitoring {WATCH_PATH} with {MAX_WORKERS} workers")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()
        print("Shutdown complete")

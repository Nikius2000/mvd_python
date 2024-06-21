import time
import os
import psycopg2
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime

DB_HOST = "localhost"
DB_NAME = "mvd"
DB_USER = "postgres"
DB_PASSWORD = "123"

class Watcher:
    def __init__(self, directory_to_watch):
        self.DIRECTORY_TO_WATCH = directory_to_watch
        self.observer = Observer()

    def run(self):
        if not os.path.exists(self.DIRECTORY_TO_WATCH):
            print(f"Error: The directory {self.DIRECTORY_TO_WATCH} does not exist.")
            return
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=False)
        self.observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()

class Handler(FileSystemEventHandler):
    @staticmethod
    def log_event(event_type, src_path):
        try:
            connection = psycopg2.connect(
                host=DB_HOST,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            cursor = connection.cursor()
            query = """
                INSERT INTO file_events (event_type, file_path, event_time)
                VALUES (%s, %s, %s);
            """
            cursor.execute(query, (event_type, src_path, datetime.now()))
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            print(f"Error: {e}")

    @staticmethod
    def on_created(event):
        if event.is_directory:
            return None
        else:
            print(f"{datetime.now()} - Created file: {event.src_path}")
            Handler.log_event("created", event.src_path)

    @staticmethod
    def on_deleted(event):
        if event.is_directory:
            return None
        else:
            print(f"{datetime.now()} - Deleted file: {event.src_path}")
            Handler.log_event("deleted", event.src_path)

    @staticmethod
    def on_modified(event):
        if event.is_directory:
            return None
        else:
            print(f"{datetime.now()} - Modified file: {event.src_path}")
            Handler.log_event("modified", event.src_path)

if __name__ == '__main__':
    path = "C:\\mvd\\files"
    w = Watcher(path)
    w.run()

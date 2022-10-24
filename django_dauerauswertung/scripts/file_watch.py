from dataclasses import dataclass
from datetime import datetime
from os import system
import logging
import logging.config

import sys
import time

import sched, time
from threading import Thread

from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, FileSystemEventHandler

from scripts.process_svantek_file import Messfiletyp, process_data_file


@dataclass
class WatchedFolder:
    target_dir: str
    typ: Messfiletyp
    project_name: str
    messpunkt_name: str
    id_in_db: int




class PrcoessFileEventHandler(FileSystemEventHandler):
    # watched_folder: WatchedFolder
    # last_notification: datetime = datetime(1900, 1, 1)
    #dead_after_seconds = 900

    def __init__(self,watched_folder: WatchedFolder) -> None:
        super().__init__()
        logging.info("Init ProcessEventHandler")
        self.watched_folder: WatchedFolder = watched_folder
        self.last_notification : datetime = datetime(1900, 1, 1)
        self.dead_after_seconds = 1200
        self.frequency_of_checks = 3400

        self.s = sched.scheduler(time.time, time.sleep)
        self.s.enter(10, 1, self.check_is_alive)
        self.thread = Thread(target=self.s.run, daemon=True)
        
        self.thread.start()
        logging.info("Init of ProcessEventHandler finished")

    def on_any_event(self, event):
        try:
            logging.debug(f"Callback svantek-File-Watcher: {event.event_type}, {event.src_path}")
            logging.debug(f"Current queue: {self.s.queue}")
            if not event.is_directory:
                if event.event_type == "modified":
                    if event.src_path.lower().endswith(".csv"):
                            self.last_notification = datetime.now()
                            logging.info(f"Process data of {event.src_path}")
                            process_data_file(event.src_path, self.watched_folder.project_name, self.watched_folder.messpunkt_name, self.watched_folder.typ, self.watched_folder.id_in_db)
                            logging.info("Data successfully processed")
        except PermissionError as ex:
            logging.exception(f"A PermissionError occurred at file {event.src_path}./n{ex}")
        except Exception as ex:
           logging.exception(f"Unhandled error occured  {self.watched_folder}")

    def check_is_alive(self):
        logging.info("Doing stuff...")
        self.s.enter(self.frequency_of_checks, 1, self.check_is_alive)
        logging.info("Scheduled next check")
        if (datetime.now() - self.last_notification).total_seconds() >= self.dead_after_seconds:
            logging.warning(f"Missing files in: {self.watched_folder.messpunkt_name} since {self.last_notification}")

def run():
    watched_folders : list[WatchedFolder] = [
        WatchedFolder("C:/upload/MB_Immendingen/mp-1", Messfiletyp.version_07_21_ohne_wetterdaten, "immendingen", "Immendingen MP 1", 1),
        WatchedFolder("C:/upload/MB_Immendingen/mp-2", Messfiletyp.version_07_21_mit_wetterdaten, "immendingen", "Immendingen MP 2", 2),
        WatchedFolder("C:/upload/MB_Immendingen/mp-3", Messfiletyp.version_07_21_ohne_wetterdaten, "immendingen", "Immendingen MP 3", 3),
        WatchedFolder("C:/upload/MB_Immendingen/mp-4", Messfiletyp.version_07_21_ohne_wetterdaten, "immendingen", "Immendingen MP 4", 4),
        WatchedFolder("C:/upload/MB_Immendingen/mp-5", Messfiletyp.version_07_21_ohne_wetterdaten, "immendingen", "Immendingen MP 5", 5),
        WatchedFolder("C:/upload/MB_Immendingen/mp-6", Messfiletyp.version_07_21_ohne_wetterdaten, "immendingen", "Immendingen MP 6", 6),
        WatchedFolder("C:/upload/DT_MA/mp-2", Messfiletyp.version_07_21_ohne_wetterdaten, "mannheim", "Mannheim MP 1", 7),
    ]

    observer = Observer()
    for w in watched_folders:
        event_handler = PrcoessFileEventHandler(w)
        observer.schedule(event_handler, w.target_dir, recursive=False)
    observer.start()
    logging.info("Started watching..")
    try:
        iteration = 0
        while True:
            time.sleep(300)
            iteration += 1
            logging.info(f"Still watching in iteration {iteration}...")
    except KeyboardInterrupt:
        logging.info(f"User ended watching in iteration {iteration}...")
        observer.stop()
    observer.join()


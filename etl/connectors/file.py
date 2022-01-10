import csv
import logging
import os
import shutil
from etl.common.transform import coroutine


class File():
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.file = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.file:
            self.file.close()

        self.log.info("Finished")

    def move_file(self, file_path: str, destination: str, overwrite: bool = False):
        self.log.info(f"Moving file: {file_path}")
        try:
            shutil.move(file_path, destination)
        except shutil.Error as e:
            if overwrite is True:
                pass
            else:
                raise e

    def clean_file(self, file_path: str):
        self.log.info(f"Deleting file: {file_path}")
        os.remove(file_path)
import logging
import os
import shutil
from typing import List

log = logging.getLogger(__name__)


def move_files(files: List, destination: str, overwrite: bool = False):
    for file in files:
        log.info(f"Moving file: {file}")
        try:
            shutil.move(file, destination)
        except shutil.Error as e:
            if overwrite is True:
                pass
            else:
                raise e


def clean_files(files: List):
    for file in files:
        log.info(f"Deleting file: {file}")
        os.remove(file)


def make_directory(directories: List):
    for directory in directories:
        os.mkdir(directory)
from etl.connectors.file import File
from typing import List
import csv


def move_files(files: List, destination: str, overwrite: bool = False):
    with File() as f:
        for file in files:
            f.move_file(file_path=file, destination=destination, overwrite=overwrite)


def clean_files(files: List):
    with File() as f:
        for file_path in files:
            f.clean_file(file_path=file_path)
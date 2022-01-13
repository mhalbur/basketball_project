import logging
import os
import shutil
from typing import List, Dict
import gzip
import csv
from etl.functions.transform import format_dict

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


def write_gzipped_csv_dict(file_path: str, data: Dict, fields: List):
    with gzip.open(file_path, 'wt', compresslevel=6) as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        for row in data:
            parsed_player = format_dict(row=row, fields=fields)
            writer.writerow(parsed_player)

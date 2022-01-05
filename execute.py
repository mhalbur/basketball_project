import logging
import os
import sys

import arrow
import yaml
from rich.logging import RichHandler


class Executor():
    def __init__(self):
        self.configuration_path = "pipelines"
        self.project = sys.argv[1].lower()
        self.package_path = f"projects.{self.project}.tasks"
        self.job = sys.argv[2].lower()
        self.path = f"{self.configuration_path}/{self.project}.yaml"
        print(f'Prepping to run {self.project}:{self.job}...')

    def read_yaml_file(self):
        with open(self.path) as file:
            return yaml.load(file, Loader=yaml.FullLoader)

    def find_yaml_job(self):
        jobs = self.read_yaml_file()['Jobs']
        for job in jobs:
            if job['Name'].lower() == self.job:
                return job

    def extract_job_details(self):
        job = self.find_yaml_job()
        try:
            function = job['Function']
            log.info(f'Running {function} from {self.package_path}...')
            return function
        except TypeError:
            log.exception(f'{self.job} not found in {self.project}')

    def run_job(self):
        try:
            function = self.extract_job_details()
            module = __import__(self.package_path, fromlist=[function])
            return getattr(module, function)
        except Exception:
            log.exception(Exception)


if __name__ == "__main__":
    log_path = f'/tmp/logs/{sys.argv[1].lower()}'
    log_directory = os.path.isdir(log_path)

    if not log_directory:
        os.mkdir(path=log_path, mode=0o775)

    logging.basicConfig(level="NOTSET",
                        format='%(asctime)s %(levelname)s:%(threadName)s %(name)s \n%(message)s',
                        datefmt="[%X]",
                        handlers=[logging.FileHandler(f'{log_path}/{arrow.utcnow().format("YYYY_MM_DD_HH_mm_ss")}.txt'),
                                  RichHandler(rich_tracebacks=True)])
    log = logging.getLogger("executor")

    method = Executor().run_job()
    method()

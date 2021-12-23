"""(python -m execute project job args1...)"""
import sys 
import yaml

class Executor():
    def __init__(self):
        self.configuration_path = "pipelines"
        self.project = sys.argv[1].lower()
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
        


    def run_job():
        return ''

Executor().find_yaml_job()
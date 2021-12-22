"""(python -m execute project job args1...)"""
import sys 
import ruamel.yaml
from pathlib import Path 

class Executor():
    def __init__(self):
        self.configuration_path = "pipelines"
        self.project = sys.argv[1].lower()
        self.job = sys.argv[2].lower()
        self.path = f"{self.configuration_path}/{self.project}.yaml"
        print(f'Prepping to run {self.project}:{self.job}...')

    def read_yaml_file(self):
        yaml = ruamel.yaml.YAML()
        print(yaml)
        yaml.allow_duplicate_keys = True
        with open(self.path) as fp:
            data = yaml.load(fp)
            print(data)

    def find_yaml_job():
        return ''


    def get_job_details():
        return ''


    def run_job():
        return ''

Executor().read_yaml_file()
"""(python -m execute project job args1...)"""
import sys
import yaml

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
            print(f'Running {function} from {self.package_path}...')
            return function
        except TypeError:
            raise Exception(f'{self.job} not found in {self.project}')
     
    def run_job(self):
        function = self.extract_job_details()
        module =  __import__(self.package_path, fromlist=[function])
        return getattr(module, function)


if __name__ == "__main__":
    method = Executor().run_job()
    method()

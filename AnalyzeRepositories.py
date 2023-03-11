import time
import os
import requests
from IPython.display import clear_output, display
import pandas as pd
import AnalyzeFmuFiles

# Helper class for a file to hold results
class ResultStore:
    def __init__(self, file_results_filepath, results_filepath):
        self.results_filepath = results_filepath
        self.file_results = AnalyzeFmuFiles.ResultStore(file_results_filepath)

        if os.path.exists(self.results_filepath):
            self.df = pd.read_csv(
                self.results_filepath,
                dtype = {
                    'Repository': pd.StringDtype(),
                    'License': pd.StringDtype(),
                },
                index_col = 'Repository')
        else:
            self.df = pd.DataFrame()

        self.succeeded_count = 0
        self.failed_count = 0
        self.preexisting_count = 0

    def check_for_preexisting_result(self, repository):
        is_preexisting = repository in self.df.index
        if is_preexisting:
            self.preexisting_count += 1
        return is_preexisting

    def add_result(self, repository, license):
        new_row = pd.DataFrame(
            {
                'Repository': pd.Series(repository, dtype = pd.StringDtype()),
                'License': pd.Series(license, dtype = pd.StringDtype()),
            })
        new_row.set_index('Repository', inplace=True)
        self.df = pd.concat([self.df, new_row])

        if license.startswith('Error:'):
            self.failed_count += 1
        else:
            self.succeeded_count += 1

    def print_stats(self):
        print(f'Analyzed {self.succeeded_count} analyzed repositories, {self.failed_count} exceptions, {self.preexisting_count} skipped (already cached)')
        if len(self.df) > 0:
            all_succeeded_count = len(self.df[self.df['License'].str.startswith('Error:') == False])
            print(f'The entire collection now has {all_succeeded_count} repositories that were analyzed successfully')
            if all_succeeded_count < len(self.df):
                print('\Errors:')
                invalid = self.df[self.df['License'].str.startswith('Error:') == True]
                for index, row in invalid.iterrows():
                    print(f'{index}: {row["License"]}')

    def save(self):
        self.df.sort_index(inplace=True)
        self.df.to_csv(self.results_filepath, index_label='Repository')

class AnalyzeRepositories:
    def __init__(self, file_results_filepath, results_filepath, is_testing=False):
        self.result_store = ResultStore(file_results_filepath, results_filepath)
        self.is_testing = is_testing

        # Get GitHub token from environment variable.
        self.github_token = os.environ.get('GITHUB_TOKEN')
        if self.github_token is None:
            raise Exception('Environment variable GITHUB_TOKEN must be set to a GitHub personal access token. See https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token.')

    def print_status(self, current_status):
        clear_output()
        print(current_status)
        self.result_store.print_stats()

    def analyze(self):
        already_checked = set()
        for index, row in self.result_store.file_results.df.iterrows():
            split_index = index.split('\\')
            repository = f'{split_index[1]}/{split_index[2]}'
            if repository in already_checked:
                continue
            already_checked.add(repository)
            if self.result_store.check_for_preexisting_result(repository):
                continue

            new_result_count = self.result_store.succeeded_count + self.result_store.failed_count
            status_after_number_of_results = 10
            if new_result_count % status_after_number_of_results == 0:
                self.print_status(f'Analyzing {repository}')

            self._analyze_repository(repository)

            max_testing_files = 5
            if self.is_testing and new_result_count >= max_testing_files:
                print(f'Limiting to {max_testing_files} files in testing mode')
                break

            # Save results to file every so often
            save_after_number_of_results = 2 if self.is_testing else 10
            if new_result_count % save_after_number_of_results == 0:
                self.result_store.save()

        self.result_store.save()
        self.print_status(f'Done')
        return self.result_store.df

    def _analyze_repository(self, repository):
        url = f'https://api.github.com/repos/{repository}'
        print(f'Getting: {url}')
        headers = {'Authorization': f'token {self.github_token}'}
        response = requests.get(url, headers=headers)
        print(f'Result: status code = {response.status_code}, url = {response.url}')
        if response.status_code == 200:
            license_json = response.json()['license']
            license = license_json['key'] if license_json is not None else 'none'
            self.result_store.add_result(repository, license)
        else:
            message = f'Error: {response.text}'
            print(message)
            self.result_store.add_result(repository, message)

        # Rate limiting
        min_sleep_time = .1
        sleep_time = min_sleep_time
        ratelimit_remaining = int(response.headers['X-Ratelimit-Remaining'])
        if ratelimit_remaining == 0:
            reset_time = int(response.headers['X-Ratelimit-Reset'])
            current_time = int(time.time())
            time_to_sleep = reset_time - current_time
            print(f'Rate limit exceeded. Sleeping for {time_to_sleep} seconds')
            sleep_time += time_to_sleep
        time.sleep(sleep_time)

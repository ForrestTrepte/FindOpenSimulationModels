import time
import os
import requests
from collections import namedtuple

class DownloadGitHubFiles:
    def __init__(self, url_list_filepath, is_testing=False):
        self.url_list_filepath = url_list_filepath
        self.is_testing = is_testing

        # Get GitHub token from environment variable.
        self.github_token = os.environ.get('GITHUB_TOKEN')
        if self.github_token is None:
            raise Exception('Environment variable GITHUB_TOKEN must be set to a GitHub personal access token. See https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token.')

    def download(self):
        download_count = 0
        with open(self.url_list_filepath, 'r') as f:
            for line in f:
                if self._process_file(line.strip()):
                    download_count += 1

                    # Don't do lots of downloading in testing mode
                    max_testing_files = 2
                    if self.is_testing and download_count >= max_testing_files:
                        print(f'Limiting to {max_testing_files} file downloads in testing mode')
                        return

                    # Avoid hitting GitHub with too many rapid-fire requests.
                    # It seems that because the downloads (https://raw.githubusercontent.com) are not part of the GitHub API URL, the rate limit headers (x-ratelimit*) are not included.
                    # According to https://stackoverflow.com/questions/66522261/does-github-rate-limit-access-to-public-raw-files, there may be a rate limit of 5000 requests per hour.
                    sleep_time = .1 if self.is_testing else 60 * 60 / 5000 * 3 # quick results when testing, 3x the rate limit when downloading all the data
                    time.sleep(sleep_time)

    def _process_file(self, github_file_url):
        # TODO: Rate limiting

        parsed_url = self._parse_url(github_file_url)
        local_filepath = self._get_local_filepath(parsed_url)
        if os.path.exists(local_filepath):
            print(f'Already downloaded {local_filepath}')
            return False
        download_url = self._get_download_url(parsed_url)
        self._download(download_url, local_filepath)
        return True

    ParsedUrl = namedtuple('ParsedUrl', ['owner', 'repo', 'commit_hash', 'filepath'])
    def _parse_url(self, github_file_url):
        # Example input: https://github.com/AIT-IES/FMITerminalBlock/blob/143403d06934acba5e434d7cb5a9158545e95f13/doc/user/tutorial-data/model/Sisyphus.fmu
        # owner = AIT-IES
        # repo = FMITerminalBlock
        # commit hash = 143403d06934acba5e434d7cb5a9158545e95f13
        # filepath = doc/user/tutorial-data/model/Sisyphus.fmu
        split = github_file_url.split('/')
        result = self.ParsedUrl(
            owner = split[3],
            repo = split[4],
            commit_hash = split[6],
            filepath = '/'.join(split[7:]),
        )
        assert(split[5] == 'blob')
        return result
    
    def _get_contents_info_url(self, parsed_url):
        # Convert the GitHub URL to a URL that provides metadata about the file
        # Example input: https://github.com/AIT-IES/FMITerminalBlock/blob/143403d06934acba5e434d7cb5a9158545e95f13/doc/user/tutorial-data/model/Sisyphus.fmu
        # Example output: https://api.github.com/repos/AIT-IES/FMITerminalBlock/contents/doc/user/tutorial-data/model/Sisyphus.fmu?ref=143403d06934acba5e434d7cb5a9158545e95f13
        result = f'https://api.github.com/repos/{parsed_url.owner}/{parsed_url.repo}/contents/{parsed_url.filepath}?ref={parsed_url.commit_hash}'
        return result

    def _get_download_url(self, parsed_url):
        # Convert the GitHub URL to a download URL
        # Example input: https://github.com/AIT-IES/FMITerminalBlock/blob/143403d06934acba5e434d7cb5a9158545e95f13/doc/user/tutorial-data/model/Sisyphus.fmu
        # Example output: https://raw.githubusercontent.com/AIT-IES/FMITerminalBlock/143403d06934acba5e434d7cb5a9158545e95f13/doc/user/tutorial-data/model/Sisyphus.fmu
        result = f'https://raw.githubusercontent.com/{parsed_url.owner}/{parsed_url.repo}/{parsed_url.commit_hash}/{parsed_url.filepath}'
        return result

    def _get_local_filepath(self, parsed_url):
        # Convert the GitHub URL to a local file path
        # Example input: https://github.com/AIT-IES/FMITerminalBlock/blob/143403d06934acba5e434d7cb5a9158545e95f13/doc/user/tutorial-data/model/Sisyphus.fmu
        # Example output if url_list_filepath is "results/urls.txt": results/downloads/AIT-IES/FMITerminalBlock/doc_user_tutorial-data_model_Sisyphus.fmu
        url_list_directory = os.path.dirname(self.url_list_filepath)
        filepath_with_underscores = '_'.join(parsed_url.filepath.split('/'))
        result = f'{url_list_directory}/downloads/{parsed_url.owner}/{parsed_url.repo}/{filepath_with_underscores}'
        return result

    def _download(self, download_url, local_filepath):
        headers = {'Authorization': f'token {self.github_token}'}
        print(f'Downloading {download_url} to {local_filepath}')
        response = requests.get(download_url, headers=headers, stream=True)

        if response.status_code == 200:
            # Stream the content of the response to a local file
            # Create directory of local file, if needed
            os.makedirs(os.path.dirname(local_filepath), exist_ok=True)
            with open(local_filepath, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
        else:
            # Print the error message if the request failed
            print(f'Request {download_url} failed: {response.status_code} - {response.text}')

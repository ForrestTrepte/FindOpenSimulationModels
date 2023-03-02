import os
import requests

class DownloadGitHubFiles:
    def __init__(self, url_list_filepath, is_testing=False):
        self.url_list_filepath = url_list_filepath
        self.is_testing = is_testing

        # Get GitHub token from environment variable.
        self.github_token = os.environ.get('GITHUB_TOKEN')
        if self.github_token is None:
            raise Exception('Environment variable GITHUB_TOKEN must be set to a GitHub personal access token. See https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token.')

    def download(self):
        file_index = 0
        with open(self.url_list_filepath, 'r') as f:
            for line in f:
                self._process_file(line.strip())
                file_index += 1

                # Don't do lots of downloading in testing mode
                max_testing_files = 3
                if self.is_testing and file_index >= max_testing_files:
                    print(f'Limiting to {max_testing_files} file downloads in testing mode')
                    return

    def _process_file(self, github_file_url):
        # TODO: Cache previously downloaded files
        # TODO: Rate limiting

        download_url = self._convert_to_download_url(github_file_url)
        local_filepath = self._convert_to_local_filepath(github_file_url)
        self._download(download_url, local_filepath)

    def _convert_to_download_url(self, github_file_url):
        # Convert the GitHub URL to a download URL
        # Example input: https://github.com/AIT-IES/FMITerminalBlock/blob/143403d06934acba5e434d7cb5a9158545e95f13/doc/user/tutorial-data/model/Sisyphus.fmu
        # Example output: https://raw.githubusercontent.com/AIT-IES/FMITerminalBlock/143403d06934acba5e434d7cb5a9158545e95f13/doc/user/tutorial-data/model/Sisyphus.fmu
        #                 https://raw.githubusercontent.com/AIT-IES/FMITerminalBlock/blob/143403d06934acba5e434d7cb5a9158545e95f13/doc/user/tutorial-data/model/Sisyphus.fmu
        # The download URL is the same as the GitHub URL, except that the 'github.com' part is replaced with 'raw.githubusercontent.com' and the 'blob/' part is removed
        download_url = github_file_url.replace('github.com', 'raw.githubusercontent.com')
        download_url = download_url.replace('/blob/', '/')
        return download_url

    def _convert_to_local_filepath(self, github_file_url):
        # Convert the GitHub URL to a local file path
        # Example input: https://github.com/AIT-IES/FMITerminalBlock/blob/143403d06934acba5e434d7cb5a9158545e95f13/doc/user/tutorial-data/model/Sisyphus.fmu
        # Example output if url_list_filepath is "results/urls.txt": results/downloads/AIT-IES/FMITerminalBlock/doc_user_tutorial-data_model_Sisyphus.fmu
        url_list_directory = os.path.dirname(self.url_list_filepath)
        split = github_file_url.split('/')
        # https://github.com is split[0 to 2]
        owner = split[3]
        repo = split[4]
        # blob/### is split[5 to 6]
        filepath = '_'.join(split[7:])
        local_filepath = f'{url_list_directory}/downloads/{owner}/{repo}/{filepath}'
        return local_filepath

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
            print(f'Request failed: {response.status_code} - {response.text}')

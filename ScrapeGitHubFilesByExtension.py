import time
import os
from IPython.display import clear_output
from collections import namedtuple
from sortedcontainers import SortedSet
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import urllib.parse

# Helper class for a file to hold results
class ResultStore:
    def __init__(self, filename):
        self.filename = filename
        self.results = SortedSet()
        if os.path.exists(self.filename):
            with open(self.filename, 'r') as f:
                for line in f:
                    self.results.add(line.strip())
        self.new_results = 0
        self.preexisting_results = 0

    def add_result(self, result):
        if result in self.results:
            self.preexisting_results += 1
            return
        self.results.add(result)
        self.new_results += 1

    def print_stats(self):
        print(f'This scan has found {self.new_results} new FMUs, {self.preexisting_results} already known FMUs')
        print(f'The entire collection now has {len(self.results)} FMUs')

    def save(self):
        with open(self.filename, 'w') as f:
            for result in self.results:
                f.write(result + '\n')

class ScrapeGitHubFilesByExtension:
    def __init__(self, extension, results_filename, is_testing=False):
        self.is_testing = is_testing
        self.extension = extension
        self.result_store = ResultStore(results_filename)

        # Create a new instance of the Chrome browser
        self.driver = webdriver.Chrome()

        # Navigate to the GitHub website
        search_url = f'https://github.com/search?q=extension%3A{extension}&type=code'
        print(f'Opening: {urllib.parse.unquote(search_url)}')
        self.driver.get(search_url)

        # It might be a good idea to automate the user login (if that's possible), but for now do it manually
        print('Use the web browser window to log in to GitHub...')

        # Wait for the URL to change to the search page
        WebDriverWait(self.driver, 180).until(EC.url_to_be(search_url))

        print('First page of search results loaded')

        # Find the heading containing the count of all search results
        h3_elements = self.driver.find_elements(By.CSS_SELECTOR, 'h3')
        for h3_element in h3_elements:
            if 'results' in h3_element.text:
                print(h3_element.text)

        # Get number of pages of results
        current_em = self.driver.find_element(By.CSS_SELECTOR, 'em.current')
        self.page_count = int(current_em.get_attribute('data-total-pages'))
        print(f'Found {self.page_count} pages of results')
        if is_testing:
            print(f'Limiting to 3 pages for testing purposes')
            self.page_count = min(self.page_count, 3)

    def __del__(self):
        self.driver.close()

    def _scrape_page_results(self):
        # Get the list items containing the search results (divs with class "code-list-item")
        item_divs = self.driver.find_elements(By.CSS_SELECTOR, 'div[class*="code-list-item"]')
        for item_div in item_divs:
            # Get the link to the FMU file (not the secondary one to the repository)
            item_links = item_div.find_elements(By.CSS_SELECTOR, 'a:not(.Link--secondary)')
            if (len(item_links) != 1):
                print(f'Warning: Parsing problem. Search result item contains {len(item_links)} links, expected just 1 link to the FMU file. Something may have changed on the GitHub website.')
            for link in item_links:
                self.result_store.add_result(link.get_attribute("href"))

        expected_number_of_results = 10
        if len(item_divs) < expected_number_of_results:
            print(f'Warning: Search page only has {len(item_divs)} items, expected {expected_number_of_results} items.')
            return False
        return True

    def scrape(self):
        # It looks like we're limited to 100 pages so, unfortunately, we won't be able to get all the results using this method.
        # We'll try using different search orders (best/indexed, ascending/descending) to give us different subsets of results.
        # Best match seems to return results inconsitently. Not sure if ascending/descending has an effect, but we'll try both.
        max_sort_order = 4
        def get_search_page_url(page, sort_order, extension='fmu'):
            assert 1 <= sort_order <= max_sort_order, f'order must be between 1 and {max_sort_order}'
            index = sort_order - 1

            order_options = [
                'asc', # ascending
                'desc', # descending
            ]
            order = order_options[index % 2]

            sort_options = [
                'indexed', # recently indexed
                '', # best match
            ]
            sort = sort_options[index // 2]

            url = f'https://github.com/search?o={order}&p={page}&q=extension%3A{extension}&s={sort}&type=Code'
            return url

        # Strangely, search pages sometimes fail to load, returning 0 of a small number of results. Retry a few times and track how often this occurs.
        retry_count = 0
        max_retry_count = 1 if self.is_testing else 15
        PageRetryRecord = namedtuple('PageRetryRecord', ['page', 'retry_count', 'succeeded'])
        page_retry_data = []

        # Process each sort order
        for sort_order in range(1, max_sort_order + 1):
            # Process each page of the search results
            # Note that we're assuming each search order has the same number of pages. That might not be correct,
            # but in practice we always seem to be hitting a limit of 100 pages, so it shouldn't matter.
            for current_page in range(1, self.page_count + 1):
                page_url = get_search_page_url(current_page, sort_order)

                clear_output(wait=True)
                print(f'Scraping page {current_page}/{self.page_count} order {sort_order}/{max_sort_order}: {urllib.parse.unquote(page_url)}')
                self.result_store.print_stats()

                self.driver.get(page_url)
                WebDriverWait(self.driver, 10).until(EC.url_to_be(page_url))
                succeeded = self._scrape_page_results()
                if succeeded or retry_count >= max_retry_count:
                    # Move on to next page
                    page_retry_data.append(PageRetryRecord(current_page, retry_count, succeeded))
                    retry_count = 0

                    # Save results to file every so often
                    save_after_number_of_pages = 2 if self.is_testing else 10
                    if current_page % save_after_number_of_pages == 0:
                        self.result_store.save()
                else:
                    # Failed, repeat this page
                    retry_count += 1
                    print(f'Retrying ({retry_count}/{max_retry_count})')

                # Avoid hitting GitHub with too many rapid-fire requests.
                # GitHub defines rate limits for the API, such as 10 requests per minute for unauthenticated requests.
                # But https://api.github.com/rate_limit doesn't seem to be affected by scraping the web site and it
                # isn't clear how rate limits are handled. Let's be cautious.
                sleep_time = 3 if self.is_testing else 20 # quick results when testing, 20 seconds when downloading all the data (seems like would be reasonable for a human to read each page in this amount of time)
                time.sleep(sleep_time) # 20 seconds would be plenty of time for a human to read each page of the search results

        self.result_store.save()

        clear_output(wait=True)
        print(f'Done scraping {self.page_count} pages * {max_sort_order} orders')
        self.result_store.print_stats()
        print()
        print("Retries:")
        for i in range(0, max_retry_count+1):
            print(f'  succeeded after {i} retries: {sum(1 if x.retry_count == i else 0 for x in page_retry_data)} pages')
        print(f'  failed: {sum(1 if not x.succeeded else 0 for x in page_retry_data)} pages')

%pip install beautifulsoup4 selenium webdriver_manager azure-storage-blob

# %%
import os
from pathlib import Path
import random
import time
import pickle

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver import ChromeOptions

from webdriver_manager.chrome import ChromeDriverManager
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


POSTINGS_LIST_FOLDER = Path("./postings_list")
POSTINGS_LIST_FOLDER.mkdir(exist_ok=True)

# %%


class RandomSleep:
    def short():
        time.sleep(abs(random.normalvariate(1, 0.5)))

    def long():
        time.sleep(abs(random.normalvariate(7, 2)))


def get_list_of_postings():
    def initial_scroll(driver, start_time, times_to_scroll=20):
        """Scrolls until 'See more jobs' button limits scrolling."""

        for _ in range(times_to_scroll):
            webdriver.ActionChains(driver).send_keys(Keys.PAGE_DOWN).perform()
            RandomSleep.short()

        time_since_start = time.time() - start_time
        print(
            f"Finished initial scroll (long one), min elapsed: {time_since_start/60:.1f}"
        )

    def click_show_more_button_and_scroll(driver, start_time, timeout_in_mins=10):
        """Presses show more button and scrolls until button pops up to limit again, then repeats. Currently breaks on exception when this function works which is a bit unintuitive."""

        class Timer:
            def get_time_since_start():
                return time.time() - start_time

            @classmethod
            def check_if_timed_out(cls, timeout_in_mins) -> bool:
                timed_out = cls.get_time_since_start() > timeout_in_mins * 60
                if timed_out:
                    print(
                        "click_show_more_button_and_scroll() timed out without reaching ElementNotInteractableException aka no longer finding button"
                    )
                return timed_out

            @classmethod
            def print_progress_info(cls, n_button_presses):
                if n_button_presses % 5 == 0:
                    print(
                        f"Min elapsed: {cls.get_time_since_start()/60:.1f}, Number of button presses:{n_button_presses}",
                    )

        show_more_button_selector = (
            r"#main-content > section.two-pane-serp-page__results-list > button"
        )

        timed_out = False
        n_button_presses = 0
        while not timed_out:
            try:
                # press button and scroll until new button limits again, repeat
                show_more_button = driver.find_element_by_css_selector(
                    show_more_button_selector
                )
                webdriver.ActionChains(driver).click(show_more_button).perform()
                n_button_presses += 1

                for _ in range(5):
                    webdriver.ActionChains(driver).send_keys(Keys.PAGE_DOWN).perform()
                    RandomSleep.short()

            except ElementNotInteractableException:
                print(
                    "Can no longer find button. If script was successful, this is due to reaching end of postings list."
                )
                break

            Timer.print_progress_info(n_button_presses)
            timed_out: bool = Timer.check_if_timed_out(timeout_in_mins)

    linkedin_url = r"https://fi.linkedin.com/jobs/data-analyst-jobs"

    options = ChromeOptions()
    options.set_capability("binary", "./chrome/chrome")
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    driver.get(linkedin_url)
    RandomSleep.long()

    start_time = time.time()
    initial_scroll(driver, start_time)
    click_show_more_button_and_scroll(driver, start_time)

    RandomSleep.long()
    page_html = driver.execute_script(
        "return document.getElementsByTagName('html')[0].innerHTML"
    )
    return page_html


def parse_html(html):
    def parse_sidebar_item(list_item):
        anchor = list_item.find("a")
        link = anchor.attrs["href"]

        metadata = list_item.find("div", attrs={"class": "base-search-card__info"})
        job_title = metadata.find("h3").text.strip()
        company_name = metadata.find("h4").text.strip()
        location = metadata.find("span").text.strip()
        listing_date = metadata.find("time").attrs["datetime"]

        return {
            "link": link,
            "job_title": job_title,
            "company_name": company_name,
            "location": location,
            "listing_date": listing_date,
        }

    soup = BeautifulSoup(html, "lxml")
    jobs_list = soup.find("ul", attrs={"class": "jobs-search__results-list"})
    jobs_list_items = jobs_list.find_all("li")
    parsed_list_items = [parse_sidebar_item(x) for x in jobs_list_items]

    return parsed_list_items


def write(parsed_list_of_postings, postings_list_folder=POSTINGS_LIST_FOLDER):
    def get_new_file_number(already_gathered_postings_lists):
        file_numbers = [
            int(filepath.stem[-1]) for filepath in already_gathered_postings_lists
        ]
        return max(file_numbers) + 1

    already_gathered_postings_lists = list(postings_list_folder.glob("*"))

    if len(already_gathered_postings_lists) == 0:
        # create first one
        filepath = postings_list_folder.joinpath("postings_list1.pkl")
        with open(filepath, "wb") as fp:
            pickle.dump(parsed_list_of_postings, fp)
    else:
        new_file_number = get_new_file_number(already_gathered_postings_lists)
        filepath = postings_list_folder.joinpath(f"postings_list{new_file_number}.pkl")

        with open(filepath, "wb") as fp:
            pickle.dump(parsed_list_of_postings, fp)


def read(postings_list_folder=POSTINGS_LIST_FOLDER):
    paths = list(postings_list_folder.glob("*"))
    postings_lists = []
    for p in paths:
        with open(p, "rb") as fp:
            postings_lists.append(pickle.load(fp))
    return postings_lists


# %%
def download_chrome_files():
    blob_service_client = BlobServiceClient(
        r"https://kparchrome.blob.core.windows.net/"
    )

    container_client = blob_service_client.get_container_client("chrome")
    blob_list = container_client.list_blobs()
    n_blobs = len(blob_list)
    i = 0
    for blob in blob_list:
        print("Blob", i, "/", n_blobs)

        blob_client = container_client.get_blob_client(blob)

        filepath = blob.name  # .split("/")

        # if double chrome/chrome/ replace one
        filepath = filepath.replace("chrome/chrome/", "chrome/")
        filepath = Path(filepath)

        # create parent dirs, otherwise dir doesn't exist error
        parents_path = Path("/".join(filepath.parts[:-1]))
        parents_path.mkdir(parents=True, exist_ok=True)

        with open(filepath, mode="wb") as sample_blob:
            download_stream = blob_client.download_blob()
            sample_blob.write(download_stream.readall())


# %%

chrome_binary_exists = Path("chrome", "chrome").exists()
if not chrome_binary_exists:
    download_chrome_files()

html = get_list_of_postings()
parsed_list_of_postings = parse_html(html)
write(parsed_list_of_postings, POSTINGS_LIST_FOLDER)
read(POSTINGS_LIST_FOLDER)

# %%
from dataclasses import dataclass
from pathlib import Path
import random
import re
import time
import pickle

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from azure_credentials import blob_client
from azure_io import upload_file_to_blob
from data_paths import POSTINGS_LIST_PATHS


# used for deduplication
IDENTIFYING_COLUMNS = [
    "job_title",
    "company_name",
    "location",
    "listing_date",
]


# %%
class RandomSleep:
    @staticmethod
    def short():
        time.sleep(abs(random.normalvariate(1, 0.5)))

    @staticmethod
    def long():
        time.sleep(abs(random.normalvariate(7, 2)))


def get_list_of_postings(timeout_in_mins=10):
    def initial_scroll(driver, start_time, times_to_scroll=20):
        """Scrolls until 'See more jobs' button limits scrolling."""

        for _ in range(times_to_scroll):
            webdriver.ActionChains(driver).send_keys(Keys.PAGE_DOWN).perform()
            RandomSleep.short()

        time_since_start = time.time() - start_time
        print(
            f"Finished initial scroll (long one), min elapsed: {time_since_start/60:.1f}"
        )

    def click_show_more_button_and_scroll(
        driver, start_time, timeout_in_mins=timeout_in_mins
    ):
        """Presses show more button and scrolls until button pops up to limit again, then repeats. Currently breaks on exception when this function works which is a bit unintuitive."""

        class Timer:
            @staticmethod
            def get_time_since_start():
                return time.time() - start_time

            @classmethod
            def check_if_timed_out(cls, timeout_in_mins) -> bool:
                timed_out = cls.get_time_since_start() > timeout_in_mins * 60
                if timed_out:
                    print("Timed out.")
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
                show_more_button = driver.find_element(
                    By.CSS_SELECTOR, show_more_button_selector
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

    print("Initializing driver.")
    # options = ChromeOptions()
    # options.set_capability("binary", BIN_CHROME)
    # options.add_argument("--headless")
    driver = webdriver.Chrome()

    print("Getting url.")
    driver.get(linkedin_url)

    RandomSleep.long()

    print("Starting to scroll.")
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


def get_new_file_number(folder):
    digit_pattern = re.compile(r"\d+")
    filepaths = [str(filepath) for filepath in list(folder.glob("*"))]
    file_numbers = [int(re.search(digit_pattern, fp).group(0)) for fp in filepaths]
    return max(file_numbers) + 1


def get_new_filepath(postings_list_folder=POSTINGS_LIST_PATHS["RAW_FOLDER"]):
    already_gathered_postings_lists = list(postings_list_folder.glob("*"))

    if len(already_gathered_postings_lists) == 0:
        # create first one
        filepath = postings_list_folder.joinpath("postings_list1.csv")

    else:
        new_file_number = get_new_file_number(postings_list_folder)
        filepath = postings_list_folder.joinpath(f"postings_list{new_file_number}.csv")

    return filepath


def deduplicate(new_merged_df, identifying_columns=IDENTIFYING_COLUMNS):
    identifying_columns = new_merged_df.columns.drop("link")
    dupes = new_merged_df.duplicated(subset=identifying_columns)
    deduped = new_merged_df[~dupes]
    dropped_dupes = new_merged_df[dupes].sort_values(new_merged_df.columns.tolist())

    return deduped


def read(postings_list_folder=POSTINGS_LIST_PATHS["RAW_FOLDER"]):
    paths = list(postings_list_folder.glob("*"))
    postings_lists = []
    for p in paths:
        postings_lists.append(pd.read_csv(p, index_col=0))

    return pd.concat(postings_lists)


def add_new_postings_into_previous_ones(
    df_list_of_postings, path=POSTINGS_LIST_PATHS["MERGED"]
) -> None:
    if path.exists():
        # merge new postings with previously merged postings
        old_merged_df = pd.read_csv(path, index_col=0)
        df_list_of_postings = pd.concat([old_merged_df, df_list_of_postings])

    # deduplicate
    deduped = deduplicate(df_list_of_postings)

    # overwrite old one with new one
    deduped.to_csv(path)


# %%
def get_and_write():
    # get postings and turn to df
    html = get_list_of_postings()
    parsed_list_of_postings = parse_html(html)
    df_list_of_postings = pd.DataFrame(parsed_list_of_postings)

    # save this run's postings locally
    new_filepath = get_new_filepath()
    df_list_of_postings.to_csv(new_filepath)

    # merge this run's postings with previous runs and save to file
    add_new_postings_into_previous_ones(df_list_of_postings)

    # upload merged results to azure
    upload_file_to_blob(blob_client)

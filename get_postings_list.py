# %%
from pathlib import Path
import random
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

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from azure_credentials import storage_account_name, container_name, key


# %%
POSTINGS_LIST_FOLDER_PKL = Path("./postings_list/pkl")
POSTINGS_LIST_FOLDER_PKL.mkdir(exist_ok=True)

POSTINGS_LIST_FOLDER_CSV = Path("./postings_list/csv")
POSTINGS_LIST_FOLDER_CSV.mkdir(exist_ok=True)

BIN_CHROME = r"./chrome/chrome"

for path in [POSTINGS_LIST_FOLDER_PKL, BIN_CHROME]:
    if not Path(path).exists():
        raise Exception(f"{path=} doesn't exist.")


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


def write(parsed_list_of_postings, postings_list_folder=POSTINGS_LIST_FOLDER_PKL):
    def get_new_file_number(already_gathered_postings_lists):
        file_numbers = [
            int(filepath.stem[-1]) for filepath in already_gathered_postings_lists
        ]
        return max(file_numbers) + 1

    print("Writing results to file.")
    already_gathered_postings_lists = list(postings_list_folder.glob("*"))

    # get write filepath
    if len(already_gathered_postings_lists) == 0:
        # create first one
        filepath = postings_list_folder.joinpath("postings_list1.csv")

    else:
        new_file_number = get_new_file_number(already_gathered_postings_lists)
        filepath = postings_list_folder.joinpath(f"postings_list{new_file_number}.csv")

    parsed_list_of_postings.to_csv(filepath)


def read(postings_list_folder=POSTINGS_LIST_FOLDER_CSV):
    paths = list(postings_list_folder.glob("*"))
    postings_lists = []
    for p in paths:
        postings_lists.append(pd.read_csv(p, index_col=0))

    return pd.concat(postings_lists)


# %%
def download_chrome_files():
    blob_service_client = BlobServiceClient(
        r"https://kparchrome.blob.core.windows.net/"
    )

    container_client = blob_service_client.get_container_client("chrome")
    blob_list = list(container_client.list_blobs())
    n_blobs = len(blob_list)
    i = 0
    for blob in blob_list:
        print("Blob", i, "/", n_blobs)

        blob_client = container_client.get_blob_client(blob)

        filepath = blob.name  # .split("/")

        # if double chrome/chrome/ replace one
        filepath = filepath.replace("chrome/chrome/", "chrome/")

        # create parent dirs, otherwise dir doesn't exist error
        parents_path = Path("/".join(filepath.parts[:-1]))
        parents_path.mkdir(parents=True, exist_ok=True)

        print(filepath)
        with open(filepath, mode="wb") as sample_blob:
            download_stream = blob_client.download_blob()
            sample_blob.write(download_stream.readall())

        i += 1


# %%
def get_and_write():
    html = get_list_of_postings()
    parsed_list_of_postings = parse_html(html)
    df_list_of_postings = pd.DataFrame(parsed_list_of_postings)
    write(df_list_of_postings, POSTINGS_LIST_FOLDER_CSV)
    # print(df_list_of_postings)


# get_and_write()

df_list_of_postings = read()


# %%  WRITE TO AZURE WITH SPARK

# Init session and download azure dependencies
spark = SparkSession.builder.config(
    "spark.jars.packages",
    "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6",
).getOrCreate()

# authorize azure
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    key,
)

# turn into spark df
df = spark.createDataFrame(df_list_of_postings)
df = df.withColumn("listing_date", df["listing_date"].cast("date"))

# write to azure
df.write.parquet(
    rf"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/test1.parquet/"
)

df_lol = spark.read.parquet(
    rf"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/test1.parquet/"
)


df_lol.show()

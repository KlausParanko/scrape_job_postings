# %%
from pathlib import Path
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import pickle

from get_postings_list import (
    POSTINGS_LIST_PATHS,
    IDENTIFYING_COLUMNS,
    download_blob_to_file,
    get_new_file_number,
)

from azure_credentials import blob_client

POSTINGS_FOLDER = Path("./postings")
POSTINGS_FOLDER.mkdir(exist_ok=True)

# %%


def read_in_postings():
    filepaths = POSTINGS_FOLDER.glob("*")
    postings = []
    for path in filepaths:
        with open(path, "rb") as fp:
            d = pickle.load(fp)
            postings.append(d)
    return postings


def _get_posting_html(driver, list_item: dict, id_for_filename: int, sleep: int):
    link = list_item["link"]

    driver.implicitly_wait(5)
    driver.get(link)
    time.sleep(sleep)

    show_more_button = driver.find_element(
        By.CSS_SELECTOR,
        "#main-content > section.core-rail.mx-auto.papabear\:w-core-rail-width.mamabear\:max-w-\[790px\].babybear\:max-w-\[790px\] > div > div > section.core-section-container.my-3.description > div > div > section > button.show-more-less-html__button.show-more-less-button.show-more-less-html__button--more.\!ml-0\.5",
    )
    show_more_button.click()
    time.sleep(sleep)

    page_html = driver.execute_script(
        "return document.getElementsByTagName('html')[0].innerHTML"
    )

    list_item["posting_html"] = page_html

    filename = "posting" + str(id_for_filename) + ".pkl"
    path = POSTINGS_FOLDER.joinpath(filename)
    with open(path, "wb") as fp:
        pickle.dump(list_item, fp)


def prepare_postings_list():
    """Remove already gotten postings and turn rows into list of dicts."""
    postings_list = pd.read_csv(POSTINGS_LIST_PATHS["MERGED"], index_col=0)
    already_gotten_postings = pd.DataFrame(read_in_postings())
    links_and_metadata = _remove_already_gotten_postings_from_list(
        postings_list, already_gotten_postings
    )
    links_and_metadata = links_and_metadata.to_dict(orient="records")
    return links_and_metadata


    driver = webdriver.Chrome()
    start_id = get_new_file_number(POSTINGS_FOLDER)
    for id, lam in enumerate(links_and_metadata, start=start_id):
        try:
            _get_posting_html(driver, lam, id, sleep=30)
        except:
            print("Hit exception at id:", id)
            time.sleep(5 * 60)
            _get_posting_html(driver, lam, id, sleep=30)


# need to create version with knowledge of which ones I have already gotten and which ones not
# currently saves to dict, probably works okay
# analysis reads in parquet


# remove already parsed from postings_list
def separate_not_yet_gotten_postings_from_list(
    postings_list, postings, identifying_columns=IDENTIFYING_COLUMNS
):
    """No need to get them again."""
    merged = pd.merge(
        postings_list,
        postings[identifying_columns],
        how="left",
        indicator="exists",
    )
    new_postings = merged[merged["exists"] == "left_only"]
    new_postings = new_postings.drop("exists", axis=1)

    return new_postings


# %%
links_and_metadata = prepare_postings_list()
get_all_posting_content(links_and_metadata)

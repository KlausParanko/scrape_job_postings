# %%
from pathlib import Path
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import pickle
from azure_io import download_blob_to_file, upload_file_to_blob

from get_postings_list import (
    IDENTIFYING_COLUMNS,
    get_new_file_number,
)

from azure_credentials import blob_client
from data_paths import POSTINGS_LIST_PATHS, POSTINGS_CONTENT_PATHS

POSTINGS_CONTENT_PATHS["RAW_FOLDER"].mkdir(exist_ok=True)

# %%


def read_in_postings():
    filepaths = POSTINGS_CONTENT_PATHS["RAW_FOLDER"].glob("*")
    postings = []
    for path in filepaths:
        with open(path, "rb") as fp:
            d = pickle.load(fp)
            postings.append(d)
    return postings


def _remove_already_gotten_postings_from_list(
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
    path = POSTINGS_CONTENT_PATHS["RAW_FOLDER"].joinpath(filename)
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


def get_all_posting_htmls(links_and_metadata, n_retries=10):
    def print_run_stats(current_iteration, total_iterations, start_time):
        print(f"{current_iteration=} / {total_iterations=}")
        print(f"Index of postings file: {idx}")
        elapsed_time_in_min = (time.time() - start_time) / 60
        print(f"Elapsed time in min: {elapsed_time_in_min:.0f}")
        average_time_per_iteration = elapsed_time_in_min / (current_iteration + 1)
        print(f"Avg time per iteration: {average_time_per_iteration:.0f}")
        expected_time_left = (
            total_iterations - current_iteration * average_time_per_iteration
        )
        print(f"{expected_time_left=}\n")

    driver = webdriver.Chrome()
    start_idx = get_new_file_number(POSTINGS_CONTENT_PATHS["RAW_FOLDER"])

    current_iteration = 0
    total_iterations = len(links_and_metadata)
    start_time = time.time()
    for idx, lam in enumerate(links_and_metadata, start=start_idx):
        print_run_stats(current_iteration, total_iterations, start_time)

        try:
            _get_posting_html(driver, lam, idx, sleep=30)
        except Exception as e:
            for _ in range(n_retries):
                print(f"Hit exception {e} at \n {lam}")
                time.sleep(5 * 60)
                _get_posting_html(driver, lam, idx, sleep=30)

        current_iteration += 1


def merge_save_and_upload_postings():
    merged = pd.DataFrame(read_in_postings())
    merged.to_parquet(POSTINGS_CONTENT_PATHS["MERGED"])
    upload_file_to_blob(
        blob_client,
        azure_filename=POSTINGS_CONTENT_PATHS["AZURE_FILENAME"],
        local_filepath=POSTINGS_CONTENT_PATHS["MERGED"],
    )


# %%

# links_and_metadata = prepare_postings_list()
# get_all_posting_htmls(links_and_metadata)
# merge_save_and_upload_postings()

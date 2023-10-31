# %%
from pathlib import Path
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import pickle

from get_postings_list import links_and_metadata

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


def get_all_posting_content(links_and_metadata):
    driver = webdriver.Chrome()
    start_id = get_new_file_number(POSTINGS_FOLDER)
    for id, lam in enumerate(links_and_metadata, start=start_id):
        try:
            _get_posting_html(driver, lam, id, sleep=30)
        except:
            print("Hit exception at id:", id)
            time.sleep(5 * 60)
            _get_posting_html(driver, lam, id, sleep=30)


# TODO: check if works
# get_all_posting_content(links_and_metadata)

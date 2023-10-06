# %%
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import os

# %%


def navigate_to_linkedin_jobs_and_get_html():
    """TODO: automate scrolling down and clicking 'show more' buttons."""
    driver = webdriver.Chrome()

    input("Ready?")

    page_html = driver.execute_script(
        "return document.getElementsByTagName('html')[0].innerHTML"
    )
    with open("test.html", "w") as fp:
        fp.write(page_html)


# %%
def parse_sidebar_item(list_item):
    anchor = list_item.find("a")
    link = anchor.attrs["href"]

    metadata = list_item.find("div", attrs={"class": "base-search-card__info"})
    job_title = metadata.find("h3").text.strip()
    company_name = metadata.find("h4").text.strip()
    location = metadata.find("span").text.strip()

    # TODO: add posting age (eg posted 2 weeks ago)

    return {
        "link": link,
        "job_title": job_title,
        "company_name": company_name,
        "location": location,
    }


# navigate_to_linkedin_jobs_and_get_html()

with open("test.html", "r") as fp:
    html = fp.read()

soup = BeautifulSoup(html, "lxml")
jobs_list = soup.find("ul", attrs={"class": "jobs-search__results-list"})

list_item = jobs_list.find_all("li")
parsed_list_items = [parse_sidebar_item(x) for x in list_item]
len(parsed_list_items)

# %%

# get posting text
driver = webdriver.Chrome()
driver.implicitly_wait(5)
driver.get(parsed_list_items[1]["link"])
time.sleep(2)


button = driver.find_element(
    By.CSS_SELECTOR,
    "#main-content > section.core-rail.mx-auto.papabear\:w-core-rail-width.mamabear\:max-w-\[790px\].babybear\:max-w-\[790px\] > div > div > section.core-section-container.my-3.description > div > div > section > button.show-more-less-html__button.show-more-less-button.show-more-less-html__button--more.\!ml-0\.5",
)
button.click()
time.sleep(2)


page_html = driver.execute_script(
    "return document.getElementsByTagName('html')[0].innerHTML"
)

with open("posting.html", "w") as fp:
    fp.write(page_html)

# refactor

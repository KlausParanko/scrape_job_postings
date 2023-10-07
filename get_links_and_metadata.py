# %%
from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver

HTML_CONTAINING_LIST_OF_POSTINGS = Path("./list_of_job_postings.html")


# %%
def get_html(save_path=HTML_CONTAINING_LIST_OF_POSTINGS):
    driver = webdriver.Chrome()

    input("Ready?")

    page_html = driver.execute_script(
        "return document.getElementsByTagName('html')[0].innerHTML"
    )

    with open(save_path, "w") as fp:
        fp.write(page_html)


def parse_html(load_path=HTML_CONTAINING_LIST_OF_POSTINGS):
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

    load_path = Path(load_path)
    if not load_path.exists():
        raise Exception(
            f"Html containing list of postings does not exist at path: {load_path}. If the file exists, fix the path. If it doesn't run get_html()."
        )
    with open(load_path, "r") as fp:
        html = fp.read()

    soup = BeautifulSoup(html, "lxml")
    jobs_list = soup.find("ul", attrs={"class": "jobs-search__results-list"})

    jobs_list_items = jobs_list.find_all("li")
    parsed_list_items = [parse_sidebar_item(x) for x in jobs_list_items]
    return parsed_list_items


# TODO: automate getting html
# get_html(save_path=HTML_CONTAINING_LIST_OF_POSTINGS)
links_and_metadata = parse_html(load_path=HTML_CONTAINING_LIST_OF_POSTINGS)

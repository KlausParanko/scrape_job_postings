# %%
from pathlib import Path
import random
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import ElementNotInteractableException

POSTINGS_LIST_FOLDER = Path("./postings_list")
POSTINGS_LIST_FOLDER.mkdir(exist_ok=True)


# %%


class RandomSleep:
    def short():
        time.sleep(abs(random.normalvariate(1, 0.5)))

    def long():
        time.sleep(abs(random.normalvariate(7, 2)))


def parse_html(load_path):
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

    driver = webdriver.Chrome()
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


def write(parsed_list_of_postings, postings_list_folder):
    def get_new_file_number(already_gathered_postings_lists):
        file_numbers = [
            int(filepath.stem[-1]) for filepath in already_gathered_postings_lists
        ]
        return max(file_numbers) + 1

    already_gathered_postings_lists = list(postings_list_folder.glob("*"))

    if len(already_gathered_postings_lists) == 0:
        # create first one
        filepath = postings_list_folder.joinpath("postings_list1.pkl")
        with open(filepath, "w") as fp:
            pickle.dump(parsed_list_of_postings, fp)
    else:
        new_file_number = get_new_file_number(already_gathered_postings_lists)
        filepath = postings_list_folder.joinpath(f"postings_list{new_file_number}.pkl")

        with open(filepath, "w") as fp:
            pickle.load(parsed_list_of_postings, fp)

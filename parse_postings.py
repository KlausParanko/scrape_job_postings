# %%
from pathlib import Path
from bs4 import BeautifulSoup
import pickle

import pandas as pd

# input
from get_postings import read_in_postings
from azure_io import upload_file_to_blob
from azure_credentials import blob_client
from data_paths import PARSED_POSTINGS_PATH

# %%


def _parse_posting_html(posting_html):
    def get_lists(posting_content) -> list[list]:
        """Postings often have bulleted lists with main points, like what the requirements are, what you will do in the job, and what the company has to offer."""
        lists = posting_content.find_all("ul")
        parsed_lists = []
        for l in lists:
            parsed = l.get_text("\n").split("\n")
            parsed_lists.append(parsed)
        return parsed_lists

    soup = BeautifulSoup(posting_html, "lxml")
    posting_text_css_selector = r"#main-content > section.core-rail.mx-auto.papabear\:w-core-rail-width.mamabear\:max-w-\[790px\].babybear\:max-w-\[790px\] > div > div > section.core-section-container.my-3.description > div > div > section > div"
    posting_content = soup.select(posting_text_css_selector)
    # print(len(posting_content))
    posting_content = posting_content[0]

    parsed = {
        "raw_text": posting_content.get_text("\n"),
        "parsed_lists": get_lists(posting_content),
    }
    return parsed


def _wrangle(postings):
    def separate_and_wrangle_bullet_lists(postings):
        """Bullet list is a list of lists. Multiple bullet lists, each bullet list separated into lines.
        ->explode multiple bullet lists join lines with newline.
        """
        bullet_lists = postings[["link", "parsed_lists"]]
        postings = postings.drop(columns="parsed_lists")

        bullet_lists = bullet_lists.explode("parsed_lists")
        bullet_lists["parsed_lists"] = bullet_lists["parsed_lists"].str.join("\n")

        return postings, bullet_lists

    postings = pd.DataFrame(postings)
    postings["listing_date"] = postings["listing_date"].apply(pd.to_datetime)
    postings, bullet_lists = separate_and_wrangle_bullet_lists(postings)
    return postings, bullet_lists


def _look_at_duplicates():
    """
    What should I use as index?
    - subset = ["job_title", "company_name", "location", "listing_date"]
        - Looked at RECORDLY ['DATA ENGINEER', 'Recordly', 'Helsinki, Uusimaa, Finland', '2023-08-31'].The postings were identical by content but their applicants number was different, so maybe they're looking for two data engineers?
    - only "link"
        - no dupes
        - use as idx?
    """

    def check_dupes_for_subset(postings, subset, show_dupes):
        print("Columns:", subset)
        dupes_mask = postings.duplicated(subset, keep=False)
        any_dupes = dupes_mask.any()
        print("Any dupes for subset?:", any_dupes)
        if show_dupes:
            display(
                "Dupes:",
                postings[dupes_mask].sort_values(["job_title", "company_name"]),
            )
        return postings[dupes_mask]

    postings = pd.read_csv(PARSED_POSTINGS_PATH["WHOLE_POSTING"])
    # ["job_title", "company_name", "location", "listing_date"] subset isn't unique
    subset = ["job_title", "company_name", "location", "listing_date"]
    subset_with_link = subset + ["link"]
    show_dupes = False
    dupes = check_dupes_for_subset(postings, subset, show_dupes)
    check_dupes_for_subset(postings, subset_with_link, show_dupes)

    # look at example duplicates link
    # recordly_mask = (dupes.loc[:, subset] == ['DATA ENGINEER', 'Recordly', 'Helsinki, Uusimaa, Finland', '2023-08-31']).all(axis=1)
    # dupes[recordly_mask].link.to_frame().style.format()

    # "link" by itself doesn't have duplicates

    print(
        "Any duplicates for only 'link'?:",
        postings.duplicated("link", keep=False).any(),
    )


# %%
if __name__ == "__main__":
    # read in and parse
    postings = read_in_postings()
    for p in postings:
        p.update(_parse_posting_html(p["posting_html"]))
        p.pop("posting_html")

    postings, bullet_lists = _wrangle(postings)

    postings.to_parquet(PARSED_POSTINGS_PATH["WHOLE_POSTING"], index=False)
    bullet_lists.to_parquet(PARSED_POSTINGS_PATH["BULLET_LISTS"], index=False)

    upload_file_to_blob(
        blob_client,
    )

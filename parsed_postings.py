# %%
from pathlib import Path
from bs4 import BeautifulSoup
import pickle

# input
from get_postings import POSTINGS_FOLDER

# outpt
PARSED_POSTINGS_PATH = Path("parsed_postings.pkl")


# %%
def _read_in_postings():
    filepaths = POSTINGS_FOLDER.glob("*")
    postings = []
    for path in filepaths:
        with open(path, "rb") as fp:
            d = pickle.load(fp)
            postings.append(d)
    return postings


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


def read():
    with open(PARSED_POSTINGS_PATH, "rb") as fp:
        return pickle.load(fp)


def look_at_duplicates():
    """
    What should I use as index?
    - subset = ["job_title", "company_name", "location", "listing_date"]
        - Looked at RECORDLY ['DATA ENGINEER', 'Recordly', 'Helsinki, Uusimaa, Finland', '2023-08-31'].The postings were identical by content but their applicants number was different, so maybe they're looking for two data engineers?
    - only "link"
        - no dupes
        - use as idx?
    """

    def check_dupes_for_subset(postings, subset, show_dupes):
        print("Index:", subset)
        dupes_mask = postings.duplicated(subset, keep=False)
        any_dupes = dupes_mask.any()
        print("Any dupes for index?:", any_dupes)
        if show_dupes:
            display(
                "Dupes:",
                postings[dupes_mask].sort_values(["job_title", "company_name"]),
            )
        return postings[dupes_mask]

    postings = read()
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


if __name__ == "__main__":
    postings = _read_in_postings()
    for p in postings:
        p.update(_parse_posting_html(p["posting_html"]))
        p.pop("posting_html")

    with open(PARSED_POSTINGS_PATH, "wb") as fp:
        pickle.dump(postings, fp)

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


if __name__ == "__main__":
    postings = _read_in_postings()
    for p in postings:
        p.update(_parse_posting_html(p["posting_html"]))
        p.pop("posting_html")

    with open(PARSED_POSTINGS_PATH, "wb") as fp:
        pickle.dump(postings, fp)

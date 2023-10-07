# %%
import pandas as pd

import parsed_postings


# %%
# Maybe filter postings on name, because many are not data engineering jobs
# filters: data & engineer, data
def get_titles():
    postings = parsed_postings.read()
    titles = []
    for p in postings:
        titles.append(p["job_title"])
    return titles


def categorize(titles):
    # titles can have multiple of search words so maybe add flags per title rather than gathering titles

    categories = [
        "data",
        "data engineer",
        "data architect",
        "data scientist",
        "data analyst",
        "ml",
        "machine learning",
        "ai",
        "artificial intelligence",
    ]

    categorized_titles = []
    for t in titles:
        t = t.lower()

        title_categories = {"title": t}
        for search_word in categories:
            if search_word in t:
                title_categories[search_word] = True
            else:
                title_categories[search_word] = False

        categorized_titles.append(title_categories)

    return pd.DataFrame(categorized_titles)


titles = get_titles()
categorized_titles = categorize(titles)

# 'data' is part of 'data engineer' etc, find out which titles have only 'data' in them
only_data = categorized_titles["data"] & (categorized_titles.sum(axis=1) == 1)
categorized_titles["only data"] = only_data

# sums
categorized_titles.sum()

categorized_titles[categorized_titles["only data"]]["title"]

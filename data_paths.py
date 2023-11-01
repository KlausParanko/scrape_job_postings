from pathlib import Path


DATA_FOLDER = Path("./data/")
DATA_FOLDER.mkdir(exist_ok=True)

POSTINGS_LIST_PATHS = {
    "RAW_FOLDER": DATA_FOLDER.joinpath("postings_list", "raw"),
    # contains deduplicated and merged results
    "MERGED": DATA_FOLDER.joinpath("postings_list", "merged.csv"),
    # MERGED but on azure blob storage
    "AZURE_FILENAME": "postings_list.csv",
}
POSTINGS_LIST_PATHS["RAW_FOLDER"].mkdir(exist_ok=True)

POSTINGS_CONTENT_PATHS = {
    "RAW_FOLDER": DATA_FOLDER.joinpath("postings", "raw"),
    "MERGED": DATA_FOLDER.joinpath("postings", "merged.parquet"),
    "AZURE_FILENAME": "postings.parquet",
}

PARSED_POSTINGS_PATH = {
    "WHOLE_POSTING": DATA_FOLDER.joinpath("parsed_postings", "parsed_postings.parquet"),
    "BULLET_LISTS": DATA_FOLDER.joinpath(
        "parsed_postings", "posting_bullet_lists.parquet"
    ),
    "AZURE_FILENAME": "parsed_postings.parquet",
}

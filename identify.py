# identify

# standard libraries
import json, logging, os, re
from datetime import datetime
from typing import Dict, Sequence

# third-party libraries
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

# local libraries
from compare import create_compare_func, look_for_ebook, normalize, normalize_univ
from create_db_cache import ENGINE, DB_CACHE_PATH_ELEMS, set_up_database


# Initialize settings and global variables

logger = logging.getLogger(__name__)
logging.basicConfig()

try:
    with open(os.path.join('config', 'env.json')) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')

logger.setLevel(ENV.get('LOG_LEVEL', 'DEBUG'))

# Set up database if necessary
if not os.path.isfile(os.path.join(*DB_CACHE_PATH_ELEMS)):
    set_up_database()

BOOKS_CSV_PATH_ELEMS = ENV['BOOKS_CSV_PATH']

worldcat_config = ENV['WORLDCAT']
WC_API_KEY = worldcat_config['WC_SEARCH_API_KEY']
WC_BIB_BASE_URL = worldcat_config['BIB_RESOURCE_BASE_URL']

with open(os.path.join('config', 'marcxml_lookup.json')) as lookup_file:
    MARCXML_LOOKUP = json.loads(lookup_file.read())


# Functions - Utilities

def create_full_title(record: Dict[str, str]):
    full_title = record['Title']
    if record["Subtitle"] not in ["N/A", ""]:
        full_title += ' ' + record['Subtitle']
    logger.debug('full_title: ' + full_title)
    return full_title


# Functions - Caching

# Create unique request string for WorldCat Search API caching
def create_unique_request_str(base_url: str, params_dict: dict, private_keys: list =["wskey"]) -> str:
    sorted_params = sorted(params_dict.keys())
    fields = []
    for param in sorted_params:
        if param not in private_keys:
            fields.append('{}-{}'.format(param, params_dict[param]))
    return base_url + '&'.join(fields)


# Make the request and cache new data, or retrieves the cached data
def make_request_using_cache(url: str, params: Dict[str, str]) -> str:
    unique_req_url = create_unique_request_str(url, params)
    cache_df = pd.read_sql(f'''
        SELECT * FROM request WHERE request_url = '{unique_req_url}';
    ''', ENGINE)

    if len(cache_df):
        logger.debug('Retrieving cached data...')
        return cache_df.iloc[0]['response']

    logger.debug('Making a request for new data...')
    response_obj = requests.get(url, params)
    logger.debug(response_obj.url)
    status_code = response_obj.status_code
    if status_code == 403:
        logger.warning('Reached API limit')
        return ''
    elif status_code != 200:
        logger.debug(response_obj.text)
        logger.warning(f'Received irregular status code: {status_code}')
        return ''

    response_text = response_obj.text
    new_request_df = pd.DataFrame({
        'request_url': [unique_req_url],
        'response': [response_text]
    })
    logger.debug(new_request_df)
    new_request_df.to_sql('request', ENGINE, if_exists='append', index=False)
    return response_text


# Functions - Processing

# Use the Bibliographic Resource tool to search for records and parse the returned MARC XML
def look_up_book_in_worldcat(book_dict: Dict[str, str]) -> pd.DataFrame:
    # Generate query string
    full_title = create_full_title(book_dict)
    logger.info(f'Looking for "{full_title}" in WorldCat...')
    query_author = normalize(f'{book_dict["Author_First"]} {book_dict["Author_Last"]}')
    logger.debug('full_title: ' + full_title)
    query_title = normalize(full_title)
    logger.debug('query_title: ' + query_title)
    query_str = f'srw.ti all "{query_title}" and srw.au all "{query_author}"'

    params = {
        'wskey': WC_API_KEY,
        "query": query_str,
        "maximumRecords": 100,
        'frbrGrouping': 'off'
    }

    result = make_request_using_cache(WC_BIB_BASE_URL, params)
    if not result:
        return pd.DataFrame()
    
    result_xml = BeautifulSoup(result, 'xml')
    number_of_records = result_xml.find("numberOfRecords").text
    logger.debug(number_of_records)

    records = result_xml.find_all("recordData")
    record_dict_list = []
    for record in records:
        record_dict = {}
        for key in MARCXML_LOOKUP:
            marc_field = MARCXML_LOOKUP[key]
            statement = record.find('datafield', tag=marc_field['datafield'])
            if not statement:
                value = pd.NA
            else:
                sub_statement = statement.find("subfield", code=marc_field['subfield'])
                # Turn NA into regex?
                if not sub_statement or 'NA' in sub_statement:
                    value = pd.NA
                else:
                    value = sub_statement.text
            record_dict[key] = value
        record_dict_list.append(record_dict)
    records_df = pd.DataFrame(record_dict_list)
    logger.info(f'Number of WorldCat records found: {len(records_df)}')
    logger.debug(records_df.head(10))
    return records_df


def run_checks_and_return_matches(orig_record: Dict[str, str], results_df: pd.DataFrame) -> pd.DataFrame:
    checked_df = results_df.copy()
    logger.debug(orig_record)
    logger.debug(checked_df)

    # Create comparison functions
    full_title = create_full_title(orig_record)
    compare_to_title = create_compare_func(full_title, 85)
    imprint_transforms = [normalize_univ]
    compare_to_imprint = create_compare_func(orig_record['Imprint'], 85, imprint_transforms)

    # Create full title column
    checked_df['Full_Title'] = checked_df['Title'] + checked_df['Subtitle']
    logger.debug(checked_df['Full_Title'])

    # Run comparisons
    checked_df['Title_Matched'] = checked_df['Full_Title'].map(compare_to_title, na_action='ignore')
    checked_df['Imprint_Matched'] = checked_df['Imprint'].map(compare_to_imprint, na_action='ignore')
    checked_df['Ebook_Present'] = checked_df['Physical_Description'].map(look_for_ebook, na_action='ignore')
    logger.info(checked_df[['Title', 'Imprint', 'Title_Matched', 'Imprint_Matched', 'Ebook_Present']])

    # Gather ISBNs
    manifest_df = checked_df.loc[(
        (checked_df['Title_Matched']) & (checked_df['Imprint_Matched'])
    )]
    logger.info(f'Matched {len(manifest_df)} records!')
    logger.info(manifest_df.head(20))

    # Add HEB_ID and Full_Title from HEB
    manifest_df = manifest_df.assign(**{
        'HEB_Title': create_full_title(orig_record)
    })
    return manifest_df


def identify_ebooks() -> None:
    # Load input data
    press_books_df = pd.read_csv(os.path.join(*BOOKS_CSV_PATH_ELEMS))
    logger.info(press_books_df)

    # For each record, fetch WorldCat data, compare to record, and document results
    match_manifest_df = pd.DataFrame({})
    non_matching_books = []
    num_books_with_matches = 0

    for press_book_row_tup in press_books_df.iterrows():
        new_book_dict = press_book_row_tup[1].copy().to_dict()
        logger.info(new_book_dict)

        wc_records_df = look_up_book_in_worldcat(new_book_dict)
        new_matches_df = run_checks_and_return_matches(new_book_dict, wc_records_df)

        if new_matches_df.empty:
            logger.warning(f'No matching records with isbns were found!')
            non_matching_books.append(new_book_dict)
        else:
            num_books_with_matches += 1
            isbns = new_matches_df['ISBN'].drop_duplicates().dropna().to_list()
            logger.info(f'Book successfully matched with {len(isbns)} record(s) with ISBN(s): {isbns}')
            logger.info(new_matches_df)
            match_manifest_df = match_manifest_df.append(new_matches_df)

    # Generate CSV output
    if not match_manifest_df.empty:
        logger.debug(match_manifest_df)
        match_manifest_df.to_csv(os.path.join('data', 'matched_manifests.csv'), index=False)

    if non_matching_books:
        no_isbn_matches_df = pd.DataFrame(non_matching_books)
        no_isbn_matches_df.to_csv(os.path.join('data', 'no_isbn_matches.csv'), index=False)

    # Log Summary Report
    report_str = '** Sumary Report from identify.py **\n\n'
    report_str += f'-- Total number of records: {len(press_books_df)}\n'
    report_str += f'-- Number of books successfully matched with ISBNs: {num_books_with_matches}\n'
    report_str += f'-- Number of books with no matches with ISBNs: {len(non_matching_books)}\n'
    logger.info(f'\n\n{report_str}')
    return None


# Main Program

if __name__ == '__main__':
    identify_ebooks()

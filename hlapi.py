# identify

# standard libraries
import json, logging, os
from datetime import datetime
from typing import Dict, Sequence

# third-party libraries
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from tqdm import tqdm
import isbnlib as ib
from safeprint import print
from diskcache import Cache

# local libraries
from compare import classify_by_format, \
                    create_compare_func, \
                    extract_extra_atoms, \
                    normalize, \
                    polish_isbn, \
                    normalize_univ, \
                    NA_PATTERN
from db_cache import make_request_using_cache # , set_up_database


# Initialize settings and global variables
BEGIN = datetime.now()
TS = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# logger = logging.getLogger(__name__)

try:
    with open(os.path.join('config', 'env.json')) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    print('Configuration file could not be found')
    # logger.error('Configuration file could not be found; please add env.json to the config directory.')

# logging.basicConfig(level=ENV.get('LOG_LEVEL', 'NOTSET'))

# # Set up database if necessary
# if not os.path.isfile(os.path.join(*ENV['DB_CACHE_PATH'])):
#     set_up_database()

BOOKS_CSV_PATH_ELEMS = ENV['BOOKS_CSV_PATH']
ALREADY_CSV_PATH_ELEMS = ENV['ALREADY_CSV_PATH']

worldcat_config = ENV['RESOURCE']
API_KEY = worldcat_config['BIB_RESOURCE_KEY']
BIB_BASE_URL = worldcat_config['BIB_RESOURCE_BASE_URL']
TEST_MODE_OPTS = ENV['TEST_MODE']

with open(os.path.join('config', 'modsxml_lookup.json')) as lookup_file:
    MODSXML_LOOKUP = json.loads(lookup_file.read())
with open(os.path.join('config', 'input_to_identify.json')) as input_to_identify_cw:
    INPUT_TO_IDENTIFY_CW = json.loads(input_to_identify_cw.read())
with open(os.path.join('config', 'identify_to_output.json')) as identify_to_output_cw:
    IDENTIFY_TO_OUTPUT_CW = json.loads(identify_to_output_cw.read())




def identify_books() -> None:
    # Load input data
    input_path = os.path.join(*BOOKS_CSV_PATH_ELEMS)
    if '.xlsx' in BOOKS_CSV_PATH_ELEMS[-1]:
        press_books_df = pd.read_excel(input_path, dtype=str)
        press_books_df = press_books_df.iloc[1:]  # Remove dummy record
    else:
        press_books_df = pd.read_csv(input_path, dtype=str)

    matches_df = pd.DataFrame({})

    if ALREADY_CSV_PATH_ELEMS[-1] != "":
        already_input_path = os.path.join(*ALREADY_CSV_PATH_ELEMS)
        if '.xlsx' in ALREADY_CSV_PATH_ELEMS[-1]:
            already_books_df = pd.read_excel(already_input_path, dtype=str)
        else:
            already_books_df = pd.read_csv(already_input_path,dtype=str)

        press_books_df.drop(already_books_df.index.to_list())
        matches_df.append(already_books_df)


    # Crosswalk to consistent column names
    # press_books_df = press_books_df.rename(columns=INPUT_TO_IDENTIFY_CW)
    # logger.debug(press_books_df.columns)

    # Limit number of records for testing purposes
    if TEST_MODE_OPTS['ON']:
        # logger.info('TEST_MODE is ON.')
        press_books_df = press_books_df.iloc[:TEST_MODE_OPTS['NUM_RECORDS']]

    # For each record, fetch WorldCat data, compare to record, analyze and accumulate matches
    non_matching_books = {}
    num_books_with_matches = 0

    iter = tqdm(press_books_df.iterrows())
    for press_book_row_tup in iter:
        iter.set_description("Looking up books")
        new_book_dict = press_book_row_tup[1].to_dict()
        # logger.info(new_book_dict)

        matching_records_df = look_up_book_in_resource(new_book_dict)

        matches_df = matches_df.append(pd.Series(
            new_book_dict,
            name=new_book_dict['ID']
        ))

        if not matching_records_df.empty:
            matches_df = matches_df.append(matching_records_df)

    # logger.debug('Matching Manifests')
    # logger.debug(matches_df.describe())

    matches_df = matches_df[ENV["OUTPUT_COLUMNS"]]

    # Generate Excel output
    if not matches_df.empty:
        try:
            save_excel(matches_df,'output')
        except:
            save_csv(matches_df,'output')
        # matches_df.to_csv(os.path.join('data', 'matched_manifests.csv'), index=False)

    # if non_matching_books:
    #     no_isbn_matches_df = pd.DataFrame.from_dict(non_matching_books,orient='index')
    #     no_isbn_matches_df = no_isbn_matches_df[ENV["OUTPUT_COLUMNS"]]
    #     try:
    #         save_excel(no_isbn_matches_df,'not_matched')
    #     except:
    #         save_csv(no_isbn_matches_df,'not_matched')
        # no_isbn_matches_df.to_csv(os.path.join('data', 'no_isbn_matches.csv'), index=False)

    # Log Summary Report
    report_str = '** Summary Report from identify.py **\n\n'
    report_str += f'-- Total number of books included in search: {len(press_books_df)}\n'
    report_str += f'-- Number of books successfully matched with records with ISBNs: {num_books_with_matches}\n'
    report_str += f'-- Number of books with no matching records: {len(non_matching_books)}\n'
    # logger.info(f'\n\n{report_str}')
    return None


def get_canon_isbn(isbnlike):
    isbn = classify_isbn(isbnlike)

    # if isbn['type'] != 'isbn13':
    #     isbn['canon'] = ib.to_isbn13(isbn['canon'])
    #     isbn['type'] = 'isbn13'

    return isbn['canon']

def classify_isbn(isbnlike):
    isbn = {}
    isbn['canon'] = ib.canonical(isbnlike)
    if ib.is_isbn10(isbnlike):
        isbn['type'] = 'isbn10'
    elif ib.is_isbn10('0'+isbn['canon']):
        isbn['canon'] = '0'+isbn['canon']
        isbn['masked'] = ib.mask(isbn['canon'])
        isbn['type'] = 'isbn10'
    elif ib.is_isbn10('00'+isbn['canon']):
        isbn['canon'] = '00'+isbn['canon']
        isbn['masked'] = ib.mask(isbn['canon'])
        isbn['type'] = 'isbn10'
    elif ib.is_isbn13(isbn['canon']):
        isbn['masked'] = ib.mask(isbn['canon'])
        isbn['type'] = 'isbn13'
    else:
        isbn['type'] = 'invalid?'
    return isbn

def identify_format(form_string):
    formats = {
        "paper" : ['paper','pbk'],
        "hardcover" : ['hard','cloth'],
        "ebook" : ['ebook','e-book','electronic']
    }

    returnable = ''
    for fmat in list(formats.keys()):
        for desc in formats[fmat]:
            if desc in form_string:
                return fmat
    return 'unknown'

# Use the Bibliographic Resource tool to search for records and parse the returned MARC XML
def look_up_book_in_resource(book_dict: Dict[str, str]) -> pd.DataFrame:
    # Generate query string
    # logger.info(f'Looking for {book_dict["Main Title"]} in Harvard LibraryCloud...')

    query_author = normalize(f"{book_dict['Author 1 Given']} {book_dict['Author 1 Initial']} {book_dict['Author 1 Family']}")
    # query_author = book_dict['authorLast']
    query_author.replace("'", " ")

    title_bool_and = create_title_bool_and(book_dict)
    params = {
        'title' : title_bool_and,
        'name' : query_author,
        'limit': 10,
        'publisher' : book_dict['Publisher']
    }

    # !!!
    # TODO: Add second query for copyright holder

    query_str = f'&'.join([k+'='+str(params[k]) for k in list(params.keys())])
    # logger.debug(query_str)
    records = {}

    result = make_request_using_cache(BIB_BASE_URL, params)
    if result:
        records = parse_modsxml(result,book_dict)
        if len(list(records.keys())) == 0:
            params.pop('publisher')
            result = make_request_using_cache(BIB_BASE_URL, params)
            if result:
                records = parse_modsxml(result,book_dict)

    if book_dict['Publisher'] != book_dict['Copyright Holder']:
        params['publisher'] = book_dict['Copyright Holder']
        second_result = make_request_using_cache(BIB_BASE_URL, params)
        if second_result:
            second_records = parse_modsxml(second_result,book_dict)
            if len(list(second_records.keys())) > 0:
                records.update(second_records)

    # if TEST_MODE_OPTS['ON'] == True:
    #     soup = BeautifulSoup(result,'lxml')
    #     with open(f'{query_author}.xml','w') as out_file:
    #         out_file.write(soup.prettify())

    if records == {}:
        return pd.DataFrame({})
    else:
        records_df = pd.DataFrame.from_dict(records,orient='index')
    # logger.info(f'Number of records found: {len(records_df)}')
    # logger.debug(records_df.head(10))
        return records_df


def create_title_bool_and(record: Dict[str, str]) -> str:

    if 'Subtitle' in record.keys() and record["Subtitle"] not in ["N/A", ""]:
        query_str = "(" + ")+AND+(".join([
            normalize(str(record['Main Title'])),
            normalize(str(record['Subtitle']))
        ]) + ")"
    else:
        query_str = normalize(record['Main Title'])

    # logger.debug('Title Boolean phrase or string: ' + query_str)
    return query_str




def parse_modsxml(xml_record,book_dict):
    result_xml = BeautifulSoup(xml_record, 'xml')
    number_of_records = result_xml.find("numFound").text
    # if int(number_of_records) > 100:
        # logger.error(f'Number of records > 100: {number_of_records}')

    items = result_xml.find("items")
    records = items.children
    record_dicts = {}
    for r in records:

        rd = {}
        rd['ID'] = r.find('mods:recordIdentifier').text
        rd['Source'] = 'Harvard Library'

        # print(r)
        titleInfo = r.find("mods:titleInfo")
        try:
            rd['Main Title'] = titleInfo.find("mods:nonSort").text.strip() +" "+ titleInfo.find("mods:title").text
        except:
            rd['Main Title'] = titleInfo.find("mods:title").text

        try:
            rd['Subtitle'] = titleInfo.find("mods:subTitle").text
        except:
            rd['Subtitle'] = ''

        names = r.find_all("mods:name")
        for n in [1,2]:

            try:
                name = names[n-1].find("mods:namePart").text.split(', ')
                rd[f'Author {n} Given'] = name[1].split()[0]
                rd[f'Author {n} Initial'] = name[1].split()[1]
                rd[f'Author {n} Family'] = name[0]

            except:
                rd[f'Author {n} Given'] = ''
                rd[f'Author {n} Initial'] = ''
                rd[f'Author {n} Family'] = ''

        try:
            rd['Author 3 Name'] = ' '.join([str for str in names[2].stripped_strings])
        except:
            rd['Author 3 Name'] = ''

        rd['Publisher'] = ''
        try:
            pubs = r.find_all("mods:publisher")
            for pub in pubs:
                if pub.text not in rd['Publisher']:
                    if rd['Publisher'] == '':
                        rd['Publisher'] = pub.text
                    else:
                        rd['Publisher'] += ' ; ' + pub.text
        except:
            rd['Publisher'] = ''

        try:
            cities = []
            placeTerms = r.find_all("mods:placeTerm")
            for term in placeTerms:
                if (term['type'] == 'text') and ("authority" not in term.attrs):
                    cities.append(term.text)
            rd['Pub City'] = ' ; '.join(cities)
        except:
            rd['Pub City'] = ''

        oclc = ''
        lccn = ''
        isbn = ''

        isbns = []
        idents = r.find_all("mods:identifier")
        for ident in idents:

            if 'type' in ident.attrs:
                if ident['type'] == 'isbn':
                    if '(' in ident.text:
                        form_string = ident.text.split('(')[-1].split(')')[0]
                    else:
                        form_string = ident.text

                    isbn = get_canon_isbn(ident.text)
                    fmat = identify_format(form_string)

                    if (isbn,fmat) not in isbns:
                        isbns.append((isbn,fmat))
                    # logger.info('MODS ISBNs',isbns)
                elif ident['type'] == 'oclc':
                    oclc = ib.canonical(ident.text)
                elif ident['type'] == 'lccn':
                    lccn = ib.canonical(ident.text)

        if 'Uncategorized ISBNs' not in rd:
            rd['Uncategorized ISBNs'] = ''

        for isbn, form in isbns:
            # print(rd['Main Title'],form,isbn)
            if form == 'ebook':
                rd['ebook ISBN'] = isbn
            elif form == 'hardcover':
                rd['hardcover ISBN'] = isbn
            elif form == 'paper':
                rd['paper ISBN'] = isbn
            elif form == 'unknown':
                if rd['Uncategorized ISBNs'] == '':
                    rd['Uncategorized ISBNs'] = isbn
                else:
                    rd['Uncategorized ISBNs'] += " ; "+str(isbn)


        try:
            relateds = r.find_all("mods:relatedItem")
            for related in relateds:
                if related['otherType'] == 'HOLLIS record':
                    rd['Online Link'] = related.find("mods:url").text
        except:
            if oclc != '':
                rd['Online Link'] = 'https://worldcat.org/oclc/'+oclc
            elif isbn != '':
                rd['Online Link'] = 'https://api.lib.harvard.edu/v2/items?q='+rd['ID']



        # logger.debug(rd)

        record_key = book_dict['ID'] + "_" + rd['ID']
        with Cache(f'hl_id_cache/{TS}') as ref:
            if record_key not in ref:
                record_dicts[record_key] = rd
                ref[record_key] = 1

    return record_dicts

def save_excel(df,stem):
    dir = get_out_dir()
    df.to_excel(dir+f'{TS}-{stem}.xlsx')

def save_csv(df,stem):
    dir = get_out_dir()
    df.to_csv(dir+f'{TS}-{stem}.csv')

def get_out_dir():

    dir_name = "outputs/"

    if not os.path.exists("outputs"):
        os.mkdir("outputs")

    if not os.path.exists(dir_name):
        os.mkdir(dir_name)

    return dir_name


# Main Program

if __name__ == '__main__':
    identify_books()
    end = datetime.now()
    print("Time elapsed:",end-BEGIN)


    # For when API is out
    # input_path = os.path.join(*BOOKS_CSV_PATH_ELEMS)
    # press_books_df = pd.read_csv(input_path, dtype=str)
    #
    # i = 0
    # for tup in press_books_df.iterrows():
    #     isbn = tup[1]['Uncategorized ISBN 1']
    #     title = tup[1]['Main Title']
    #     if (not pd.isnull(isbn)) and (i < 1000):
    #         process_isbn(title,isbn)
    #     i +=1

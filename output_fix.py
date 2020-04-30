import pandas as pd
from hlapi import look_up_book_in_resource
from tqdm import tqdm

def prepend_id_on_gb_record(df):
    for id in df.index.values:
        sor = df.at[id,'Sort']
        if '_' not in sor:
            hebid = sor
        if 'GB_API' in sor:
            df.at[id,'Sort'] = hebid+'_'+sor

# df = pd.read_excel("outputs/1-2020-04-15-full-output.xlsx")
# prepend_id_on_gb_record(df)
# df.to_excel("outputs/fixed-full-output.xlsx",index=False)

def remove_false_paper_positives(df):
    isbn_cols = ["ebook ISBN","paper ISBN","hardcover ISBN","Uncategorized ISBN"]
    id_list = df.index.values
    tqdm_iter = tqdm(id_list)
    tqdm_iter.set_description("Fixing paperback ISBNs")
    for sort_id in tqdm_iter:
        if "HEB" not in df.at[sort_id,"ID"]:
            if "GB_API" not in df.at[sort_id,"ID"]:
                hebid = sort_id.split("_")[0]
                michpub_record_dict = df.loc[hebid].to_dict()
                new_records = look_up_book_in_resource(michpub_record_dict)
                if sort_id in new_records.index.values:
                    for col_name in isbn_cols:
                        df.at[sort_id,col_name] = None
                        if col_name in new_records.columns.values:
                            df.at[sort_id,col_name] = new_records.at[sort_id,col_name]

df = pd.read_excel("outputs/2020-04-16-fixed-full-output.xlsx",index_col="Sort")
remove_false_paper_positives(df)
df.to_excel("outputs/fixed-full-output.xlsx")

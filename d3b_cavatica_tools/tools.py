import os

import pandas as pd
import psycopg2
from tqdm import tqdm


def read_cavatica_cli_filelist(filepath_or_buffer):
    """"""
    out = pd.read_csv(
        filepath_or_buffer,
        sep="\t",
        names=["cavatica_id", "file_name", "cavatica_location"],
    )
    return out


def genomic_file_fuzzy_finder(file_name):
    """"""
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()
    cur.execute(
        """
            SELECT kf_id, external_id
            FROM genomic_file
            WHERE external_id LIKE '%{0}'
        """.format(
            file_name
        )
    )
    resp = cur.fetchall()
    out = resp[0] + (file_name,)
    cur.close()
    conn.close()
    return [out]


def find_genomic_files_in_ds(filelist):
    """"""
    gf_list = []
    for file_name in tqdm(filelist["file_name"]):
        file_name = file_name.replace("'", "")
        gf_list.extend(genomic_file_fuzzy_finder(file_name))
    return gf_list

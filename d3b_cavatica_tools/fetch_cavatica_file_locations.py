import pandas as pd
import tools as cvt

# get the file list path


def fetch_locations(filepath_or_buffer, out_location):
    filelist_path = filepath_or_buffer
    filelist = cvt.read_cavatica_cli_filelist(filelist_path)
    gfs = cvt.find_genomic_files_in_ds(filelist)

    gfs_df = pd.DataFrame(gfs, columns=("kf_id", "external_id", "file_name"))

    filelist.file_name = filelist.file_name.str.replace("'", "")
    gf_info = filelist.set_index("file_name").join(
        gfs_df.set_index("file_name"), how="outer"
    )

    out = gf_info.reset_index(drop=True)
    # Save the output file

    out.to_csv(out_location)

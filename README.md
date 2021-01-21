# Collection of reusable python utilities

## Requires

Python >= 3.6

## How to install

Using pip

`pip install -e git+ssh://git@github.com/d3b-center/d3b-cavatica-python-tools.git#egg=d3b-cavatica-tools`

## Included so far

### Import Files From a volume

This script imports files from a cavatica volume to a cavatica project (and creates the project if nescessary)

To get full details of all the flags and arguments, run 

```bash
python import_files_from_volume.py -h
```


#### Usage

```bash
python import_files_from_volume.py SD_12345678 "Project Name" --file_list filelist.csv --db_url $DATABASE_URL --sbg_profile profile_name
```

I'll describe the different arguments before describing what they do:

- `SD_12345678` is the name of the cavatica volume where the files are currently stored. For most of d3b's purposes, these volumes are named after the study ID. These Volumes contain pointers to the relevant s3 bucket.
- "Project Name" is the name of the project where the files will be deposited. If the project does not yet exist, the project will be created. Note that if the project does not yet exist, you can specify the project's billing group and description with the arguments `--billing_group` and `--project_description`. Otherwise, defaults to billing group `CBTN` and project description `Delivery project automatically created for you.`.
- `--file_list filelist.csv` points to a file where the first column (or only column) is a list of genomic file KF_IDs, e.g. `GF_12345678`.
- `--db_url $DATABASE_URL` points to the url of the postgres database to query for information about the genomic files and the related biospecimens and participants.
- `--sbg_profile profile_name` points to the profile with access credentials to the sevenbridges api.  Info about setting up the configuration can be found (here)[https://docs.sevenbridges.com/docs/store-credentials-to-access-seven-bridges-client-applications-and-libraries]

### How it works

The tool takes in a list of genomic file IDs, a source volume, and a destination project. 

1. Checkto see if it can find the volume specified by the user. If it can't find the volume, the script throws an error.
2. Check to see if the project exists. If it doesn't exist, the project is created. 
3. Query the specified database for the genomic files. 
4. In batches, make requests against the seven bridges API to import the files returned from the database query from the source volume to the target project. 

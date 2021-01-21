# Collection of reusable python utilities

## Requires

Python >= 3.6

## How to install

Using pip

`pip install -e git+ssh://git@github.com/d3b-center/d3b-utils-python.git#egg=d3b_utils`

## Included so far

### Import Files From a volume

This script imports files from a cavatica volume to a cavatica project (and creates the project if nescessary)

#### Usage:

```bash
python import_files_from_volume.py SD_12345678 "Project Name" --file_list filelist.csv
```

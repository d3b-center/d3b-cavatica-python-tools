import sevenbridges as sbg
import argparse
import os
import pandas as pd
import psycopg2
import time
import json
import datetime

# Arguments
parser = argparse.ArgumentParser(
    prog="Import Files to a Cavatica Project from S3",
    description="""Import files from a cavatica volume to a cavatica project.
                    This will create the project if it does not already exist.
                   """,
)

parser.add_argument(
    "volume",
    metavar="Cavatica Volume Name",
    type=str,
    help="""Name of the volume in cavatica where the files are.""",
)

parser.add_argument(
    "project",
    metavar="Cavatica project name",
    type=str,
    help="""Name of the project in cavatica where the files should be put.""",
)

parser.add_argument(
    "--db_connection_url",
    metavar="connection url to query",
    type=str,
    help="""URL to the database that will be queried to retrieve file 
            information. e.g.:
            postgresql://username:password@localhost:5432/postgres
            """,
)

parser.add_argument(
    "--file_list",
    metavar="CSV with list of files to be imported",
    type=str,
    help="""CSV file with one column, with file kf_ids""",
)

parser.add_argument(
    "--sbg_profile",
    metavar="SBG Config Profile",
    type=str,
    help="""Configuration profile to use with the Seven Bridges API.
            Info about setting up the configuration can be found at
            https://docs.sevenbridges.com/docs/store-credentials-to-access-seven-bridges-client-applications-and-libraries
            Default is 'kids-first-drc'
            """,
    default="kids-first-drc",
)
parser.add_argument(
    "--billing_group",
    metavar="SBG Billing Group for new projects",
    type=str,
    help="""Billing group to use if setting up a new project.
            Default is 'CBTN'
            """,
    default="CBTN",
)
parser.add_argument(
    "--project_description",
    metavar="New project description",
    type=str,
    help="""Describe the new project to be created
            """,
    default="Delivery project automatically created for you.",
)


args = parser.parse_args()
print(f"Args: {args.__dict__}")

# Check if the file list exists
# if not os.path.isfile(args.file_list):
#     print("The file list can't be found")
#     sys.exit()


config_file = sbg.Config(profile=args.sbg_profile)
api = sbg.Api(config=config_file)

# Volume Set up -----

volume_name = args.volume
# Check if the volume specified exists
all_volumes = api.volumes.query(limit=100)
my_volume = [v for v in all_volumes.all() if v.name == volume_name]

if not my_volume:
    print("Volume {} does not exist, check name / mounting".format(volume_name))
    raise KeyboardInterrupt
else:
    my_volume = my_volume[0]

print("Volume {} found.".format(my_volume.name))


# Project Setup -----

# [USER INPUT] Set project name here:
new_project_name = args.project

# What are my funding sources?
billing_groups = api.billing_groups.query()

my_billing_group = [
    bg for bg in billing_groups.all() if bg.name == args.billing_group
]
if not my_billing_group:
    print(
        "Billing Group {} does not exist, check name / mounting".format(
            args.billing_group
        )
    )
    raise KeyboardInterrupt
else:
    my_billing_group = my_billing_group[0]

# check if this project already exists. LIST all projects and check for name
#  match
my_project = [
    p for p in api.projects.query(limit=100).all() if p.name == new_project_name
]

if my_project:
    print(
        "A project with the name {} already exists, skipping creation".format(
            new_project_name
        )
    )
    my_project = my_project[0]
else:
    # CREATE the new project
    new_project = {
        "billing_group": my_billing_group.id,
        "name": new_project_name,
        "description": args.project_description,
    }

    my_project = api.projects.create(
        name=new_project["name"],
        billing_group=new_project["billing_group"],
        description=new_project["description"],
    )

    # (re)list all projects, and get your new project
    my_project = [
        p
        for p in api.projects.query(limit=100).all()
        if p.name == new_project_name
    ][0]

    print("Your new project {} has been created.".format(my_project.name))
    if hasattr(my_project, "description"):
        print("Project description: {} \n".format(my_project.description))

my_files = api.files.query(limit=100, project=my_project)
if my_files.total == 0:
    print("no files in the project")


# File Import -----
gf_ids = pd.read_csv(args.file_list, header=None)[0].unique()

# Conver gf_ids into file names

connection_url = args.db_connection_url


def fetch_data_view(query, vars=None):
    cur = conn.cursor()
    cur.execute(query, vars)
    resp = cur.fetchall()
    cur.close()
    return resp


conn = psycopg2.connect(connection_url)

file_table = fetch_data_view(
    """
        SELECT bs.participant_id,
            bsgf.biospecimen_id,
            p.family_id,
            ox.vital_status,
            p.ethnicity,
            p.race,
            p.gender,
            gf.kf_id,
            gf.external_id
        FROM participant p
        JOIN biospecimen bs ON p.kf_id = bs.participant_id
        JOIN biospecimen_genomic_file bsgf ON bs.kf_id = bsgf.biospecimen_id
        JOIN genomic_file gf ON gf.kf_id = bsgf.genomic_file_id
        LEFT JOIN outcome ox ON ox.participant_id = p.kf_id
        WHERE gf.kf_id in %s
    """,
    (tuple(gf_ids),),
)


def strip_path(path):
    _, _, fname = path.rpartition("/")
    return fname


def is_running(response):
    if not response.valid or response.resource.error:
        raise Exception(
            "\n".join(
                [
                    str(response.resource.error),
                    response.resource.error.message,
                    response.resource.error.more_info,
                ]
            )
        )
    return response.resource.state not in ["COMPLETED", "FAILED", "ABORTED"]


def bulk_import_files(file_df, volume, project, overwrite=True, chunk_size=100):
    "Imports list of files from volume in bulk"
    final_responses = []
    # import files in batches of 100 each
    for i in range(0, len(file_df), chunk_size):
        # check the rate limiting
        if i > 0:
            remaining = final_responses[-1]._api.remaining
            reset_time = final_responses[-1]._api.reset_time
            if remaining < (2 * ((i + chunk_size) - i)):
                time_delta = reset_time - datetime.datetime.now()
                if time_delta.total_seconds() > 0:
                    print(
                        "Rate limit will be reached soon.",
                        "Rate limit will be reset at",
                        reset_time,
                        ".\n",
                        "Waiting",
                        time_delta.total_seconds(),
                        "seconds.",
                    )
                    time.sleep(time_delta.total_seconds())

        print("importing files:", i, ":", i + chunk_size)
        # setup list of dictionary with import requests
        imports = [
            {
                "volume": volume,
                "location": row["genomic_file_external_id"],
                "project": project,
                "name": strip_path(row["genomic_file_external_id"]),
                "overwrite": True,
                "metadata": {
                    "Kids First Participant ID": row["participant_id"],
                    "Kids First Biospecimen ID": row["biospecimen_id"],
                },
            }
            for index, row in file_df[i : i + chunk_size].iterrows()  # noqa
        ]

        # initiate bulk import of batch and wait until finished
        responses = api.imports.bulk_submit(imports)
        while any([is_running(r) for r in responses]):
            time.sleep(10)
            responses = api.imports.bulk_get([r.resource for r in responses])
        response_iter = iter(responses)
        for import_item in imports:
            import_item["response"] = next(response_iter)
        # set the metadata on each file
        result_list = [r.resource.result for r in final_responses]
        files = []
        for import_item in imports:
            file_ = import_item["response"].resource.result
            file_.metadata = import_item["metadata"]
            files.append(file_)
        file_responses = api.files.bulk_edit(files)
        final_responses.extend(responses)
    return final_responses


file_df = pd.DataFrame(
    file_table,
    columns=(
        "participant_id",
        "biospecimen_id",
        "family_id",
        "vital_status",
        "ethnicity",
        "race",
        "gender",
        "genomic_file_id",
        "genomic_file_external_id",
    ),
).drop_duplicates()

print("about to import", len(file_df), "files")

# Run the import Job
responses = bulk_import_files(
    file_df=file_df,
    volume=my_volume,
    project=my_project,
)

print("Checking to see job status")
# Check to see if all the jobs completed
jobs = []
failed_jobs = []
for job in responses:
    j = job.resource
    job_dict = {
        "id": j.id,
        "state": j.state,
        "source_volume": j.source.volume,
        "source_location": j.source.location,
        "destination_project": j.destination.project,
        "destination_name": j.destination.name,
        "href": j.href,
        "started_on": j.started_on,
        "finished_on": j.finished_on,
        "result": j.result,
        "error": j.error,
    }
    print("Job ID:", j.id)
    print("Status:", j.state)
    if j.state == "COMPLETED":
        job_dict["result"] = {
            "href": j.result.href,
            "id": j.result.id,
            "is_folder": j.result.is_folder(),
            "modified_on": j.result.modified_on,
            "name": j.result.name,
        }
    if j.state == "FAILED":
        job_dict["error"] = {
            "code": j.error.code,
            "status": j.error.status,
            "message": j.error.message,
            "more_info": j.error.more_info,
        }
        failed_jobs.append(job)
        print("error code: ", j.error.code)
        print("error message:", j.error.message)
        print("error info:", j.error.more_info)
    print("\n")
    jobs.append(job_dict)

with open("job_report.json", "w+") as f:
    json.dump(jobs, f, indent=4, default=str)


if failed_jobs:
    print("There were", len(failed_jobs), "failed jobs")

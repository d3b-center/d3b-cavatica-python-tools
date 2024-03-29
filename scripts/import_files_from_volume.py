import argparse
import json
import time
from logging import Logger

import pandas as pd
import psycopg2
import sevenbridges as sbg
from d3b_cavatica_tools.utils.common import get_key, strip_path
from d3b_cavatica_tools.utils.logging import get_logger
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm


def get_args():
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

    parser.add_argument(
        "--log_level",
        metavar="Logging Level",
        type=str,
        help="""Logging level to use. One of: "DEBUG", "INFO", "WARNING", "ERROR"
    , "CRITICAL"
                """,
        default="INFO",
    )
    return parser.parse_args()


def fetch_data_view(query, vars=None):
    cur = conn.cursor()
    cur.execute(query, vars)
    resp = cur.fetchall()
    cur.close()
    return resp


def is_running(response):
    if not response.valid or response.resource.error:
        if not response.valid:
            breakpoint()
        else:
            logger.error(
                "\n\t".join(
                    [
                        "Import Job Error:",
                        str(response.resource.error),
                        response.resource.error.message,
                        response.resource.error.more_info,
                    ]
                    + [f"{k}: {v}" for k, v in response.resource._old.items()]
                )
            )
    return response.resource.state not in ["COMPLETED", "FAILED", "ABORTED"]


def bulk_import_files(
    file_df, volume, project, overwrite=True, chunk_size=100
):
    "Imports list of files from volume in bulk"
    final_responses = []
    # import files in batches of 100 each
    for i in range(0, len(file_df), chunk_size):
        logger.info(f"importing files: {i}:{i + chunk_size}")
        # setup list of dictionary with import requests
        imports = [
            {
                "volume": volume,
                "location": get_key(row["url"]),
                "project": project,
                "name": strip_path(row["url"]),
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
            logger.debug("Waiting 10 seconds for the jobs to complete")
            time.sleep(10)
            responses = api.imports.bulk_get([r.resource for r in responses])
        response_iter = iter(responses)
        for import_item in imports:
            import_item["response"] = next(response_iter)
        # set the metadata on each file
        files = []
        for import_item in imports:
            if import_item["response"].resource.state == "COMPLETED":
                file_ = import_item["response"].resource.result
                file_.metadata = import_item["metadata"]
                files.append(file_)
        file_responses = api.files.bulk_edit(files)
        final_responses.extend(responses)
    return final_responses


if __name__ == "__main__":
    logger = get_logger(__name__, testing_mode=False)
    args = get_args()
    logger.setLevel(args.log_level)
    logger.info(f"Args: {args.__dict__}")

    # Check if the file list exists
    # if not os.path.isfile(args.file_list):
    #     print("The file list can't be found")
    #     sys.exit()

    config_file = sbg.Config(profile=args.sbg_profile)
    api = sbg.Api(
        config=config_file,
        error_handlers=[
            sbg.http.error_handlers.rate_limit_sleeper,
            sbg.http.error_handlers.maintenance_sleeper,
        ],
    )

    # Volume Set up -----

    volume_name = args.volume
    # Check if the volume specified exists
    all_volumes = api.volumes.query(limit=100)
    my_volume = [v for v in all_volumes.all() if v.name == volume_name]

    if not my_volume:
        logger.error(
            f"Volume {volume_name} does not exist, check name / mounting"
        )
        raise KeyboardInterrupt
    else:
        my_volume = my_volume[0]

    logger.info(f"Volume {my_volume.name} found.")

    # Project Setup -----

    # [USER INPUT] Set project name here:
    new_project_name = args.project

    # What are my funding sources?
    billing_groups = api.billing_groups.query()

    my_billing_group = [
        bg for bg in billing_groups.all() if bg.name == args.billing_group
    ]
    if not my_billing_group:
        logger.error(
            f"Billing Group {args.billing_group} does not exist, check name / mounting"  # noqa
        )
        raise KeyboardInterrupt
    else:
        my_billing_group = my_billing_group[0]

    # check if this project already exists. LIST all projects and check for name
    #  match
    my_project = [
        p
        for p in api.projects.query(limit=100).all()
        if p.name == new_project_name
    ]

    if my_project:
        logger.info(
            f"Project {new_project_name} already exists, skipping creation"
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

        logger.info(f"Your new project {my_project.name} has been created.")
        if hasattr(my_project, "description"):
            logger.info(f"Project description: {my_project.description} \n")

    my_files = api.files.query(limit=100, project=my_project)
    if my_files.total == 0:
        logger.debug("no files in the project")

    # File Import -----
    gf_ids = pd.read_csv(args.file_list, header=None)[0].to_list()

    # Convert gf_ids into file names

    connection_url = args.db_connection_url
    conn = psycopg2.connect(connection_url)
    query = f"""
    SELECT bs.participant_id,
                bsgf.biospecimen_id,
                p.family_id,
                ox.vital_status,
                p.ethnicity,
                p.race,
                p.gender,
                bsgf.genomic_file_id,
                idx_scrape.url
    FROM participant p
    JOIN biospecimen bs ON p.kf_id = bs.participant_id
    JOIN biospecimen_genomic_file bsgf ON bs.kf_id = bsgf.biospecimen_id
    JOIN genomic_file gf ON gf.kf_id = bsgf.genomic_file_id
    LEFT JOIN outcome ox ON ox.participant_id = p.kf_id
    LEFT JOIN file_metadata.indexd_scrape idx_scrape on gf.latest_did = idx_scrape.did
    WHERE gf.kf_id in ({str(gf_ids)[1:-1]})
    """

    file_df = pd.read_sql(query, conn)

    logger.info(f"about to import {len(file_df)} files")
    # Run the import Job
    responses = bulk_import_files(
        file_df=file_df,
        volume=my_volume,
        project=my_project,
    )
    logger.info("Building job reports. This may take a few minutes.")
    # Check to see if all the jobs completed
    jobs = []
    failed_jobs = []
    with logging_redirect_tqdm():
        for job in tqdm(responses):
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
            logger.debug(f"Job ID: {job_dict['id']}")
            if j.state == "COMPLETED":
                job_dict["result"] = {
                    "href": job.resource.result.href,
                    "id": job.resource.result.id,
                    "is_folder": job.resource.result.is_folder(),
                    "modified_on": job.resource.result.modified_on,
                    "name": job.resource.result.name,
                }
            elif j.state == "FAILED":
                job_dict["error"] = {
                    "code": j.error.code,
                    "status": j.error.status,
                    "message": j.error.message,
                    "more_info": j.error.more_info,
                }
                failed_jobs.append(job_dict)
                logger.error(
                    "\n\t".join(
                        [
                            "Job info",
                            f"Job ID: {j.id}",
                            f"Status: {j.state}",
                            f"error code: {j.error.code}",
                            f"error message: {j.error.message}",
                            f"error info: {j.error.more_info}",
                        ]
                    )
                )
            else:
                logger.error(
                    "\n\t".join(
                        [
                            "other job status",
                            f"Job ID: {j.id}",
                            f"Status: {j.state}",
                        ]
                    )
                )
            jobs.append(job_dict)

    with open("job_report.json", "w+") as f:
        json.dump(jobs, f, indent=4, default=str)

    if failed_jobs:
        logger.error(f"There were {len(failed_jobs)} failed jobs")
        with open("job_report_errors.json", "w+") as f:
            json.dump(failed_jobs, f, indent=4, default=str)

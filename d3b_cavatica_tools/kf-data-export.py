import sevenbridges as sbg
import argparse
import os
import csv
import boto3
import time

breakpoint()
# print datetime.datetime.now()

base_url = "https://cavatica-api.sbgenomics.com/v2/"
token = os.environ["CAVATICA_ZYK_TOKEN"]
api = sbg.Api(url=base_url, token=token)

parser = argparse.ArgumentParser()
parser.add_argument("-list", required=True)
parser.add_argument("-vol", required=True)
args = parser.parse_args()

input_file = csv.DictReader(open(args.list))
task_list = []
for i in input_file:
    task_list.append(i)

drc = boto3.session.Session(profile_name="chop")
client = drc.client("s3")

v = api.volumes.get(args.vol)
exports = []
edict = {}

while len(task_list) or len(exports) > 0:
    # print datetime.datetime.now(),
    # print "task_list =", len(task_list), "exports =", len(exports)
    for i in task_list:
        t = api.tasks.get(i["tid"])
        # print datetime.datetime.now(),
        # print "now deal with", t.name, t.status
        if t.status == "COMPLETED":
            task_list.remove(i)
            p = t.project
            cram = t.outputs["cram"]
            gvcf = t.outputs["gvcf"]
            crai = api.files.query(project=p, names=[cram.name + ".crai"])[0]
            tbi = api.files.query(project=p, names=[gvcf.name + ".tbi"])[0]
            for f in [cram, gvcf, crai, tbi]:
                # print datetime.datetime.now(),
                # print "now deal with", f.name
                try:
                    edict["export"] = api.exports.submit_export(
                        file=f, volume=v, location="harmonized/" + f.name
                    )
                    # print datetime.datetime.now(),
                    # print "now export " + f.name
                    edict["kid"] = i["kid"]
                    edict["tid"] = i["tid"]
                    exports.append(edict)
                except:
                    print("{} can't be export.".format(f.name))
                    continue
    # print datetime.datetime.now(), "now check status"
    for e in exports:
        # print datetime.datetime.now(),
        # print "bucket " + v.service['bucket']
        # print "key " + e['export'].destination.location
        # print "now check " + e['export'].source.name
        if e["export"].reload().state == "COMPLETED":
            c_fileid = e["export"].source.id
            # print e['kid'], c_fileid, e['tid']
            client.put_object_tagging(
                Bucket=v.service["bucket"],
                Key=e["export"].destination.location,
                Tagging={
                    "TagSet": [
                        {"Key": "KidsFirst_ID", "Value": e["kid"]},
                        {"Key": "CavaticaFile", "Value": c_fileid},
                        {"Key": "CavaticaTask", "Value": e["tid"]},
                    ]
                },
            )
            exports.remove(e)
    time.sleep(3)

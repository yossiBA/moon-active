#!/usr/bin/python
# imports
import datetime
import json
from multiprocessing import Pool
import sys
import os
import argparse

def mkdirs(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)

def log(msg):
    with open(logfile, "a+") as log_file:
        date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print "[%s]: %s" % (date, msg)
        log_file.write("[%s]: %s \n" % (date, msg))
        log_file.close()


def upload_report(file_location):
    """Uploads reports to the bucket."""
    cmd = "gsutil cp %s %s" % (file_location, report_output)
    log(cmd)
    cp_status = os.system(cmd)
    if not cp_status == 0:
        raise BaseException("error while running %s." % cmd)


def execute_query(sql):
    """Execute the query and save the result locally"""
    log(sql)  # Simulation of query execution
    date = "_" + datetime.datetime.now().strftime("%Y_%m_%d_%H_%M")
    report_location = output_dir + sql.replace('SELECT * FROM ', '').replace(';', '') + date
    with open(report_location, "w") as report:
        report.write(sql)
        report.close()
    upload_report(report_location)


def execute_job(queries_flow):
    log("INFO: Start executing reports")
    for key, value in sorted(queries_flow.items()):
        try:
            if len(value) > 1:  # if need to run in parallel
                p = Pool(max_pool)
                p.map(execute_query, value)
                p.close()
                p.join()
            else:   # if need to run serial
                execute_query(value[0])
        except:
                print "Unexpected error:", sys.exc_info()[0]
                raise
    print report_output


def read_query_file(file_name):
    with open(file_name, "r") as q:
        query = str(q.read())
        q.close()
        return query


def prepare_job(job_file):
    with open(job_file) as job:
        job_config = json.load(job)  # read job json config
        log("INFO: Start preparing job - %s" % job_config["jobName"])
        for key, value in job_config["reportsFlow"].items():
            for i in range(0, len(value)):
                value[i] = read_query_file(reports_dir + value[i])
    return job_config


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Parallel/Serial reports processor')
    parser.add_argument('job_name', help='json job config name e.g job_1', default='job_1')
    parser.add_argument('--base_dir', help='base src directory', default=os.path.dirname(os.path.realpath(__file__)))
    parser.add_argument('--max_pool', help='max multiprocess', default=3)

    args = parser.parse_args()

    mkdirs(args.base_dir + '/logs/')
    mkdirs(args.base_dir + '/output/')

    logfile = args.base_dir + '/logs/{}.log'.format(args.job_name)
    jobFile = args.base_dir + '/jobs/{}.json'.format(args.job_name)
    output_dir = args.base_dir + '/output/'
    reports_dir = args.base_dir + '/reports/'
    max_pool = args.max_pool

    job_def = prepare_job(jobFile)
    report_output = job_def["reportOutput"]
    execute_job(job_def["reportsFlow"])
    log("INFO: Executing job - %s finished successfully" % job_def["jobName"])
import datetime
import json
from multiprocessing import Pool


def log(msg):
    with open(logfile, "a+") as log_file:
        date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print "[%s]: %s" % (date, msg)
        log_file.write("[%s]: %s \n" % (date, msg))
        log_file.close()


def read_query_file(file_name):
    with open(file_name, "r") as query:
        return str(query.read())


def prepare_job(job_file):
    with open(job_file) as job:
        job_config = json.load(job)
        log("INFO: Start preparing job - %s" % job_config["jobName"])
        for key, value in job_config["reportsFlow"].items():
            for i in range(0, len(value)):
                value[i] = read_query_file(reports_dir+value[i])
    return job_config


def execute_query(sql):
    print(sql)


def execute_job(queries_flow, report_output):
    for key, value in sorted(queries_flow.items()):
        if len(value) > 1:
            with Pool(3) as p:
                print(p.map(execute_query, [1, 2, 3]))
        else:
            execute_query(value)
    print report_output


if __name__ == "__main__":
    logfile = 'logs/etl.log'
    jobFile = 'jobs/job_1.json'
    reports_dir = 'reports/'

    job_def = prepare_job(jobFile)
    execute_job(job_def["reportsFlow"],job_def["reportOutput"])
    log("INFO: End Process")
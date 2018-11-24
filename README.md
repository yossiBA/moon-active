# moon-active

What Is This?
-------------

This program intended to provide a framework for creating reports for analysts.
Any analyst can create a  JSON job config file and define the flow in which he wants to run a series of queries when he has the ability to run the queries in:
1.Parallel e.g:
          Query2 ---|
                    |---> Query5
          Query4 ---|

2.Serial e.g:
            Query1 -> Query2 -> Query3
And when the query finished executing the output uploaded to google bucket.

How To Install This Program
--------------------------

1. Clone this repository to the server/machine in which you want to execute this program.

2. Execute Bash commend$ [sudo] pip install -r requirements.txt

How To Use This Program
-----------------------

1. Add to jobs directory the JSON job config e.g:
-------------------------------------------------------------------------------
{
    "jobName":"Coin Master parallel reports processing",
    "description":"step 1: reports 1-3 in parallel, step 2: reports 4-5 in parallel",
    "reportsFlow": {
                     "1": ["report_1.sql", "report_2.sql", "report_3.sql"],
                     "2": ["report_4.sql", "report_5.sql"],
                     "3": ["report_6.sql"]
                   },
    "reportOutput":"gs://moon_bi_test_fghkdfd34/"
}
-------------------------------------------------------------------------------
when running reportes in parallel the queries that we desire to run in paralle will be a list in the reportsFlow, in this example reports 1-3 will run in parallel and when the last of them finishd the second step will start and run reports 4-5 allso in parallel when finished the last step(3) will execute report 6 (if we want to run queries serialy evry step will contain one report ).

2. Add to reports directory the reports (reports 1-6 in the above example) files that contain the query which you want to execute.

3. Add parameter job name in this format e.g:  job_1.json --> job_1 (without .json)

4. The attribute max_pool limit the number of processes that can run in parallel (default is 3 when there a need to run more than 3 add another parameter with the number of max_pool but! no more than the number of CPU core that the machine/server has)

5. Execute/Schedule this program with Rundeck/crontab etc...

** In case you need to investigate the execution, the program writes logs to a file in the format of job_name.log under the logs directory.

Enjoy :)

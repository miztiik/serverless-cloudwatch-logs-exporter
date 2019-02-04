# -*- coding: utf-8 -*-
"""
.. module: Export Logs from cloudwatch & Store in given S3 Bucket
    :platform: AWS
    :copyright: (c) 2019 Mystique.,
    :license: Apache, see LICENSE for more details.
.. moduleauthor:: Mystique
.. contactauthor:: miztiik@github issues
"""

#####################################################################################
#                                                                                   #
#               SOME REMINDERS - THINGS TO REMEMBER & FOLLOW                        #
#                                                                                   #
#####################################################################################
"""
- The Cloudwatch logs to be monitored should be in the lambda env variables and comma(,) separated.
-- The log group filter will match log group "as-is", i.e it is ASSUMED to be case sensitive,  So make sure the correct log group full names as it appears in the env variable. 
-- For example: "/aws/lambda/log-group-name". 
-- For multiple log groups: "/aws/lambda/lg1,/aws/lambda/lg2,/aws/lambda/lg3"
- The S3 Bucket MUST be in the same region
- The `retention_days` defaults to 90 days, Customize in `global_vars`
-- AWS CW Log exports doesn't effectively keep track of logs that are exported previously in a native way. 
-- To avoid exporting the same data twice, this script uses a timeframe of 24 hour period. This period is the 90th day in the past.
-- Run the script everyday to keep the log export everyday.
----------------------|<------LOG EXPORT PERIOD------>|---------------------------------------|
                    91stDay                        90thDay                                   Today
- The default time for awaiting task completion is 5 Minutes(300 Seconds). Customize in `global_vars`
- FROM AWS Docs,Export task:One active (running or pending) export task at a time, per account. This limit cannot be changed.
-- Ref[1] - https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
- The lambda/python is written and tested for v3.7
- Increase Lambde Timeout based on requirements.
- Lambda IAM - Role: CloudWatch Access List/Read & S3 Bucket - HEAD, List, Put. I tested with slighly elevated S3 privileges, But should be able to tighten it.
"""
########################################################
#                                                      #
#               S3 BUCKET POLICY JSON                  #
#                                                      #
########################################################
"""
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "logs.YOUR-REGION.amazonaws.com"
            },
            "Action": "s3:GetBucketAcl",
            "Resource": "arn:aws:s3:::YOUR-BUCKET-NAME"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "logs.YOUR-REGION.amazonaws.com"
            },
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::YOUR-BUCKET-NAME/*",
            "Condition": {
                "StringEquals": {
                    "s3:x-amz-acl": "bucket-owner-full-control"
                }
            }
        }
    ]
}

TODO: Future Enhancement to beautify logs?? Already pretty verbose, maybe prettify
TODO: Describe LogStreams before export task, to avoid dummy files?
"""
import boto3
import os
import uuid
import time
import datetime
import asyncio
import json
import logging
from botocore.vendored import requests
from botocore.client import ClientError

# Initialize Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def set_global_vars():
    """
    Set the Global Variables
    If User provides different values, override defaults

    This function returns the AWS account number

    :return: global_vars
    :rtype: dict
    """
    global_vars = {"status": False}
    try:
        global_vars["Owner"]                    = "Mystique"
        global_vars["Environment"]              = "Prod"
        global_vars["aws_region"]               = "us-east-1"
        global_vars["tag_name"]                 = "serverless_cloudwatch_logs_exporter"
        global_vars["retention_days"]           = 90
        global_vars["cw_logs_to_export"]        = ["/aws/lambda/indian-space-facts","/aws/lambda/Asislambda"]
        #global_vars["cw_logs_to_export"]        = os.environ.get("cw_logs_to_export").split(",")
        global_vars["log_dest_bkt"]             = "cw-log-exports-01"
        global_vars["time_out"]                 = 300
        global_vars["status"]                   = True
    except Exception as e:
        logger.error("Unable to set Global Environment variables. Exiting")
        global_vars["error_message"]            = str(e)
    return global_vars

def gen_uuid():
    """ Generates a uuid string and return it """
    return str( uuid.uuid4() )

def gen_ymd_from_epoch(t):
    """ Generates a string of the format "YYYY-MM-DD" from the given epoch time"""
    # Remove the milliseconds
    t = t/1000
    ymd =  ( str(datetime.datetime.utcfromtimestamp(t).year) + \
                "-" + \
                str(datetime.datetime.utcfromtimestamp(t).month) + \
                "-"  + \
                str(datetime.datetime.utcfromtimestamp(t).day)
    )
    return ymd

def gen_ymd(t):
    """ Generates a string of the format "YYYY-MM-DD" from datetime"""
    ymd =  ( str(t.year) + "-" + str(t.month) + "-"  + str(t.day) )
    return ymd

def does_bucket_exists( bucket_name ):
    """
    Check if a given S3 Bucket exists and return a boolean value. The S3 'HEAD' operations are cost effective

    :param name: bucket_name
    :type value: str

    :return: bucket_exists_status
    :rtype: dict
    """
    bucket_exists_status = { 'status':False, 'error_message':'' }
    try:
        s3 = boto3.resource('s3')
        s3.meta.client.head_bucket( Bucket = bucket_name )
        bucket_exists_status['status'] = True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            bucket_exists_status['status'] = False
            bucket_exists_status['error_message'] = str(e)
        else:
            # logger.error('ERROR: {0}'.format( str(e) ) )
            bucket_exists_status['status'] = False
            bucket_exists_status['error_message'] = str(e)
    return bucket_exists_status

def get_cloudwatch_log_groups(global_vars):
    """
    Get the list of Cloudwatch Log groups

    :param global_vars: The list of global variables
    :param type: json

    :return: resp_data Return a dictionary of data, includes list of 'log_groups'
    :rtype: json
    """
    resp_data = {'status': False, 'log_groups':[], 'error_message': ''}
    client = boto3.client('logs')
    try:
        # Lets get all the logs
        resp = client.describe_log_groups( limit = 50 )
        resp_data['log_groups'].extend( resp.get('logGroups') )
        # Check if the results are paginated
        if resp.get('nextToken'):
            while True:
                resp = client.describe_log_groups( nextToken = resp.get('nextToken'), limit = 50 )
                resp_data['log_groups'].extend( resp.get('logGroups') )
                # Check & Break, if the results are no longer paginated
                if not resp.get('nextToken'):
                    break
        resp_data['status'] = True
    except Exception as e:
        resp_data['error_message'] = str(e)
    return resp_data

def filter_logs_to_export(global_vars, lgs):
    """
    Get a list of log groups to export by applying filter

    :param global_vars: The list of global variables
    :param type: json
    :param lgs: The list of CloudWatch Log Groups
    :param type: json

    :return: resp_data Return a dictionary of data, includes list of filtered 'log_groups'
    :rtype: json
    """
    resp_data = {'status': False, 'log_groups':[], 'error_message': ''}
    # Lets filter for the logs of interest
    for lg in lgs.get('log_groups'):
        if lg.get('logGroupName') in global_vars.get('cw_logs_to_export'):
            resp_data['log_groups'].append(lg)
            resp_data['status'] = True
    return resp_data

async def export_cw_logs_to_s3(global_vars, log_group_name, retention_days, bucket_name, obj_prefix = None):
    """
    Export the logs in the log_group to the given S3 Bucket. Creates a subdirectory(prefix). Defaults to the log group name

    :param global_vars: The list of global variables
    :param type: json
    :param log_group_name: The name of the log group to be exported
    :param type: json
    :param retention_days: The number of days older logs to be archived. Defaults to '90'
    :param type: json
    :param bucket_name: The list of CloudWatch Log Groups
    :param type: str
    :param obj_prefix: The list of CloudWatch Log Groups
    :param type: str

    :return: resp_data Return a dictionary of data, includes list of 'log_groups'
    :rtype: json
    """
    resp_data = {'status': False, 'task_info':{}, 'error_message': ''}
    if not retention_days: retention_days = 90
    if not obj_prefix: obj_prefix = log_group_name.split('/')[-1]
    now_time = datetime.datetime.now()
    # To effectively archive logs
    # Setting for 24 Hour time frame (From:91th day); Captures the 24 hour logs on the 90th day
    n1_day = now_time - datetime.timedelta(days = int(retention_days) + 1)
    # Setting for 24 Hour time frame (Upto:90th day)
    n_day = now_time - datetime.timedelta(days = int(retention_days))
    f_time = int(n1_day.timestamp() * 1000)
    t_time = int(n_day.timestamp() * 1000)
    d_prefix = str( log_group_name.replace("/","-")[1:] ) + "/" +str( gen_ymd(n1_day) )
    # d_prefix = str( log_group_name.replace("/","-")[1:] )
    # Check if S3 Bucket Exists
    resp = does_bucket_exists(bucket_name)
    if not resp.get('status'):
        resp_data['error_message'] = resp.get('error_message')
        return resp_data
    try:
        client = boto3.client('logs')
        r = client.create_export_task(
                taskName = gen_uuid(),
                logGroupName = log_group_name,
                fromTime = f_time,
                to = t_time,
                destination = bucket_name,
                destinationPrefix = d_prefix
                )
        # Get the status of each of those asynchronous export tasks
        r = get_tsk_status(r.get('taskId'), global_vars.get('time_out'))
        if resp.get('status'):
            resp_data['task_info'] = r.get('tsk_info')
            resp_data['status'] = True
        else:
            resp_data['error_message'] = r.get('error_message')
    except Exception as e:
        resp_data['error_message'] = str(e)
    return resp_data

def get_tsk_status(tsk_id, time_out):
    """
    Get the status of the export task list until `time_out`.

    :param tsk_id: The task id of CW Log export
    :param type: str
    :param time_out: The mount of time to wait in seconds
    :param type: str

    :return: resp_data Return a dictionary of data, includes list of 'log_groups'
    :rtype: json
    """
    resp_data = {'status': False, 'tsk_info':{}, 'error_message': ''}
    client = boto3.client('logs')
    if not time_out: time_out = 300
    t = 3
    try:
        # Lets get all the logs
        while True:
            resp = client.describe_export_tasks(taskId = tsk_id)
            tsk_info = resp['exportTasks'][0]
            if t > int(time_out):
                resp_data['error_message'] = f"Task:{tsk_id} is still running. Status:{tsk_info['status']['code']}"
                resp_data['tsk_info'] = tsk_info
                break
            if tsk_info['status']['code'] != "COMPLETED":
                # Crude exponential back off
                t*=2
                time.sleep(t)
            else:
                resp_data['tsk_info'] = tsk_info
                resp_data['status'] = True
                break
    except Exception as e:
        resp_data['error_message'] = f"Unable to verify status of task:{tsk_id}. ERROR:{str(e)}"
    return resp_data

def lambda_handler(event, context):
    """
    Entry point for all processing. Load the global_vars

    :return: A dictionary of tagging status
    :rtype: json
    """
    """
    Can Override the global variables using Lambda Environment Parameters
    """
    global_vars = set_global_vars()

    resp_data = {"status": False, "error_message" : '' }

    if not global_vars.get('status'):
        logger.error('ERROR: {0}'.format( global_vars.get('error_message') ) )
        resp_data['error_message'] = global_vars.get('error_message')
        return resp_data

    lgs = get_cloudwatch_log_groups(global_vars)
    if not lgs.get('status'):
        logger.error(f"Unable to get list of cloudwatch Logs.")
        resp_data['error_message'] = lgs.get('error_message')
        return resp_data

    f_lgs = filter_logs_to_export(global_vars, lgs)
    if not (f_lgs.get('status') or f_lgs.get('log_groups')):
        err = f"There are no log group matching the filter or Unable to get a filtered list of cloudwatch Logs."
        logger.error( err )
        resp_data['error_message'] = f"{err} ERROR:{f_lgs.get('error_message')}"
        resp_data['lgs'] = {'all_logs':lgs, 'cw_logs_to_export':global_vars.get('cw_logs_to_export'), 'filtered_logs':f_lgs}
        return resp_data

    # TODO: This can be a step function (or) async 'ed
    resp_data['export_tasks'] = []
    #loop = asyncio.get_event_loop()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # asyncio.set_event_loop(asyncio.new_event_loop())
    # wait_coro = asyncio.wait(to_do) 
    # res, _ = loop.run_until_complete(wait_coro)

    # Lets being the archving/export process
    for lg in f_lgs.get('log_groups'):
        resp = loop.run_until_complete( export_cw_logs_to_s3( global_vars, lg.get('logGroupName'),global_vars.get('retention_days'), global_vars.get('log_dest_bkt')) )
        print(resp)
        resp_data['export_tasks'].append(resp)
    loop.close()
    # If the execution made it through here, no errors in triggering the export tasks. Set to True.
    resp_data['status'] = True
    return resp_data

if __name__ == '__main__':
    lambda_handler(None, None)
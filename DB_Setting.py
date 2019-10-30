#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Nokia Proprietary - Nokia Internal Use
#
# SharePoint download files configuration file.
#
# Do not change anything around the values, just the value within single quotes.
#
# url - The URL to the SharePoint destination
# username - Username as whom to log into SharePoint in order to download files.
# password - Pasword of username to log into SharePoint
#
from builtins import str

import pyodbc
import os
from datetime import datetime

settings = {
    'url': 'https://nokia.sharepoint.com/sites/SageDS',
    'username': 'svcsage@nokia.onmicrosoft.com',
    'password': 'sage12345!'
}
SMTP_Server = {
    'smtp_server': "smtp.office365.com",
    'port': '587',  # for Starttls
    'sender_email': "analytics.sage.ext@nokia.com",
    'password': "Nokia123"
}

sql_db_settings = {
    'SAGE_ETL_DB_HOST': '10.76.62.204',
    'SAGE_Test_DBNAME': 'MUKESH_DEV',
    'SAGE_STG_DBNAME': 'SAGESTG',
    'SAGE_DB_USER': 'sage_etl_rw',
    'SAGE_DB_PASS': 'wR@st1eNg'
}

log_settings = {
    'batch_id': 1,
    'phase_id': 2,
    'user_name': 'sage_etl_rw'
}

Project_Name='OPEX' #Project Name parameter for V_Files_View

# runtime_settings = {'scriptpath' : ''}

connStrTest = 'Driver={SQL Server};' \
              'Server=' + sql_db_settings['SAGE_ETL_DB_HOST'] + ';' \
                                                                'Database=' + sql_db_settings['SAGE_STG_DBNAME'] + ';' \
                                                                                                                    'uid=' + \
              sql_db_settings['SAGE_DB_USER'] + ';' \
                                                'pwd=' + sql_db_settings['SAGE_DB_PASS'] + ';'


# conn = pyodbc.connect(connTestDB) #Commented by Mukesh
# cursor = conn.cursor()#Commented by Mukesh
# print('conn=',conn)
# print('cusor=',cursor)

# def set_rumtime_settings(arg, pathvalue):
#     runtime_settings[arg] = pathvalue

def get_cursor(sqlcmd):
    try:
        conn = pyodbc.connect(connStrTest)
        cursor = conn.cursor()
        # print("sqlcmd = ", sqlcmd)
        cursor.execute(sqlcmd)
    except Exception as _e:
        print('sqlcmd =', sqlcmd)
        _msg = ("Error in executing' -> {}".format(_e))
        cursor.close()
        raise ValueError(_msg)

    columns = [column[0] for column in cursor.description]
    # print(columns)
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
        # print('type of result\n',type(results))
        # print('result\n',results)
    cursor.close()
    return results


def dml_cursor(sqlcmd):
    try:
        conn = pyodbc.connect(connStrTest)
        cursor = conn.cursor()
        # print("in dml_cursor sqlcmd = ", sqlcmd)
        cursor.execute(sqlcmd)
        cursor.execute("commit")
        cursor.close()
    except Exception as _e:
        print("in dml_cursor sqlcmd = ", sqlcmd)
        _msg = ("Error in executing ' -> {}".format(_e))
        cursor.close()
        raise ValueError(_msg)


def run_os_cmd(os_cmd):
    try:
        print('Initiating System Command1 -> \n' + os_cmd)
        os.system(os_cmd)
        print("\n")
    except Exception as _e:
        _msg = ("Error while executing " + os_cmd + "{}".format(_e))
        raise ValueError(_msg)


def insert_log(exec_id, proc_name, statement_type, table_proc_name, num_of_rows, success_failure):
    batch_id = log_settings['batch_id']  # Daily
    phase_id = log_settings['phase_id']  # Quality Reports
    user_name = "'" + log_settings['user_name'] + "'"
    proc_name = "'" + proc_name.replace('_STG_LOAD', '') + "'"
    statement_type = "'" + statement_type + "'"
    table_proc_name = "'" + table_proc_name + "'"
    # num_of_rows = 'null'
    task_statement_num = str(1)
    start_time = "'" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "'"
    end_time = start_time
    success_failure = "'" + success_failure + "'"

    # sqlcmd="select id task_exec_id from T_TASK_MASTER where batch_id = 1 and task_name = " + proc_name + ";"
    # results = get_cursor(sqlcmd)
    # for row in results:
    task_exec_id = 0  # row['task_exec_id']
    # print("task_exec_id = ", task_exec_id)
    task_exec_id = str(task_exec_id)
    sqlcmd = "exec P_PROCESS_STG_LOG " + task_exec_id + ',' + str(exec_id) + ',' + str(batch_id) \
             + ',' + str(phase_id) + ',' + user_name + ',' + proc_name + ',' + statement_type \
             + ',' + task_statement_num + ',' + table_proc_name + ',' + num_of_rows + ',' + start_time \
             + ',' + end_time + ',' + success_failure
    # print ("exec P_PROCESS_DWH_LOG = " , sqlcmd)
    dml_cursor(sqlcmd)

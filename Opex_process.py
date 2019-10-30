#!/usr/bin/env python
#LoA_Process.py

import os
import shutil
import pyodbc
from datetime import datetime
import DB_Setting as qrs
#import datedelta

#Get Process Mon_YYYY as of today
today = datetime.now()
Mon_YYYY = today.strftime("%b %Y")
period = 'P' + today.strftime("%m")

#timestamp = str(datetime.now())
timestamp = str(datetime.now()).replace(':', '_').replace('.','_')

#Insert Log
proc_name =  qrs.Project_Name+'-Main Process'
statement_type =  "Python Script -> Opex_process.py"
table_proc_name = 'null'

sqlcmd="SELECT coalesce(max(Exec_ID),0) + 1 exec_id FROM T_PROCESS_STG_LOG;"
results = qrs.get_cursor(sqlcmd)
for row in results:
	exec_id = row['exec_id']
	print("exec_id = ", exec_id)

script_path = os.path.dirname(os.path.abspath(__file__)) + "\\"

# qrs.set_rumtime_settings("scriptpath", _scriptpath)

qrs.insert_log(exec_id, proc_name,statement_type,table_proc_name,'null', "Started")
print(qrs.Project_Name,'\n')
sqlcmd = """
SELECT SRC_FOLDER_PATH, FILE_NAME_PATTERN, FILE_NAME_EXTENSION, DEST_FOLDER_PATH
,STRING_AGG(WORKSHEET_NAME, ',') WITHIN GROUP (ORDER BY WORKSHEET_NAME) AS WORKSHEET_NAMES
,STRING_AGG(TABLE_NAME, ',') WITHIN GROUP (ORDER BY WORKSHEET_NAME) AS TABLE_NAMES
FROM V_FILES_MASTER_STG where PROJECT='"""+qrs.Project_Name+"""' 
group by SRC_FOLDER_PATH, FILE_NAME_PATTERN, FILE_NAME_EXTENSION, DEST_FOLDER_PATH
"""
print (sqlcmd)
#cursor.execute(sqlcmd)

results = qrs.get_cursor(sqlcmd)
#print (results)
for row in results:
	src = '\"' + row['SRC_FOLDER_PATH'].replace('https://nokia.sharepoint.com','') +  Mon_YYYY + '\"'
	file_pattern = '\"' + row['FILE_NAME_PATTERN'] +'.' + row['FILE_NAME_EXTENSION'] + '\"'
	dest = '\"' + row['DEST_FOLDER_PATH']  + '\"'
	worksheet_names = '\"' + row['WORKSHEET_NAMES']  + '\"'
	table_names = '\"' + row['TABLE_NAMES']  + '\"'

	os_cmd = script_path + 'Opex_download_files.py --src ' + src + ' --dest ' + dest + ' --file_pattern ' + file_pattern + ' --log_level INFO' \
			 + ' --period ' + period + ' --worksheet_names ' + worksheet_names + ' --table_names ' + table_names + ' --exec_id ' + str(exec_id) \
			 + ' --script_path ' + script_path
	print ('Initiating System Command from qq_process-> \n' + os_cmd)
	qrs.run_os_cmd(os_cmd)

#cursor.close()
qrs.insert_log(exec_id, proc_name,statement_type,table_proc_name,'null', "Completed")
exit(1)


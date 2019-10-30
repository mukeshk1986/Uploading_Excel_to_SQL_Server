#!/usr/bin/env python
'''
###########################################################
    Name of Module      :  Opex_dynamic_upload_to_DB
    Purpose             :  Uploading Opex Excel sheets to SQL Server Database
    Parameter           :  Five parameters   1.Python file 2.Execution Id 3.Excel file 4.Table Name 5. Sheet Names( comma separted )
    Calling Information :  python Opex_dynamic_upload_to_DB.py 3 "C:\\Users\\m48kumar\\sage-etl\\files_downloaded\\P09 OPEX FPO V2.xlsx" T_TEMP_OPEX "FPO"
                           python Opex_dynamic_upload_to_DB.py 3 "C:\\Users\\sage-etl\\Opex\\P09 OPEX FPO V2.xlsx" T_TEMP_OPEX "FPO"
    Author              :  Mukesh Kumar
    Guided by           :  Saravanan
    Create date         :  Oct 20th, 2018
    Description         :  Script to parse excel and load into sql server database
    Version             :  1
    Updated By          :
    Updated on          :
    Additional Information: It can be called directly or from any other python file with parameters
###########################################################
'''

import pandas as pd
import numpy as np
import errno
import logging
import time
from sqlalchemy import create_engine, MetaData, Table, select
from six.moves import urllib
import datetime as dt
import DB_Setting as qrs
import sys
import os
import re
print (sys.argv)
## On command line 5 parameters are mandatory
if len(sys.argv) != 5:
    print("Usage: Opex_dynamic_upload_to_DB.py <exec_id> <XL_FilePath> <table_names sepetated by comma> <worksheet_names sepetated by comma>")
    exit(-1)
else:
    exec_id = sys.argv[1]
    file = sys.argv[2]
    table_names = sys.argv[3]
    worksheet_names = sys.argv[4]
# Defining Engine with host and credentials parameters from Database setting file
paramsTest = urllib.parse.quote_plus(qrs.connStrTest) #commented by Mukesh
engineTest = create_engine("mssql+pyodbc:///?odbc_connect=%s" % paramsTest) #commented by Mukesh
schemaTest = qrs.sql_db_settings['SAGE_STG_DBNAME'] #commented by Mukesh
created_by = qrs.log_settings['user_name']

#
# Set up logging to go to both a logfile and stdout
#
# logging.basicConfig(format='%(asctime)s  %(levelname)-10s %(processName)s  %(name)s %(message)s',filename=time.strftime("..\logs\DL-%Y-%m-%d.log") )
# LOG_LEVEL = logging.INFO
# LOG = logging.getLogger(__name__)
# LOG.setLevel(LOG_LEVEL)
# SH = logging.StreamHandler(sys.stdout)
# SH.setLevel(LOG_LEVEL)
# LOG.addHandler(SH)
## Functions to upload to sql server
def upload_to_db (xl_data, schema, tn, if_exists, engine):
    #print ("Table name = " ,tn)
    proc_name = tn
    statement_type = "Uploading to DB as " + if_exists + " in schema " + schema
    table_proc_name = tn
    #success_failure = "Success"
    qrs.insert_log(exec_id, proc_name, statement_type, table_proc_name, 'null', "Started")
    try:
        xl_data.to_sql(name=tn,con=engine, index=False, if_exists=if_exists)
    except Exception as e:
        statement_type="{} error ".format(e)
        qrs.insert_log(exec_id, proc_name, statement_type, table_proc_name, 'null', "Exception")
        print("Error :" , statement_type)
        # xl_data.to_csv(tn + '.csv', index=False)
        # xl_data = pd.read_csv(tn + '.csv')
        # xl_data.to_sql(name=tn, con=engine, index=False, if_exists=if_exists)
    num_of_rows = str(xl_data['Update_TO_STG'].count())
    qrs.insert_log(exec_id, proc_name, statement_type, table_proc_name, num_of_rows, "Completed")

def compare_df_cols_n_update_db(xl_data,tn, prev_df):
    col_pos_dict_o = {}
    prev_df_col_list = prev_df.columns.values
    for index, value in enumerate(prev_df_col_list, 0):
        col_pos_dict_o[value] = [index, prev_df[value].dtype]
    #print("col_pos_dict_o = ", col_pos_dict_o)

    col_pos_dict_n = {}
    xl_data_col_list = xl_data.columns.values
    for index, value in enumerate(xl_data_col_list, 0):
        col_pos_dict_n[value] = [index, xl_data[value].dtype]
    print (col_pos_dict_o.items() == col_pos_dict_n.items())

    if col_pos_dict_o.items() == col_pos_dict_n.items(): # old and new columns list are same
        upload_to_db(xl_data, schemaTest, tn, 'append', engineTest)#commented by Mukesh
    else:
        new_prev_df = pd.DataFrame(data={})
        #sync prev_df with xl_data_col_list in new_prev_df
        for index, value in enumerate(xl_data_col_list, 0):
            if value in col_pos_dict_o:
                #print ("Exists = ", index, value)
                new_prev_df.insert(index,value, prev_df[value])
            else:
                #print ("Does not Exist = ", index,  value)
                new_prev_df.insert(index, value, np.nan)
                new_prev_df[value] = new_prev_df[value].astype(xl_data[value].dtype, inplace=True)
        #sync xl_data and new_prev_df with prev_df_col_list
        for index, value in enumerate(prev_df_col_list, 0):
            if value in col_pos_dict_n == False:
                #print ("Does not Exist = ", index,  value)
                xl_data.insert(index, value, np.nan)
                xl_data[value] = prev_df[value].astype(prev_df[value].dtype, inplace=True)
                new_prev_df.insert(index, value, prev_df[value])
        if prev_df.equals(new_prev_df):
            upload_to_db(xl_data, schemaTest, tn, 'append', engineTest)
        else:
            upload_to_db(xl_data, schemaTest, tn, 'append', engineTest)
## Main parts of the file
xl_name = file
_xlfilename = os.path.basename(file)
ws_tn_dict = dict(zip(worksheet_names.split(','),table_names.split(',') ))
for (ws,tn)  in ws_tn_dict.items():
    today = dt.datetime.now()
    print("worksheet ", ws, "Started at ", dt.datetime.now(), "for table ", tn)
    # LOG.info("worksheet ",ws, "Started at ",dt.datetime.now(), "for table " , tn)
    xl = pd.ExcelFile(file)
    # reading First header row columns
    ## new way
    df1 = pd.read_excel(xl, header=[0], index=False, sheet_name=ws)
    df1.columns = df1.columns.str.replace(' ', '')
    df1 = df1.rename(columns={c: re.sub(r"[)(]", '', c.replace(ws, '')) for c in df1.columns})  # Remove spaces from columns
    df1.rename(columns={'Unnamed:5': 'Domain_Category'}, inplace=True)
    #df1.rename(columns={df1.columns[1]: "GranOPEX"},inplace=True)
    df1.rename(columns={ df1.columns[1]: "GranOPEX", df1.columns[2]:"QTDAct" }, inplace = True)## Sometimes they put column as OPEX and it was removed during the
    # df1 = pd.read_excel(xl, header=[0], index=False, sheet_name=ws) commented by Mukesh
    # df1.rename(columns={'Unnamed: 5': 'Domain_Category'}, inplace=True) #5th unnamed column is carrrying the Domain Category information #Mukesh
    cols1 = df1.columns.tolist()
    # reading Second header row columns
    df2 = pd.read_excel(xl, header=[1], index=False, sheet_name=ws)
    cols2 = df2.columns.tolist()
    # print(df2.head(10))
    # print('Second header=\n',cols2,'\n')
    list_cols = ''
    AllColumn = []
    for i in range(len(cols1)):
        if ("Unnamed" not in cols1[i]):

            list_cols = cols1[i]
        if ("Unnamed" in cols2[i]):
            if ("Unnamed" in cols1[i]):
                AllColumn.append('')
            else:
                AllColumn.append(str(list_cols))
        else:
            AllColumn.append(str(list_cols) + '-' + str(cols2[i]))
    xl_f_data = pd.read_excel(xl, header=[0, 1], index=False, sheet_name=ws,nrows=235 )
    xl_f_data.columns = AllColumn
    xl_f_data = xl_f_data.replace({'NaN': np.nan})
    xl_f_data.iloc[:, 5].fillna(method='ffill', inplace=True)
    if xl_f_data.columns[-1] != '': ## Sometimes user is keeping the last column as blank sometimes they didn't
        xl_f_data['End'] = np.nan
        xl_f_data.rename(columns={"End": ""}, inplace=True)
    #xl_data = pd.read_excel(xl, ws)
    # _filename = os.path.basename(file)
    today = dt.datetime.now()
    # ## added for dynamic insert
    # Creating the groups of dataframe to insert into the database
    col_array = np.array(xl_f_data.columns)
    blank_pos = np.argwhere(col_array == '').flatten()
    df_groups = pd.DataFrame(columns=['Start', 'End'])
    for i in range(len(blank_pos) - 1):
        df_groups = df_groups.append({'Start': blank_pos[i] + 1, 'End': blank_pos[i + 1]}, ignore_index=True)
    # print(df_groups)
    # _Domain_Category = xl_f_data['Domain_Category'].unique()
    # for catg in _Domain_Category:
    df_catg_data = xl_f_data#[xl_f_data['Domain_Category'] == catg]
    # print(df_catg_data.iloc[:,3:6])
    df_groups_final = df_groups[df_groups['Start'] != df_groups['End']]
    for index, row in df_groups_final.iterrows():
        # print(index,row['Start'],row['End'])
        if index == 0:
            df_fix = df_catg_data.iloc[:, row['Start']:row['End']]
            # print('df1',df_fix.head(5))
            _newdf = pd.DataFrame(columns=df_fix.columns)
            _newdf['PERIOD'] = ''
            _newdf['COST'] = 0
        else:
            df_temp = df_catg_data.iloc[:, row['Start']:row['End']]
                # df_cat_merge=pd.merge(df_fix,df_temp,left_index=True)
                #df_final_db = pd.concat([df_fix, df_temp], sort=False)
            for index_label, fix_rec in df_fix.iterrows():
                for _colindex, _colvalue in df_temp.loc[index_label, :].iteritems():
                        # for _colindex, _colvalue in tempdf[index_label].iteritems():
                    fix_rec['PERIOD'] = _colindex
                    fix_rec['COST'] = _colvalue
                    fix_rec['Created_dttm'] = today.strftime("%Y-%m-%d %H:%M:%S")
                    fix_rec['Created_by'] = created_by
                    fix_rec['Update_TO_STG'] = 0
                    fix_rec['FileName'] = _xlfilename
                    fix_rec['COST_DESC'] = ws
                    _newdf = _newdf.append(fix_rec)
    #_newdf.to_excel("T_TEMP_OPEX_Final.xlsx")
    try:
        prev_df = pd.read_sql_table(table_name=tn, con=engineTest)
        prev_table_found = 'Yes'
        print("Table found", prev_df.columns)
        # LOG.info('Table: {} found.The column will be compared with excel sheet.'.format(tn,ws))
    except Exception as e:
        print("Table Not found", e, Exception)
        # LOG.info('Table: {} not found so it will be created now.'.format(tn))
        prev_table_found = 'No'
        upload_to_db(_newdf, schemaTest, tn, 'replace', engineTest)
        # xl_data.to_sql(name=table_names, con=engine, index=False, if_exists='replace')
    if (prev_table_found == 'Yes'):
        compare_df_cols_n_update_db(_newdf, tn, prev_df)
    print('worksheet: {} took {} Seconds: to complete'.format(ws,(dt.datetime.now() - today).seconds))
    # LOG.info('worksheet: {} took {} Seconds: to complete'.format(ws, (dt.datetime.now() - today).seconds))
    ## end of dynamic insert
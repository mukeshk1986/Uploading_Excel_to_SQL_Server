#!/usr/bin/env python3
#qr_download_files.py
# -*- coding: utf-8 -*-
# (c) 2018 Nokia Proprietary - Nokia Internal Use

import argparse
import errno
import logging
import os
import sys
import time
import re
import pyodbc

from datetime import datetime

from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.file import File
import DB_Setting as qrs

PROG_DESCRIPTION = """
NAME: {}

DESCRIPTION: Download each file found in a SharePoint folder and after downloading the
             file, move the file on the SharePoint to an Archived folder on SharePoint.

PURPOSE: This script downloads all files found in a SharePoint folder to a local folder and
         as the files are downloaded, move successfully downloaded files to an Archived folder
         on SharePoint located in the folder above the source folder..

         Note that the source path is relative to SharePoint's base folder, for example:
         src = /sites/SageDS/Shared Documents/LOAs/Jul 2018
         Not including the '/sites/SageDS' in the path will result in failure to download
         files.
         The Archived folder and date folder will be created if they do not already exist.
         The date folder will be the same as that in the supplied src path.
         Given a src = /sites/SageDS/Shared Documents/LOAs/Jul 2018, the Archived folder
         will be /sites/SageDS/Shared Documents/LOAs/Archived/Jul 2018.
"""

#
# Set up logging to go to both a logfile and stdout
#
logging.basicConfig(format='%(asctime)s  %(levelname)-10s %(processName)s  %(name)s %(message)s',
                    filename=time.strftime("..\logs\DL-%Y-%m-%d.log") )
LOG_LEVEL = logging.INFO
LOG = logging.getLogger(__name__)
LOG.setLevel(LOG_LEVEL)
SH = logging.StreamHandler(sys.stdout)
SH.setLevel(LOG_LEVEL)
LOG.addHandler(SH)

#https://nokia.sharepoint.com/:f:/r/sites/AAQT/AA QTKPIs/Scorecards-Draft/2019/Working Files?csf=1
#
# Define string constants that can be overridden via command line arguments
#
DEFAULT_DEST_PATH = '.'		# Default file copy destination path is current directory
DEFAULT_SP_URL = 'https://nokia.sharepoint.com/sites/SageDS/'	# Default SharePoint URL
#DEFAULT_SP_URL = 'https://nokia.sharepoint.com/:f:/r/sites/AAQT/AA QTKPIs/Scorecards - Draft/2019/Working Files?csf=1'	# Default SharePoint URL
DEFAULT_SP_USERNAME = 'svcsage@nokia.onmicrosoft.com'		# Default username to access SharePoint
DEFAULT_FILE_PATTERN = 'all'

class SharePoint(object):
    def __init__(self, url, username, password, log):
        """
        SharePoint object initialization.
        :param url: SharePoint destination URL (See DEFAULT_SP_URL above for example)
        :param username: Username to access SharePoint (See DEFAULT_SP_USERNAME above for example)
        :param log: where to send log messages
        :type log: logging.Logger
        """
        self._url = url
        self._username = username
        self._password = password
        self.log = log

        #
        # Authenticate for this run
        #
        log.debug("SharePoint: Acquiring AuthenticationContext {} for {}".format(url, username))
        self._ctx_auth = AuthenticationContext(url=url)

        #
        # Looks like, at the time of this writing, the Office365-REST_Python-Client
        # library exception handling leaves a lot to be desired as their internal
        # method calls don't test the results of lower-level method calls in order to
        # percolate up problems they encounter.
        #
        # NOTE: Errors will actually go to the log file but not standard output for some
        #       unknown reason
        #
        log.debug("SharePoint: Acquiring token from {} for {}".format(url, username))
        self._ctx_auth.acquire_token_for_user(username=username, password=password)

        #
        # Kludgy test to see if a problem occurred
        # Admittedly this test will break if a different authentication provider is used
        #
        if self._ctx_auth.provider.token is None:
            _msg = "SharePoint: Unable to get SharePoint authorization. Error: {}".format(self._ctx_auth.get_last_error())
            log.critical(_msg)
            raise ValueError(_msg)

        log.debug("SharePoint: Authentication token obtained from {} for {}".format(url, username))

        #
        # Get cookies
        #
        log.debug("SharePoint: Getting cookies from {} for {}".format(url, username))
        self._ctx = ClientContext(self.url, self._ctx_auth)
        log.debug("SharePoint: Obtained ClientContext from {} for {}".format(url, username))

    @property
    def url(self):
        return self._url

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    def list_files(self, path):
        """
        Get a list of the filenames in passed folder.

        :param path: Path to the folder from which to get the list of files
        :type path: string
        :return: List of filenames found in the folder
        """
        self.log.debug("list_files: Get list of files for path: {}".format(path))

        #
        # Build and execute the folder query
        #
        folder = self._ctx.web.get_folder_by_server_relative_url(path)
        self._ctx.load(folder)
        self._ctx.execute_query()

        #
        # Build and execute the files query from the folder query results
        #
        files = folder.files
        self._ctx.load(files)
        self._ctx.execute_query()

        ##print ("files ", files)

        #
        # Build list of files found in the path
        #
        #_file_list = []
        _file_dict = {}
        for _f in files:
            self.log.debug("list_files: Filename: {}".format(_f.properties["Name"]))
            _file_dict[_f.properties["Name"]] = _f.properties["TimeCreated"]
        #
        # Return the list of files
        #
        ##print ("_file_dict = ", _file_dict)
        #return _file_list
        return _file_dict

    def check_archived_folder(self, src_path):
        """
        After downloading files, they're moved to an Archived folder on SharePoint.
        Check to make sure the Archiv folders exists or create them if they do not.
        The Archived folder path is comprised of two parts, "Archived" and "Month Year"
        folder as in Archived/Oct 2018 and is placed within the download folder path like:
        .../Shared Documents/NPT/Sep 2018           Download source path
        .../Shared Documents/NPT/Archived/Sep 2018  Archived path
        :param src_path: Download source base directory (ie. /sites/SageDS/Shared Documents/NPT/Oct 2018)
        """
        self.log.debug("check_archived_folders: src_path:{}".format(src_path))

        #
        # Create some useful variables from download source folder
        # Incoming src_path variable should be like: /sites/SageDS/Shared Documents/NPT/Sep 2018
        #
        _base_path = os.path.dirname(src_path)    # Grab the head of src_path (ie. /sites/SageDS/Shared Documents/NPT)
        _date_folder = os.path.basename(src_path) # Grab the base of src_path (ie. Sep 2018)

        self.log.debug("check_archived_folders: base_path:{} date_folder:{}".format(_base_path, _date_folder))

        #
        # Get the list of folders in our base folder
        #
        _folder = self._ctx.web.get_folder_by_server_relative_url(_base_path)
        _folders = _folder.folders
        self._ctx.load(_folders)
        self._ctx.execute_query()

        #
        # Look for Archived folder in base folder
        #
        _archived_found = False
        for _f in _folders:
            self.log.debug("check_archived_folders: is 'Archived' = '{}'".format(_f.properties["Name"]))
            if "Archived" == _f.properties["Name"]:
                self.log.debug("check_archived_folders: 'Archived' folder found, no need to create")
                _archived_found = True

        #
        # Did we find an Archived folder
        #
        if not _archived_found:
            #
            # Nope, need to add it
            #
            self.log.debug("check_archived_folders: Creating 'Archived' folder")
            _folders.add("Archived")
            self._ctx.execute_query()
            self.log.debug("check_archived_folders: Created 'Archived' folder")

        #
        # Now check for a date folder within the Archived folder
        #
        _base_path += "/Archived"
        _folder = self._ctx.web.get_folder_by_server_relative_url(_base_path)
        _folders = _folder.folders
        self._ctx.load(_folders)
        self._ctx.execute_query()

        #
        # Look for the date folder obtained from original path within the Archived folder
        #
        _date_folder_found = False
        for _f in _folders:
            self.log.debug("check_archived_folders: is '{}' = '{}'".format(_date_folder, _f.properties["Name"]))
            if _date_folder == _f.properties["Name"]:
                self.log.debug("check_archived_folders: '{}' folder found, no need to create".format(_date_folder))
                _date_folder_found = True

        #
        # Did we find a date folder within the Archived folder
        #
        if not _date_folder_found:
            #
            # Nope, need to create one
            #
            self.log.debug("check_archived_folders: Creating '{}' folder".format(_date_folder))
            _folders.add(_date_folder)
            self._ctx.execute_query()
            self.log.debug("check_archived_folders: '{}' folder created ".format(_date_folder))

    def download_files(self, src, dest, file_pattern, period, worksheet_names, table_names, exec_id, script_path):
        #print ("In download_files")
        """
        Download all the files in a src SharePoint directory to local dest directory.

        :param src: Source SharePoint folder from which to download files
        :type src: string
        :param dest: Destination local directory into which to download files
        :type dest: string
        :return: Number of files downloaded
        """
        #
        # Create some useful variables from download source folder
        # Incoming src variable should be like: /sites/SageDS/Shared Documents/NPT/Oct 2018
        #
        _base_path = os.path.dirname(src)    # Grab the head of dir (ie. /sites/SageDS/Shared Documents/NPT)
        #print("_base_path :", _base_path)
        _date_folder = os.path.basename(src) # Grab the base of dir (ie. Oct 2018)
        #print("_date_folder :", _date_folder)
        _move_to_path = "{}/Archived/{}/".format(_base_path, _date_folder)
        ##print("_move_to_path :", _move_to_path)

        self.log.debug("download_files: Starting src:{} base:{} date:{}".format(src, _base_path, _date_folder))

        #
        # Make sure Archived folder exists in SharePoint before moving on
        #
        self.check_archived_folder(src)

        #
        # Get the list of filenames in the SharePoint folder
        #
        _files = self.list_files(src)

        ##print ("_files :", _files)

        #
        # Keep a count of the number of files downloaded
        #
        _num_downloaded = 0

        #
        # Make sure there's something to download
        #
        if not len(_files):
            self.log.info("download_files: No files found to download from {} of {}".format(src, self.url))
            return _num_downloaded

        self.log.info("files_found: Number of files {} found from {} of {}".format(len(_files), src, self.url))

        #
        # Walk the list of files, downloading each one into destination directory
        #

        for _f, _fct in _files.items():
            #print ("In _files Loop")
            _spn = "{}/{}".format(src, _f) 	# Source pathname
            ##print ('_spn =', _spn)
            _dpn = os.path.join(dest, _f)	# Destination pathname
            ##print ('_dpn =', _dpn)

            _upper_file_name = _f.upper()
            ##print ('_upper_file_name =', _upper_file_name)
            # self.log.info('_upper_file_name = ' + _upper_file_name)
            _upper_file_pattern = file_pattern.upper()
            # self.log.info('_upper_file_pattern = ' + _upper_file_pattern)
            ##print ('_upper_file_pattern =', _upper_file_pattern)

            file_pattern_match = re.findall(_upper_file_pattern,_upper_file_name )
            # self.log.info('file_pattern_match = ' + str(file_pattern_match))
            if file_pattern_match:
                ##print ("Found files with the pattern {}".format(file_pattern))
                self.log.debug("download_files: Downloading {} to {}".format(_spn, _dpn))
                # self.log.info("download_files: Downloading {} to {}".format(_spn, _dpn))
                # Insert Log
                proc_name = _f
                statement_type = "Download File -> " + _f
                table_name = 'null'
                # success_failure = "Download Complete"
                # self.log.info('exec_id = ' + str(exec_id) + ' proc_name = ' + proc_name + ' statement_type = '
                #               + statement_type)
                qrs.insert_log(exec_id, proc_name, statement_type, table_name, 'null', "Started")

                #
                # Download the file
                #
                try:
                    with open(_dpn, 'wb') as _ofd:
                        _response = File.open_binary(self._ctx, _spn)
                        _ofd.write(_response.content)
                    # success_failure = "Download Complete"
                    qrs.insert_log(exec_id, proc_name, statement_type, table_name, 'null', "Completed")
                except Exception as e:
                    _msg = "{} error encountered creating destination file {}".format(e, _dpn)
                    log.critical(_msg)
                    raise ValueError(_msg)
                dct = datetime.fromtimestamp(os.path.getmtime(_dpn))
                ##print ("down load time : " ,dct)
                ##print ("type of _fct = ", type(_fct))
                #_fp.append(dct)
                ##print ("_fp = ", _fp)
                #valid_files_dict[_f] = _fp
                ##print ("valid_files_dict = ", valid_files_dict)

                sqlcmd = "INSERT INTO [T_STG_FILES_DOWNLOADED]" \
                         + "([FILE_NAME_PATTERN],[FILE_NAME],[DOWNLOADED_TIME],[PERIOD],[PROJECT],[FILE_CREATION_TIME]) " \
                         + "VALUES(" \
                         + "'" + file_pattern +  "'," \
                         +  "'" + _f +  "'," \
                         + "CAST(left('" + str(dct) + "',23)" + " AS datetime),'" \
                         + period +  "'," \
                         +"'" + qrs.Project_Name + "'," \
                         + "CAST ('" +_fct + "' AS datetime))"
                ##print('sqlcmd = ', sqlcmd)
                qrs.dml_cursor(sqlcmd)
                LOG.info("Uploaded to data base strarts:Work sheet {} Table: {} ".format(worksheet_names, table_names))
                os_cmd = script_path + 'Opex_dynamic_upload_to_DB.py ' + str(exec_id) + ' "' + _dpn + '" "' +  table_names + '" "' + worksheet_names + '"'
                print ('Initiating System Command -> \n' + os_cmd)
                qrs.run_os_cmd(os_cmd)
                LOG.info("All Work sheet {} Uploaded to Table: {} was successful".format(worksheet_names, table_names))
                #print ("file = " ,  "'" + dest + "\\" +_f + "'")
                #print ("worksheet_names = "  ,  "'" + worksheet_names+ "'")
                #print ("table_names = "  ,  "'" + table_names + "'")

                _num_downloaded += 1

                #
                # File downloaded, move it to Archived folder
                #
                _to_path = _move_to_path + _f #commented by Mukesh not archieve for testing
    
                self.log.debug("download_files: Moving {} to {}".format(_spn, _to_path))
                #
                # Moving the file
                #
                try:
                    print ("in moving block")
                    LOG.info("\n Sharepoint:File {} has been archived in {} ".format(self._ctx, _spn,_to_path))
                    _resp = File.move(self._ctx, _spn, _to_path) #commented by Mukesh not archieve for testing
                    ##print ("in moving block after MOve")
                except Exception as e:
                    _msg = "{} error encountered moving {} to  file {}".format(e, _spn, _to_path)
                    log.critical(_msg)
                    raise ValueError(_msg)
                #_resp = File.move(self._ctx, _spn, _to_path)
    
                #
                # Was move successful
                #
                if _resp.status_code != 200:
                    self.log.error("download_files: Error: {} moving {} to {}".format(_resp.status_code, _spn, _to_path))
                else:
                    self.log.debug("download_files: Moved {} to {}".format(_spn, _to_path))

        #
        # Return count of files downloaded
        #
        return _num_downloaded

def parse_commandline():
    """
    Parse command line arguments, filling in defaults.

    :return: vars of the arglist
    """
    #
    # Check for command line arguments, else just use defaults
    #
    _p = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                 description=PROG_DESCRIPTION.format(os.path.basename(sys.argv[0])))
    _p.add_argument('--log_level',
                    default=logging.getLevelName(LOG_LEVEL),
                    choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                    help="set log level (default: %(default)s)"
                    )
    _p.add_argument('--url',
#                    default=DEFAULT_SP_URL,
                    help=('base SharePoint URL '
                          '(default=%(default)s)'))
    _p.add_argument('--username',
#                    default=DEFAULT_SP_USERNAME,
                    help=('username to use to access SharePoint '
                          '(default=%(default)s)'))
    _p.add_argument('--password',
                    help=('password for the username to access SharePoint '))
    _p.add_argument('--src',
                    help='Required SharePoint folder path from which to download files ',
                    required=True)
    _p.add_argument('--dest',
                    default=DEFAULT_DEST_PATH,
                    help=('destination path into which to copy files (created if doesn\'t exist) '
                          '(default=%(default)s)'))
    _p.add_argument('--file_pattern',
                    default=DEFAULT_FILE_PATTERN,
                    help=('file pattern to be downloaded '
                          '(default=%(default)s)'))
    _p.add_argument('--worksheet_names',
                    help='worksheet_names',
                    required=True)
    _p.add_argument('--table_names',
                    help='table_names',
                    required=True)
    _p.add_argument('--period',
                    help='period ',
                    required=True)
    _p.add_argument('--exec_id',
                    help='exec_id ',
                    required=True)
    _p.add_argument('--script_path',
                    help='script_path ',
                    required=True)

    _vars = vars(_p.parse_args())

    #
    # Set log level in both the log file and the handler.
    #
    LOG.setLevel(getattr(logging, _vars['log_level']))
    SH.setLevel(getattr(logging, _vars['log_level']))
    LOG.debug("parse_commandline: Set log level to %s " % _vars['log_level'])

    return _vars

def test_variable(name, var, args):
    """
    Test to see if configuration file exists and has key defined.
    """
    try:
        qrs.settings
        try:
            qrs.settings[name]
            return qrs.settings[name]
        except KeyError:
            pass
    except NameError:
        pass

    if not args[name]:
        LOG.critical("{} argument requred but not supplied - exiting!".format(name))
        exit(1)
    else:
        return args[name]

def main():
    #
    # Process command line arguments
    #
    _args = parse_commandline()

    #
    # Verify SharePoint connectivity credentials supplied to script
    #
    _url = ""
    _url = test_variable("url", _url, _args)

    _username = ""
    _username = test_variable("username", _username, _args)

    _password = ""
    _password = test_variable("password", _password, _args)

    _start_time = datetime.now()
    LOG.info("main: Download SharePoint files from {} of {} started".format(_args['src'], _url))

    #
    # Create the SharePoint object
    #
    LOG.debug("main: Accessing SharePoint {} as user {}".format(_url, _username))

    try:
        _sharepoint = SharePoint(url=_url,
                                 username=_username,
                                 password=_password,
                                 log=LOG)
    except ValueError:
        exit(1)
    ##print ("after getting _sharepoint object")
    ##print ("type of _sharepoint", type(_sharepoint))
    #
    # See if destination directory exists, if not create it based on requirements
    #
    try:
        os.makedirs(_args['dest'], exist_ok=True)
    except OSError as e:
        if e.errno != errno.EEXIST:
            _msg = "main: Error: {} creating destination directory {} - exiting!".format(e, _args['dest'])
            log.critical(_msg)
            raise ValueError(_msg)
    ##print ("after os.makedirs(_args['dest']" ,_args['dest'])
    #
    # Download the files
    #
    try:
        #print ("Before calling _sharepoint.download_files")

        # parms = "_sharepoint.download_files parms " + "src= " + _args['src'] + " dest= " + _args['dest']\
        #         + " file_pattern = " + _args['file_pattern'] + " period= " + _args['period'] + ' ' +\
        #         _args['worksheet_names'] + " table_names= " + _args['table_names']\
        #         + " exec_id= " + _args['exec_id']
        #
        # LOG.info(parms)

        _num_downloaded = _sharepoint.download_files(_args['src'], _args['dest'], _args['file_pattern']
                                                     , _args['period'], _args['worksheet_names']
                                                     , _args['table_names'], _args['exec_id'], _args['script_path'])
    except Exception:
        exit(1)

    #
    # Done
    #
    LOG.info("main: Download of {} files from {} of {} to {} took {} seconds".format(_num_downloaded,
                                                                                     _args['src'],
                                                                                     _url,
                                                                                     _args['dest'],
                                                                                     (datetime.now() - _start_time)))

if __name__ == '__main__':
    main()


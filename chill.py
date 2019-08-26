""" chill.py:
 Description: relax and chill the library is fine

 Created by : Daniel Jaramillo
 Creation Date: 11/01/2019
 Modified by:     Date:
 All rights(C) reserved to Teoco
"""
import os
import sys
import argparse
import os_tools
import base64
import glob
import time
from HlxTools import HlxTools
from LoggerInit import LoggerInit
from Partrans import Partrans
from threading import Thread
from TestReport import TestReportJunit
from datetime import datetime
from ParseHLD import ParseHLD


def parse_args():
    """
    Parse input arguments
    """
    global conf_file
    app_logger=logger.get_logger("parse_args")
    parser = argparse.ArgumentParser()

    parser.add_argument('-c','--conf',
    	help='Configuration Json file',
    	required=True,
    	type=str)

    args=parser.parse_args()
    conf_file=args.conf

    if not os.path.exists(conf_file):
        app_logger.error('{conf_file} not found'.format(conf_file=conf_file))
        sys.exit(1)

def main():
    app_logger=logger.get_logger("main")
    global conf_file
    parse_args()

    DB_USER=os.environ['DB_USER']
    DB_PASSWORD=base64.b64decode(os.environ['DB_PASSWORD'])
    ORACLE_SID=os.environ['ORACLE_SID']
    DB_HOST=os.environ['DB_HOST']

    partrans=Partrans(conf_file,DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST)    
    #Parse hld
    parse_hld=ParseHLD(partrans.configuration['HLD'])
    parse_hld.load_hld()
    #Parse and transform data
    partrans.parse_data(parse_hld)

    #Simulate Data using helix
    LIBRARY_NAME=partrans.configuration['library']
    MASK=partrans.configuration['input_rd_mask']
    LOCAL_DIR=partrans.configuration['input_rd']
    INSTANCE_ID="1717"
    curr_datetime=datetime.now().strftime("%Y_%m_%d_%H_%M")


    TEST_REPORT_FILE_JUNIT="test_reports/{LIBRARY_NAME}_test_report.xml"\
        .format(curr_datetime=curr_datetime,LIBRARY_NAME=LIBRARY_NAME)

    #Validate environment variables
    if 'DVX2_IMP_DIR' not in os.environ:
        app_logger.error('DVX2_IMP_DIR env variable not defined')
        sys.exit(1)
    DVX2_IMP_DIR=os.environ['DVX2_IMP_DIR']
    if 'DVX2_LOG_DIR' not in os.environ:
        app_logger.error('DVX2_LOG_DIR env variable not defined')
        sys.exit(1)
    DVX2_LOG_DIR=os.environ['DVX2_LOG_DIR']
    DVX2_LOG_FILE=os.path.join(DVX2_LOG_DIR,\
        "dvx2_{LIBRARY_NAME}_{INSTANCE_ID}.log"\
        .format(LIBRARY_NAME=LIBRARY_NAME,INSTANCE_ID=INSTANCE_ID))
    #Make log file empty
    open(DVX2_LOG_FILE, 'w').close()
    #Validate if Library exists
    connect_file=os.path.join(DVX2_IMP_DIR,'scripts',LIBRARY_NAME+'.connect')
    if not connect_file:
        app_logger.error('Library {LIBRARY_NAME} does not exist'\
            .format(LIBRARY_NAME=LIBRARY_NAME))
        sys.exit(1)
    #Validate raw data files
    if not os.path.isdir(LOCAL_DIR):
        app_logger.error('Input dir {LOCAL_DIR} does not exist'\
            .format(LOCAL_DIR=LOCAL_DIR))
        sys.exit(1)
    if len(glob.glob(os.path.join(LOCAL_DIR,MASK))) ==0:
        app_logger.error('No raw data files available in {LOCAL_DIR}'\
            .format(LOCAL_DIR=LOCAL_DIR))
        sys.exit(1)

    #Create HlxTools object
    hlxtools=HlxTools(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST,INSTANCE_ID,MASK,
        LOCAL_DIR)

    #Kill connect
    app_logger.info('Stopping connect file')
    os_tools.kill_process('connect','{LIBRARY_NAME}_{INSTANCE_ID}'\
        .format(LIBRARY_NAME=LIBRARY_NAME,
            INSTANCE_ID=INSTANCE_ID))


    #Parse DBL file
    hlxtools.parse_dbl(DVX2_IMP_DIR,connect_file)
    hlxtools.activate_dbl_oracle(DVX2_IMP_DIR)

    # #REMOVE
    # #Get the DBL errors after running the simulation
    # error_list=hlxtools.parse_dbl_error_files()
    # #Get data loaded into the tables
    # oracle_cfg_data=hlxtools.load_cfg_data_oracle(
    #     partrans.configuration["schema"])
    # oracle_data=hlxtools.load_data_oracle(partrans.transformed_data,
    #     partrans.configuration["schema"])
    # #Compare the data
    # data_report=hlxtools.compare_data(partrans,
    #     oracle_data,
    #     parse_hld,
    # oracle_cfg_data)
    # report={}
    # report['data_report']=data_report
    # report['error_list']=error_list
    # report['table_list']=list(parse_hld.metadata['Tables'].dropna(how='all')
    #     ['Table Name'])
    # #Generate the report
    # testreportjunit=TestReportJunit(report)
    # testreportjunit.create_db_errors()
    # testreportjunit.create_data_summary()
    # testreportjunit.create_data_missing()
    # testreportjunit.create_data_diffs()
    # testreportjunit.create_missing_cols()
    # testreportjunit.save_file(TEST_REPORT_FILE_JUNIT)
    # sys.exit(0)
    # #REMOVE

    #Create GD Access
    access_id=hlxtools.create_access(LIBRARY_NAME)
    if not access_id:
        app_logger.error('Access could not be created')
        sys.exit(1)

    #Delete the data in the tables
    hlxtools.delete_data(partrans.transformed_data)

    #Remove dbl temp Files
    hlxtools.remove_dbl_files()

    #Run connect
    worker = Thread(target=hlxtools.run_connect, args=(access_id,
        LIBRARY_NAME))
    worker.setDaemon(True)
    worker.start()
    #Wait for connect to come up
    hlxtools.wait_connect()
    
    #restore Dbl ActiveStatus
    hlxtools.restore_dbl_oracle(DVX2_IMP_DIR)

    #Copy rd files to input folder
    hlxtools.copy_rd()

    #Wait for raw data to be processed
    hlxtools.wait_rd()
    app_logger.info('sleeping 180 seconds')
    time.sleep(180)

    #Wait for bcp files to be processed
    hlxtools.wait_bcp()

    #Schedule CfgTables
    hlxtools.schedule_cf_tables(partrans.configuration["schema"])
    #Run CfgTables
    hlxtools.run_cf_tables(partrans.configuration["schema"])
    app_logger.info('sleeping 60 seconds')
    time.sleep(60)
    #Schedule Aggregations
    hlxtools.schedule_aggr(partrans.configuration["schema"])
    #Run Aggregations
    hlxtools.run_aggr(partrans.configuration["schema"])
    app_logger.info('sleeping 60 seconds')
    time.sleep(60)
    #Update Thinout
    # hlxtools.update_thinout(partrans.configuration["schema"])

    #Get the DBL errors after running the simulation
    error_list=hlxtools.parse_dbl_error_files()
    #Get data loaded into the tables
    oracle_data=hlxtools.load_data_oracle(partrans.transformed_data,
        partrans.configuration["schema"])
    #Get data loaded into the cfg tables
    oracle_cfg_data=hlxtools.load_cfg_data_oracle(
        partrans.configuration["schema"])
    #Compare the data
    data_report=hlxtools.compare_data(partrans,
        oracle_data,
        parse_hld,
        oracle_cfg_data)
    report={}
    report['data_report']=data_report
    report['error_list']=error_list
    report['table_list']=list(parse_hld.metadata['Tables'].dropna(how='all')
        ['Table Name'])
    #Generate the report
    testreportjunit=TestReportJunit(report)
    testreportjunit.create_db_errors()
    testreportjunit.create_data_summary()
    testreportjunit.create_data_missing()
    testreportjunit.create_data_diffs()
    testreportjunit.create_missing_cols()
    testreportjunit.save_file(TEST_REPORT_FILE_JUNIT)

    #Kill connect
    app_logger.info('Stopping connect file')
    os_tools.kill_process('connect','{LIBRARY_NAME}_{INSTANCE_ID}'\
        .format(LIBRARY_NAME=LIBRARY_NAME,
            INSTANCE_ID=INSTANCE_ID))

if __name__ == "__main__":
    conf_file=""
    logger=LoggerInit()
    main()

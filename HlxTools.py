""" HlxTools.py:

 Description: variuos n2 commands

 Created by : Daniel Jaramillo
 Creation Date: 11/01/2019
 Modified by:     Date:
 All rights(C) reserved to Teoco
"""
import os
import sys
import glob
import time
import shutil
import os_tools
import cx_Oracle
import pandas as pd
import datetime
import sh
from ManagedDbConnection import ManagedDbConnection
from LoggerInit import LoggerInit


class HlxTools:

    def __init__(self,DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST,INSTANCE_ID,MASK,
            LOCAL_DIR):
        self.DB_USER=DB_USER
        self.DB_PASSWORD=DB_PASSWORD
        self.ORACLE_SID=ORACLE_SID
        self.DB_HOST=DB_HOST
        self.app_logger=LoggerInit().get_logger("HlxTools")
        self.INSTANCE_ID=INSTANCE_ID
        self.MASK=MASK
        self.LOCAL_DIR=LOCAL_DIR
        self.GD_NAME="GD_MEDIATION"
        self.TMP_DIR=os.path.join(os.getcwd(),'tmp')
        if not os.path.exists(self.TMP_DIR):
            os.makedirs(self.TMP_DIR)
        self.CYCLE_INTERVAL=5
        self.NE_NAME='Mediation_Server'
        self.dbl_file_list=set()
        self.batchevery=0
        self.work_dir_list=set()
        self.error_dir_list=set()
        self.failed_dir_list=set()
        self.table_list_oracle=set()
        self.table_list_hadoop=set()


    def create_access(self,LIBRARY_NAME):
        """
        Creates a GD access to process Local to GD Files
        """
        sqlplus_script="""
            connect {DB_USER}/{DB_PASSWORD}@{ORACLE_SID}
            begin
                comm_db.PA_PROJ_MED.SP_INSERT_ACC_L2G(
                    IN_SUBNET_NAME=>'{IN_SUBNET_NAME}',
                    IN_ACCESS_NAME=>'{IN_ACCESS_NAME}',
                    IN_GD_NAME=>'{IN_GD_NAME}',
                    IN_LOCAL_DIR=>'{IN_LOCAL_DIR}',
                    IN_CYCLE_INTERVAL=>'{IN_CYCLE_INTERVAL}',
                    IN_MASK=>'{IN_MASK}',
                    IN_ADVAMCED_MASK=>'{IN_ADVAMCED_MASK}',
                    IN_AGING_FILTER=>'{IN_AGING_FILTER}',
                    IN_SORT_ORDER=>'{IN_SORT_ORDER}',
                    IN_SOURCE_FILE_FINISH_POLICY=>'{IN_SOURCE_FILE_FINISH_POLICY}',
                    IN_SOURCE_SUFFIX_PREFIX=>'{IN_SOURCE_SUFFIX_PREFIX}',
                    IN_LOOK_IN_SUBFOLDERS=>'{IN_LOOK_IN_SUBFOLDERS}',
                    IN_SUB_FOLDERS_MASK=>'{IN_SUB_FOLDERS_MASK}',
                    IN_POST_SCRIPT=>'{IN_POST_SCRIPT}',
                    IN_SHOULD_RETRANSFER=>'{IN_SHOULD_RETRANSFER}',
                    IN_RETRANFER_OFFSET=>'{IN_RETRANFER_OFFSET}',
                    IN_ENABLEFILEMONITOR=>'{IN_ENABLEFILEMONITOR}',
                    IN_NE_NAME=>'{IN_NE_NAME}'
                );
            end;
            /
        """.format(
            DB_USER=self.DB_USER,
            DB_PASSWORD=self.DB_PASSWORD,
            ORACLE_SID=self.ORACLE_SID,
            IN_SUBNET_NAME=LIBRARY_NAME,
            IN_ACCESS_NAME=LIBRARY_NAME,
            IN_GD_NAME=self.GD_NAME,
            IN_LOCAL_DIR=self.LOCAL_DIR+"/in_sim/",
            IN_CYCLE_INTERVAL=self.CYCLE_INTERVAL,
            IN_MASK=self.MASK,
            IN_ADVAMCED_MASK="",
            IN_AGING_FILTER="NEWEST:1m",
            IN_SORT_ORDER="NoSort",
            IN_SOURCE_FILE_FINISH_POLICY="Delete",
            IN_SOURCE_SUFFIX_PREFIX="",
            IN_LOOK_IN_SUBFOLDERS="",
            IN_SUB_FOLDERS_MASK="",
            IN_POST_SCRIPT="",
            IN_SHOULD_RETRANSFER="Always",
            IN_RETRANFER_OFFSET="AllFile",
            IN_ENABLEFILEMONITOR=0,
            IN_NE_NAME=self.NE_NAME
        )
        sqlplus_output = os_tools.run_sqlplus(sqlplus_script)
        #Create input and done folders
        if not os.path.exists(self.LOCAL_DIR+"/in_sim/"):
            os.makedirs(self.LOCAL_DIR+"/in_sim/")

        with ManagedDbConnection(self.DB_USER,
                self.DB_PASSWORD,self.ORACLE_SID,self.DB_HOST) as db:
            cursor=db.cursor()
            sqlplus_script="""
                select ACCESS_NUM
                from comm_db.med_access where access_name='{LIBRARY_NAME}'
            """.format(LIBRARY_NAME=LIBRARY_NAME)
            try:
                cursor.execute(sqlplus_script)
                access_id=''
                for row in filter(None,cursor):
                    self.app_logger.info('access id {access_id} was created'\
                        .format(access_id=row[0]))
                    access_id=row[0]
            except cx_Oracle.DatabaseError as e:
                self.app_logger.error(e)
                self.app_logger.error(sqlplus_script.replace('\n',' '))
                sys.exit(1)
        self.app_logger.info('Refreshing {GD_NAME} process'\
            .format(GD_NAME=self.GD_NAME))
        pid=os_tools.kill_process('GD_Name',self.GD_NAME)
        if pid !=0:
            self.app_logger.error('{process_name} is not running'\
                .format(process_name=process_name))
            sys.exit(1)
        self.app_logger.info('sleeping 20 seconds')
        time.sleep(20)
        pid=os_tools.check_running('GD_Name',self.GD_NAME)
        if not pid:
            self.app_logger.error('{process_name} is not running'\
                .format(process_name=process_name))
            sys.exit(1)
        return access_id

    def run_connect(self,access_id,LIBRARY_NAME):
        """
        Run a library connect script
        """
        #(define GDSubscription "Notification -Protocol File-Transfer -NeTypeName Mediation_Server -AType 10 -Subnet 42756 -NeNum 3150611 -Access 42284")\
        self.connect_log=os.path.join(self.TMP_DIR,'{LIBRARY_NAME}.connect.log'\
            .format(LIBRARY_NAME=LIBRARY_NAME))
        args=['connect',
                '-daemon',
                '{LIBRARY_NAME}.connect'.format(LIBRARY_NAME=LIBRARY_NAME),
                '-expr',
                '\'(load "n2_std.connect")\
                (load "n2_logger.connect")\
                (add-log-module "connect" (get-env-else "N2_LOG_DIR" ".")\
                    "conductor_{LIBRARY_NAME}_{INSTANCE_ID}")\
                (export "conductor_{LIBRARY_NAME}_{INSTANCE_ID}" self)\
                (define conductor-instance-id {INSTANCE_ID})\
                (define library-instance-name "{LIBRARY_NAME}")\
                (define NI_DIR "/tmp/")\
                (define dvx2-log-location (get-env "DVX2_LOG_DIR") )\
                (define dvx2-log-prefix "dvx2_")\
                (define GDSubscription "Notification -Protocol File-Transfer\
                    -Access {access_id}")\
                (define DeactivateAP 0)\
                (define NI_DIR "/tmp")\'\
                 > {connect_log} 2>&1'.format(connect_log=self.connect_log,
                    INSTANCE_ID=self.INSTANCE_ID,
                    access_id=access_id,
                    LIBRARY_NAME=LIBRARY_NAME)
                ]
        self.app_logger.info('Running {LIBRARY_NAME}.connect'\
            .format(LIBRARY_NAME=LIBRARY_NAME))
        os.system(' '.join(args))

    def activate_dbl_oracle(self,DVX2_IMP_DIR):
        """
        Remove ActiveStatus=NOBCP from the DBLoader files
        """
        for dbl_file in self.dbl_file_list:
            bdbl_file=os.path.join(DVX2_IMP_DIR,'config','Dbl',
                "Chill_"+dbl_file)
            dbl_file=os.path.join(DVX2_IMP_DIR,'config','Dbl',dbl_file)
            data=[]
            #Ignore impala dbl files
            if "impala" in dbl_file:
                continue
            with open(dbl_file,'r') as file:
                #save a copy of the dbl file
                shutil.copy(dbl_file,bdbl_file)
                filedata=file.read().split('\n')
                for line in filedata:
                    if "ActiveStatus=NOBCP" not in line:
                        data.append(line)
            with open(dbl_file,'w') as file:
                for line in data:
                    file.write(line+'\n')

    def restore_dbl_oracle(self,DVX2_IMP_DIR):
        """
        restore dbl backup
        """
        for dbl_file in self.dbl_file_list:
            bdbl_file=os.path.join(DVX2_IMP_DIR,'config','Dbl',
                "Chill_"+dbl_file)
            dbl_file=os.path.join(DVX2_IMP_DIR,'config','Dbl',dbl_file)
            #Ignore impala dbl files
            if "impala" in dbl_file:
                continue
            shutil.copy(bdbl_file,dbl_file)


    def parse_dbl(self,DVX2_IMP_DIR,connect_file):
        """
        Get table list and batchevery time from dbl fiile
        """
        self.app_logger.info('Parsing {connect_file}'\
            .format(connect_file=connect_file))        
        with open(connect_file) as file:
            filedata=file.read().split('\n')
            for line in filedata:
                if ".dbl" in line:
                    self.dbl_file_list.add(line.split('"')[1])

        for dbl_file in self.dbl_file_list:
            dbl_file=os.path.join(DVX2_IMP_DIR,'config','Dbl',dbl_file)
            self.app_logger.info('Parsing {dbl_file}'.format(dbl_file=dbl_file))
            with open(dbl_file,'r') as file:
                filedata=file.read().split('\n')
                for line in filedata:
                    if "DBProfile" in line:
                        profile=line.split("=")[1]
                    elif "TargetTable" in line and 'impala' in dbl_file:
                        self.table_list_hadoop.add(profile+'.'+line.split("=")[1])
                    elif "TargetTable" in line :
                        self.table_list_oracle.add(profile+'.'+line.split("=")[1])
                    elif "BatchEvery" in line:
                        self.batchevery=max(self.batchevery,line.split("=")[1])
                    elif "WorkDir" in line:
                        self.work_dir_list.add(os.path.expandvars(
                            line.split('=')[1].replace('$/','/')))
                    elif "ErrorDir" in line:
                        self.error_dir_list.add(os.path.expandvars(
                            line.split('=')[1].replace('$/','/')))
                    elif "FailedDir" in line:
                        self.failed_dir_list.add(os.path.expandvars(
                            line.split('=')[1].replace('$/','/')))

    def remove_dbl_files(self):
        """
        Remove error and work files from Db loader
        """
        for dir in list(self.work_dir_list)\
            +list(self.error_dir_list)\
            +list(self.failed_dir_list):
            self.app_logger.info('Removing files from {dir}'\
                .format(dir=dir))
            filelist=glob.glob(dir+"/*_{INSTANCE_ID}_*"\
                .format(INSTANCE_ID=self.INSTANCE_ID))
            for filename in filelist:
                try:
                    os.remove(filename)
                except OSError:
                    pass


    def copy_rd(self):
        """
        Copy raw data files to the input folder
        """
        target_dir=os.path.join(self.LOCAL_DIR,'in_sim')
        self.app_logger.info('Copying rd files to {target_dir}'\
            .format(target_dir=target_dir))
        rd_file_list=glob.glob(os.path.join(self.LOCAL_DIR,self.MASK))
        for file in rd_file_list:
            shutil.copy(file,target_dir)

    def wait_rd(self):
        """
        Wait for raw data to be processed
        """
        target_dir=os.path.join(self.LOCAL_DIR,'in_sim')
        while True:
            rd_files=glob.glob(os.path.join(target_dir,self.MASK))
            if len(rd_files) == 0:
                break
            self.app_logger.info('{rd_files} raw data files on queue'\
                .format(rd_files=len(rd_files)))
            time.sleep(10)
        self.app_logger.info('sleeping 30 seconds')
        time.sleep(30)

    def wait_bcp(self):
        """
        Wait for bcp files to be processed
        """
        while True:
            bcp_files=[]
            found=False
            for dir in self.work_dir_list:
                bcp_files=glob.glob(dir+"/*_{INSTANCE_ID}_*"\
                    .format(INSTANCE_ID=self.INSTANCE_ID))
                if len(bcp_files) > 0:
                    found=True
                    self.app_logger.info('{bcp_files} bcp files on queue on \
                        {dir}'.format(
                            bcp_files=len(bcp_files),
                            dir=dir))
            if not found:
                break
            time.sleep(10)
        self.app_logger.info('sleeping 60 seconds')
        time.sleep(60)

    def parse_dbl_error_files(self):
        """
        Parse Dbl error files
        """
        error_list={}
        file_list=[]
        #Get the list of error files in all folders
        for dir in self.error_dir_list:
            file_list.extend(glob.glob(dir+"/*_{INSTANCE_ID}_*.log"\
                .format(INSTANCE_ID=self.INSTANCE_ID)))
        #Parse all log files
        for filename in file_list:
            filename_arr=[set(),[]]
            with open(filename,'r') as file:
                filedata=file.read().split('\n')
                for line in filedata:
                    #Table name found
                    if line.startswith('Table '):
                        table_name='_'.join(line.split(',')[0].split(' ')[1]\
                            .split('.')[1].split('_')[:-1])
                        if table_name not in error_list:
                            error_list[table_name]={}
                    #Error found
                    elif line.startswith('ORA-'):
                        #Oracle Error found
                        filename_arr[0].add(line)
                    elif line.startswith('Record '):
                        #Oracle Error found
                        filename_arr[0].add(line.split(':')[1])
                    #Statistics found
                    elif 'Rows' in line:
                        #Adding the summary of data loaded
                        filename_arr[1].append(line)
                if table_name in error_list:
                    error_list[table_name][filename]=filename_arr
        return error_list


    def wait_connect(self):
        """
        Wait for connect to come up
        """
        do_loop=True
        while do_loop:
            self.app_logger.info("Waiting for connect to come up")
            time.sleep(20)
            with open(self.connect_log) as file:
                filedata=file.read().split('\n')
                for idx,line in enumerate(filedata):
                    if "LogicError" in line:
                        self.app_logger.error('\n'.join(filedata[idx-5:]))
                        sys.exit(1)
                    elif "Subcribed to" in line:
                        do_loop=False
                        break


    def delete_data(self,transformed_data):
        """
        Delete data in target tables for datetime found in raw data files
        """
        self.app_logger.info("Deleting data from target tables")
        with ManagedDbConnection(self.DB_USER,
                self.DB_PASSWORD,self.ORACLE_SID,self.DB_HOST) as db:
            cursor=db.cursor()
            for table,data in transformed_data.items():
                for _datetime in data['datetime_list']:
                    sqlplus_script="""
                        delete from {table}
                        where
                        datetime=to_date('{_datetime}','YYYY-MM-DD HH24:MI:SS')
                    """.format(table=table,
                        _datetime=_datetime
                    )
                    try:
                        cursor.execute(sqlplus_script)
                        db.commit()
                    except cx_Oracle.DatabaseError as e:
                        self.app_logger.error(e)
                        self.app_logger.error(sqlplus_script.replace('\n',' '))

    def load_data_oracle(self,transformed_data,schema):
        """
        Queries the data from the dbl tables for the datetime list
        """
        oracle_data={}
        cfg_tables={}
        self.app_logger.info("Getting data loaded to Oracle")
        with ManagedDbConnection(self.DB_USER,
                self.DB_PASSWORD,self.ORACLE_SID,self.DB_HOST) as db:
            cursor=db.cursor()
            #Get the list of cfg tables
            sqlplus_script="""
                select distinct a.MT_TABLE_PREFIX,a.MT_TABLE_TYPE, b.CONF_DB_NAME, b.CONF_TABLE_NAME
                from PMMCONF_DB.PMM_OM_MASTER_TABLE a,PMMCONF_DB.PMM_ENT_CONF b 
                where a.MT_DBNAME='{schema}'
                and a.MT_TABLE_TYPE=b.ENTITY_NAME
            """.format(schema=schema)
            try:
                cursor.execute(sqlplus_script)
                for row in filter(None,cursor):
                    cfg_tables[schema+'.'+row[0]]=row[2]+'.'+row[3]
            except cx_Oracle.DatabaseError as e:
                self.app_logger.error(e)
                self.app_logger.error(sqlplus_script.replace('\n',' '))

            for table,data in transformed_data.items():
                for _datetime in data['datetime_list']:
                    sqlplus_script="""
                        select * from {table}
                        where
                        datetime=to_date('{_datetime}','YYYY-MM-DD HH24:MI:SS')
                    """.format(table=table,
                        _datetime=_datetime
                    )
                    try:
                        cursor.execute(sqlplus_script)
                        #Create Data frame with the results
                        names = [ x[0] for x in cursor.description]
                        rows = cursor.fetchall()
                        table_df=pd.DataFrame( rows, columns=names)
                        #Add dataframe to the data dict
                        if table not in oracle_data:
                            oracle_data[table]={'data':table_df}
                        else:
                            frames=[oracle_data[table]['data'],table_df]
                            oracle_data[table]['data']=pd.concat(frames)
                        oracle_data[table]['datetime_list']=\
                            set(oracle_data[table]['data']['DATETIME'])
                        stable='_'.join(table.split('_')[:-1])
                        oracle_data[table]['cfg_table']=cfg_tables[stable]

                    except cx_Oracle.DatabaseError as e:
                        self.app_logger.error(e)
                        self.app_logger.error(sqlplus_script.replace('\n',' '))
        return oracle_data


    def load_cfg_data_oracle(self,schema):
        """
        Queries all the cfg data for the entity tables for the schema
        """
        cf_data={}
        cfg_table_list=[]
        self.app_logger.info("Getting data loaded to Oracle cfg tables")
        with ManagedDbConnection(self.DB_USER,
                self.DB_PASSWORD,self.ORACLE_SID,self.DB_HOST) as db:
            cursor=db.cursor()
            sqlplus_script="""
                select distinct b.CONF_DB_NAME, b.CONF_TABLE_NAME, b.CONF_KEY_FIELDS
                from PMMCONF_DB.PMM_OM_MASTER_TABLE a,PMMCONF_DB.PMM_ENT_CONF b 
                where a.MT_DBNAME='{schema}'
                and a.MT_TABLE_TYPE=b.ENTITY_NAME
            """.format(schema=schema)
            try:
                cursor.execute(sqlplus_script)
                for row in filter(None,cursor):
                    cfg_table_list.append((row[0]+'.'+row[1],row[2]))
            except cx_Oracle.DatabaseError as e:
                self.app_logger.error(e)
                self.app_logger.error(sqlplus_script.replace('\n',' '))
            for cfg_table in cfg_table_list:
                table=cfg_table[0]
                keys=cfg_table[1]
                sqlplus_script="""
                    select {keys}
                    from {table}                    
                """.format(table=table,keys=keys)
                try:
                    cursor.execute(sqlplus_script)
                    #Create Data frame with the results
                    names = [ x[0] for x in cursor.description]
                    rows = cursor.fetchall()
                    table_df=pd.DataFrame( rows, columns=names)
                    #Add dataframe to the data dict
                    if table not in cf_data:
                        cf_data[table]=table_df
                    else:
                        frames=[cf_data[table],table_df]
                        cf_data[table]=pd.concat(frames)

                except cx_Oracle.DatabaseError as e:
                    self.app_logger.error(e)
                    self.app_logger.error(sqlplus_script.replace('\n',' '))
        return cf_data



    def get_table_index(self,table_name,schema):
        """
        returns a list with the index fields for the given table
        """
        with ManagedDbConnection(self.DB_USER,
                self.DB_PASSWORD,self.ORACLE_SID,self.DB_HOST) as db:
            cursor=db.cursor()
            sqlplus_script="""
                SELECT DISTINCT A.CONF_KEY_FIELDS
                FROM PMMCONF_DB.PMM_ENT_CONF A, PMMCONF_DB.PMM_OM_MASTER_TABLE B
                WHERE A.ENTITY_NAME=B.MT_TABLE_TYPE
                AND B.MT_TABLE_PREFIX='{table_name}'
                AND B.MT_DBNAME='{schema}'
            """.format(table_name=table_name,schema=schema)
            try:
                cursor.execute(sqlplus_script)
                #Create Data frame with the results
                return [x[0] for x in cursor.fetchall()][0].split(',')
            except cx_Oracle.DatabaseError as e:
                self.app_logger.error(e)
                self.app_logger.error(sqlplus_script.replace('\n',' '))

    def convert_dt_str(self,ilist):
        """
        Loops the given list and converts the Timestamp to string
        """
        for i,ilist_field in enumerate(ilist):
            if type(ilist_field) is pd.Timestamp:
                ilist[i]=ilist[i].strftime('%Y-%m-%d %H:%M')
        return ilist

    def schedule_cf_tables(self,schema):
        """
        Schedule all the n2 tasks like cfgtables
        """
        self.app_logger.info("Scheduling CfgTable tasks")
        with ManagedDbConnection(self.DB_USER,
                self.DB_PASSWORD,self.ORACLE_SID,self.DB_HOST) as db:
            cursor=db.cursor()
            entity_list=[]
            sqlplus_script="""
                select ENTITY_NAME, RULE_ID 
                from PMMCONF_DB.PMM_ENT_CONF 
                where CONF_DB_NAME = '{schema}'
            """.format(schema=schema)
            try:
                cursor.execute(sqlplus_script)
                for row in filter(None,cursor):
                    entity_list.append((row[0],row[1]))
            except cx_Oracle.DatabaseError as e:
                self.app_logger.error(e)
                self.app_logger.error(sqlplus_script.replace('\n',' '))
                sys.exit(1)
        start_date=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:00")
        for entity in entity_list:
            stdin = ["gs pms pms_connect", 
                "\n", 
                "ex pms (define s (PMMManagingService))", 
                "\n", 
                "ex pms (define args '("+'"'+"CfgTable"+'"'+" {RULE_ID}'\
                 ( ".format(RULE_ID=entity[1])+'"'+"ScheduleStartDate"+'"'+" "+'"'\
                 +"{start_date}".format(start_date=start_date)\
                 +'"'+") '("+'"'+"ScheduleUnits"+'"'+" "+'"'+"Hours"+'"'+") '\
                  ( "+'"'+"ScheduleNumUnits"+'"'+" 12 ) \
                  '("+'"'+"ScheduleOffset"+'"'+" 0)))",
                "\n",
                "ex pms (s "+'"'+"ScheduleRule"+'"'+" args)",
                "\n"
                ]
            sh.conqt(_in=stdin)


    def run_aggr(self,schema):
        """
        Run all the Aggregations for the given schema
        """
        self.app_logger.info("Running TRAggr tasks")
        with ManagedDbConnection(self.DB_USER,
                self.DB_PASSWORD,self.ORACLE_SID,self.DB_HOST) as db:
            cursor=db.cursor()
            command_list=[]
            #Run Aggregations
            sqlplus_script="""
                select 'ex s ScheduleTask ' || TASK 
                from config_db.n2_scheduler_task 
                where task in
                (select mt_task_id 
                    from pmmconf_db.pmm_om_master_table 
                    where mt_dbname ='{schema}')
            """.format(schema=schema)
            try:
                cursor.execute(sqlplus_script)
                for row in filter(None,cursor):
                    command_list.append(row[0])
            except cx_Oracle.DatabaseError as e:
                self.app_logger.error(e)
                self.app_logger.error(sqlplus_script.replace('\n',' '))
                sys.exit(1)

            for command in command_list:
                stdin = ["gs s the-scheduler", 
                    "\n", 
                    command, 
                    "\n"
                    ]
                sh.conqt(_in=stdin)

    def update_thinout(self,schema):
        """
        Update the thinout for the schema
        """
        self.app_logger.info("Running Thinout updates")
        with ManagedDbConnection(self.DB_USER,
                self.DB_PASSWORD,self.ORACLE_SID,self.DB_HOST) as db:
            cursor=db.cursor()
            command_list=[]
            #Run Aggregations
            sqlplus_script="""
                select 'ex s ' || CASE WHEN TN.DATABASE_NAME IS NULL THEN 'Create' ELSE 'Update' END || 'Rule|ThinOut|' || MT.MT_TABLE_NAME || '|' || MT.MT_DBNAME || '|' || MT.MT_DBNAME || '|' || 'DATETIME|' || TO_CHAR(SYSTIMESTAMP+3/24,'YYYY-MM-DD HH24:MI:SS') || '.000|' || 'Suspended|' || 'Hours|12|' || 
                CASE
                WHEN MT.MT_SUFFIX = '5M' THEN 'Days|30|'
                WHEN MT.MT_SUFFIX = '15M' THEN 'Days|30|'
                WHEN MT.MT_SUFFIX = 'HH' THEN 'Days|30|'
                WHEN MT.MT_SUFFIX = 'HR' THEN 'Days|90|'
                WHEN MT.MT_SUFFIX = 'DY' THEN 'Days|3650|'
                WHEN MT.MT_SUFFIX = 'WK' THEN 'Days|3600|'
                WHEN MT.MT_SUFFIX = 'MO' THEN 'Years|3|'
                WHEN MT.MT_SUFFIX = 'YR' THEN 'Years|3|'
                END || '-1|-1|-1|-1|-1||ORACLE' 
                FROM PMMCONF_DB.PMM_OM_MASTER_TABLE MT 
                LEFT JOIN CONFIG_DB.N2_THINOUT_RULE_BACKUP TN 
                ON MT.MT_DBNAME = TN.DATABASE_NAME 
                AND MT.MT_TABLE_NAME = TN.TABLE_NAME 
                LEFT JOIN ALL_TABLES AL ON MT.MT_DBNAME = AL.OWNER 
                AND MT.MT_TABLE_NAME = AL.TABLE_NAME 
                WHERE AL.OWNER = '{schema}'
            """.format(schema=schema)
            try:
                cursor.execute(sqlplus_script)
                for row in filter(None,cursor):
                    command_list.append(row[0])
            except cx_Oracle.DatabaseError as e:
                self.app_logger.error(e)
                self.app_logger.error(sqlplus_script.replace('\n',' '))
                sys.exit(1)

            for command in command_list:
                stdin = ["gs s PMMManagingService", 
                    "\n", 
                    command, 
                    "\n"
                    ]
                sh.conqt(_in=stdin)


    def schedule_aggr(self,schema):
        """
        Schedule all the Aggregations for the given schema
        """
        with ManagedDbConnection(self.DB_USER,
                self.DB_PASSWORD,self.ORACLE_SID,self.DB_HOST) as db:
            cursor=db.cursor()
            command_list=[]
            self.app_logger.info("Activating TRAggr tasks")
            #Activate Aggregations
            sqlplus_script="""
                select 'ex m ActivateRule|' || INFO 
                from config_db.n2_scheduler_task where task in
                (select mt_task_id 
                    from pmmconf_db.pmm_om_master_table 
                    where mt_dbname ='{schema}')   
            """.format(schema=schema)
            try:
                cursor.execute(sqlplus_script)
                for row in filter(None,cursor):
                    command_list.append(row[0])
            except cx_Oracle.DatabaseError as e:
                self.app_logger.error(e)
                self.app_logger.error(sqlplus_script.replace('\n',' '))
                sys.exit(1)

            for command in command_list:
                stdin = ["gs m PMMManagingService", 
                    "\n", 
                    command, 
                    "\n"
                    ]
                sh.conqt(_in=stdin)

            self.app_logger.info("Scheduling TRAggr tasks")
            #Schedule Aggregations
            command_list=[]
            sqlplus_script="""
                select 'ex s ScheduleRule|TRAggr|'||mt_rule_id||'|ScheduleUnits@'||
                DECODE (MT_SUFFIX, '15M', 'Hours',
                'HR', 'Hours',
                'DY','Days',
                'WK','Weeks',
                'MO','Months',
                'YR','Years')
                ||'|ScheduleNumUnits@1|ScheduleStartDate@'||to_char(sysdate,'YYYY-MM-DD HH24:MI') 
                from config_db.n2_timer, config_db.n2_scheduler_task, pmmconf_db.pmm_om_master_table
                where clock_id = task
                and task = mt_task_id
                and mt_dbname ='{schema}'
            """.format(schema=schema)
            try:
                cursor.execute(sqlplus_script)
                for row in filter(None,cursor):
                    command_list.append(row[0])
            except cx_Oracle.DatabaseError as e:
                self.app_logger.error(e)
                self.app_logger.error(sqlplus_script.replace('\n',' '))
                sys.exit(1)

            start_date=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:00")
            for command in command_list:
                stdin = ["gs s PMMManagingService", 
                    "\n", 
                    command,
                    "\n"
                    ]
                sh.conqt(_in=stdin)
        

    def run_cf_tables(self,schema):
        """
        run all the n2 tasks like cfgtables
        """
        self.app_logger.info("Running CfgTable tasks")
        with ManagedDbConnection(self.DB_USER,
                self.DB_PASSWORD,self.ORACLE_SID,self.DB_HOST) as db:
            cursor=db.cursor()
            task_list=[]
            sqlplus_script="""
                select TASK 
                from CONFIG_DB.N2_SCHEDULER_TASK t 
                where DEST = 'CfgTable' 
                and INFO in 
                (select 'CfgTable|'||RULE_ID 
                    from PMMCONF_DB.PMM_ENT_CONF 
                    where CONF_DB_NAME = '{schema}')               
            """.format(schema=schema)
            try:
                cursor.execute(sqlplus_script)
                for row in filter(None,cursor):
                    task_list.append(row[0])
            except cx_Oracle.DatabaseError as e:
                self.app_logger.error(e)
                self.app_logger.error(sqlplus_script.replace('\n',' '))
                sys.exit(1)
        for task in task_list:
            stdin = ["gs s the-scheduler", 
                "\n",
                "ex s ResumeTask {task}".format(task=task), 
                "\n",
                "ex s ScheduleTask {task}".format(task=task), 
                "\n"]
            sh.conqt(_in=stdin)

    def compare_data(self,partrans,oracle_data,parse_hld,oracle_cfg_data):
        """
        Compares the data given
        """
        self.app_logger.info("Executing tests")
        tables_df=parse_hld.metadata['Tables'].dropna(how='all')
        schema=partrans.configuration["schema"]
        raw_data=partrans.transformed_data
        report={}
        for index,row in tables_df.iterrows():
            table_name=row['Table Name']
            resolution=row['Base Granularity']
            ftable_name="{schema}.{table_name}_{resolution}"\
                .format(schema=schema,
                    table_name=table_name,
                    resolution=resolution)
            counters_df=parse_hld.metadata['Key_Counters_Kpis']
            #Filer columns for the table
            counters_df=counters_df.loc[counters_df['Table Name']==table_name]
            #Filer out KPI columns
            counters_df=counters_df.loc[counters_df['TYPE']!='KPI']
            counters=list(counters_df['Counter/KPI DB Name'])
            report[ftable_name]={
                'raw_data':False,
                'oracle_data':False,
                'raw_data_records':0,
                'oracle_records':0,
                'diffs':{},
                'keys':[],
                'missing_raw_data_records':[],
                'missing_oracle_records':[],
                'missing_raw_data_columns':[],
                'missing_oracle_columns':[],
                'missing_in_conf':[],
                'cfg_table':'',
                'validation':""
                }
            #Data is available in Oracle
            if ftable_name in oracle_data:
                if not oracle_data[ftable_name]['data'].empty:
                    report[ftable_name]['oracle_data']=True
                    report[ftable_name]['oracle_records']=\
                        len(oracle_data[ftable_name]['data'].index)
                report[ftable_name]['missing_oracle_columns']=\
                    list(set(counters)-\
                    set(oracle_data[ftable_name]['data'].columns.tolist()))

            #Data is available in Raw Data
            if ftable_name in raw_data:
                if not raw_data[ftable_name]['data'].empty:
                    report[ftable_name]['raw_data']=True
                    report[ftable_name]['raw_data_records']=\
                        len(raw_data[ftable_name]['data'].index)
                report[ftable_name]['missing_raw_data_columns']=\
                    list(set(counters)-\
                    set(raw_data[ftable_name]['data'].columns.tolist()))


            #Validate db instances are in cfg table
            if report[ftable_name]['oracle_data']:
                key_list=self.get_table_index(table_name,schema)
                #Check all the keys do exist in the db
                cfg_table=oracle_data[ftable_name]['cfg_table']
                temp=list(set(key_list)-\
                    set(oracle_data[ftable_name]['data'].columns.tolist()))
                if len(temp)>0:
                    # print(ftable_name,temp)
                    report[ftable_name]['missing_in_conf']=\
                        "missing keys in db table {keys}"\
                        .format(keys=','.join(temp))
                    report[ftable_name]['cfg_table']=cfg_table
                    continue
                df_db=oracle_data[ftable_name]['data'].set_index(key_list)
                report[ftable_name]['cfg_table']=cfg_table

                #Check all the keys do exist in the cf table
                temp=list(set(key_list)-\
                    set(oracle_cfg_data[cfg_table].columns.tolist()))
                if len(temp)>0:
                    report[ftable_name]['missing_in_conf']=\
                        "missing keys in cfg table {keys}"\
                        .format(keys=','.join(temp))
                    report[ftable_name]['cfg_table']=cfg_table
                    continue

                df_cfg=oracle_cfg_data[cfg_table].set_index(key_list)
                #Get the keys found in db and not in cfg
                temp_list=df_db.index.difference(df_cfg.index).tolist()
                for temp in temp_list:
                    temp=self.convert_dt_str(list(temp))
                    #Store the keys as string and not as a list
                    report[ftable_name]['missing_in_conf']\
                        .append(','.join(temp))
                    report[ftable_name]['cfg_table']=cfg_table

            #Data found in oracle and in raw data
            if report[ftable_name]['raw_data'] \
                and report[ftable_name]['oracle_data']:
                #Get the Keys and add the DATETIME to it
                key_list=self.get_table_index(table_name,schema)+['DATETIME']
                report[ftable_name]['keys']=key_list
                #Check all the keys do exist in the raw data
                temp=list(set(key_list)-\
                    set(raw_data[ftable_name]['data'].columns.tolist()))
                if len(temp)>0:
                    report[ftable_name]['validation']=\
                        "missing keys in raw_data {keys}"\
                        .format(keys=','.join(temp))
                    continue
                #Check all the keys do exist in the db
                temp=list(set(key_list)-\
                    set(oracle_data[ftable_name]['data'].columns.tolist()))
                if len(temp)>0:
                    report[ftable_name]['validation']=\
                        "missing keys in oracle_data {keys}"\
                        .format(keys=','.join(temp))
                    continue

                #Set the index in the data frames
                df_rd=raw_data[ftable_name]['data'].set_index(key_list)
                df_db=oracle_data[ftable_name]['data'].set_index(key_list)

                #Get the keys found in raw data and not in db
                temp_list=df_rd.index.difference(df_db.index).tolist()
                for temp in temp_list:
                    temp=self.convert_dt_str(list(temp))
                    #Store the keys as string and not as a list
                    report[ftable_name]['missing_oracle_records']\
                        .append(','.join([str(i) for i in temp]))

                #Get the keys found in db and not in raw data
                temp_list=df_db.index.difference(df_rd.index).tolist()
                for temp in temp_list:
                    temp=self.convert_dt_str(list(temp))
                    #Store the keys as string and not as a list
                    report[ftable_name]['missing_raw_data_records']\
                        .append(','.join([str(i) for i in temp]))

                #Inner Join the raw data dna db dataframes
                joined_df=pd.merge(df_rd,
                    df_db,
                    right_index=True,
                    left_index=True,
                    how='inner')
                #Loop over all keys found in raw data and db
                for index,row in joined_df.iterrows():
                    #Loop over all counters defined in the HLD
                    key_dict={}
                    for counter in counters:
                        #If counter is found in rd and db it can be compared
                        if counter+"_x" in row and counter+"_y" in row:
                            try:
                                #Try to see if is double
                                diff=round(float(row[counter+"_x"]),3)\
                                    -round(float(row[counter+"_y"]),3)
                                if diff!=0:
                                    key_dict[counter]={
                                        'rd':row[counter+"_x"],
                                        'db':row[counter+"_y"],
                                        }
                            except Exception as e:
                                #Compare as string
                                if row[counter+"_x"]!=row[counter+"_y"]:
                                    key_dict[counter]={
                                        'rd':row[counter+"_x"],
                                        'db':row[counter+"_y"],
                                        }
                    if key_dict:
                        #If diff found add it to the diffs in the final report
                        index=self.convert_dt_str(list(index))
                        #convert the index to string
                        index_str=','.join(list(index))
                        report[ftable_name]['diffs'][index_str]=key_dict
        return report

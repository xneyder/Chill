""" Partrans.py:

 Description: Parse and transform raw data

 Created by : Daniel Jaramillo
 Creation Date: 11/01/2019
 Modified by:     Date:
 All rights(C) reserved to Teoco
"""
import os
import sys
import glob
import traceback
import cx_Oracle
import pandas as pd
import copy
import math
from datetime import datetime, timedelta
from StringIO import StringIO
from LoggerInit import LoggerInit
from ManagedDbConnection import ManagedDbConnection
from collections import OrderedDict


class Partrans:

    def __init__(self,conf_file,DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST):
        self.DB_USER=DB_USER
        self.DB_PASSWORD=DB_PASSWORD
        self.ORACLE_SID=ORACLE_SID
        self.DB_HOST=DB_HOST
        self.transformed_data={}
        self.conf_file=conf_file
        self.app_logger=LoggerInit().get_logger("Partrans")
        self.parse_conf_file()
        self.execute_views()
        self.datetime_list=set()
        self.errors=[]

    def parse_conf_file(self):
        """
        Description:  Parse the Chill sheet
        """
        xl=pd.ExcelFile(self.conf_file)
        self.configuration={'columns':OrderedDict(),'views':OrderedDict()}
        df=xl.parse('Chill')
        df=df.dropna(how='all')
        start_fields=False
        start_views=False
        for index,row in df.iterrows():
            #Ignore empty lines
            if not row[0]:
                continue
            #Set the start for the fields
            if row[0]=='view' and not start_views:
                start_views=True
                start_fields=False
                continue
            #Set the start for the fields
            if row[0]=='field' and not start_fields:
                start_fields=True
                start_views=False
                continue
            #Parse views
            if start_views:
                view=row[0]
                self.configuration['views'][view]=row[1]
            #Parse a field
            if start_fields:
                field=row[0]
                #OM group is special in the jcons config dictionary
                if field=='OM_GROUP':
                    self.configuration['OM_GROUP']={
                        'source':row[1],
                        'tag':row[2],
                        'column':row[3],
                        'value':row[4],
                        'function':row[5]
                    }
                else:
                    cs=row[8]
                    if cs not in self.configuration['columns']:
                        self.configuration['columns'][cs]=OrderedDict()
                    self.configuration['columns'][cs][field]={
                        'source':row[1],
                        'tag':row[2],
                        'inputs':row[3],
                        'value':row[4],
                        'function':row[5],
                        'view':row[6],
                        'default':row[7],
                        'cs':cs,
                    }
            else:
                #Parse regular fields
                self.configuration[row[0]]=row[1]

    def parse_valid_lines(self):
        valid_lines=self.configuration['valid_lines'].replace('[','')
        valid_lines=valid_lines.replace(']','')
        initial=valid_lines.split(':')[0]
        if initial:
            initial=int(initial)
        else:
            initial=None
        final=valid_lines.split(':')[1]
        if final:
            final=int(final)
        else:
            final=None
        return initial,final

    def get_tag(self,alldata,tag):
        """
        resurns the line in the file that contains the tag
        """
        result=""
        for line in alldata:
            if tag in line:
                result=line
                break
        return result

    def execute_views(self):
        """
        Executes and saves all the views in a dictionary
        """
        self.views={}
        with ManagedDbConnection(self.DB_USER,\
            self.DB_PASSWORD,self.ORACLE_SID,self.DB_HOST) as db:
            cursor=db.cursor()
            for view_name,sql in self.configuration['views'].items():
                try:
                    cursor.execute(sql)
                    names = [ x[0].upper() for x in cursor.description]
                    rows = cursor.fetchall()
                    self.views[view_name]=pd.DataFrame( rows, columns=names)
                except cx_Oracle.DatabaseError as e:
                    self.app_logger.error(e)
                    self.app_logger.error(sql.replace('\n',' '))

    def calc_field(self,column_name,column,file_name,alldata,df,table_name):
        try:
            function='None'
            if column['source'].lower()=="filename":
                bfile_name=os.path.basename(file_name)
                function=column['function']\
                    .replace('arg1',"'"+bfile_name+"'")
                return eval(function)

            elif column['source'].lower()=="tag":
                #Find the tag
                tag=self.get_tag(alldata,column['tag'])
                function=column['function']\
                        .replace('tag',"'"+tag+"'")

                return eval(function)

            elif column['source'].lower()=="constant":
                return column['value']

            if column['source'].lower()=="column":
                return_values=[]
                source_column_list=column['inputs'].split(',')
                for index,row in df.iterrows():
                    function=column['function']
                    for idx,source_column in enumerate(source_column_list):
                        source_column=source_column.replace(' ','')
                        function=function.replace('arg'+str(idx+1),
                                "'"+str(row[source_column])+"'")
                    return_values.append(eval(function))
                return return_values

            if column['source'].lower()=="lookup":
                return_values=[]
                source_column_list=column['inputs'].split(',')
                for index,row in df.iterrows():
                    function=column['function']
                    for idx,source_column in enumerate(source_column_list):
                        source_column=source_column.replace(' ','')
                        try:
                            function=function.replace('arg'+str(idx+1),
                                "'"+str(row[source_column])+"'")
                        except KeyError:
                            self.app_logger.error("Field {source_column} not \
                                found and is beign used for {column_name}"\
                                .format(column_name=column_name,
                                    source_column=source_column
                                ))
                    view_name=column['view']
                    function=function.replace('view',
                        'self.views["'+view_name+'"]')
                    # print(self.views["CONF_VZ_ENODEB_VW"]['LK_ENODEB_ID'])
                    try:
                        result=eval(function)
                    except Exception as e:
                        result=column['default']
                    #Caculate the default value
                    # if not result:
                    return_values.append(result)
                return return_values

        except Exception as e:
            self.app_logger.error(table_name+' '+column_name+' '+function+": "+
                traceback.format_exc())
            self.errors.append(table_name+' '+column_name+' '+function+": "+
                traceback.format_exc())
            return None

    def parse_data(self,parse_hld):
        counters_df=parse_hld.metadata['Key_Counters_Kpis']
        tables_df=parse_hld.metadata['Tables']
        #Check valid_lines
        initial,final=self.parse_valid_lines()
        file_list=glob.glob(os.path.join(self.configuration['input_rd'],
            self.configuration['input_rd_mask']))
        self.app_logger.info("Parsing {num_files} raw data files"\
            .format(num_files=len(file_list)))
        for file_name in file_list:
            # self.app_logger.info("Parsing {file_name}"\
            #     .format(file_name=file_name))
            with open(file_name) as file:
                alldata=file.read().split('\n')
                datatoparse=alldata[initial:final]
                #Remove lines to ignore from raw data file
                if 'ignore_lines' in self.configuration:
                    middata=datatoparse
                    datatoparse=[]
                    for line in middata:
                        if line in self.configuration['ignore_lines']:
                            continue
                        datatoparse.append(line)
                datatoparse='\n'.join(datatoparse)
                stream=StringIO(datatoparse)
                if self.configuration["format"].lower() == "csv":
                    df=pd.read_csv(stream,sep=self.configuration["delimiter"])
                df_len=len(df.index)
                #Get OM_GROUP
                om_group=self.calc_field('OM_GROUP',
                    self.configuration["OM_GROUP"],
                    file_name,
                    alldata,
                    df,
                    '')
                #Get table Name
                temp_df=tables_df.loc\
                    [tables_df['Counter Group in RD']==om_group]
                if temp_df.empty:
                    #No definition for om_group in HLD
                    # self.app_logger.warning("No definition for OM {om_group} \
                    #     in HLD".format(om_group=om_group))
                    continue
                table_name=temp_df.iloc[0,:]['Table Name']
                resolution=temp_df.iloc[0,:]['Base Granularity']
                ftable_name="{schema}.{table_name}_{resolution}"\
                    .format(schema=self.configuration["schema"],
                        table_name=table_name,
                        resolution=resolution)
                #Get relevant data from hld for OM GROUP
                om_hld_df=counters_df.loc\
                    [counters_df['Table Name']==table_name]
                #Build the final df looping all the columns in the HLD
                table_df=pd.DataFrame()
                #Calculate the columns that dont have nay function
                for index,row in om_hld_df.iterrows():
                    column_name=row['Counter/KPI DB Name']
                    raw_data_name=row['Raw Data Counter Name/OID']
                    type=row['TYPE']
                    #Ignore KPI
                    if type =='KPI':
                        continue
                    calculate=True

                    if column_name in self.configuration["columns"]["ALL"]:
                        calculate=False
                    if table_name in self.configuration["columns"]:
                        if column_name in \
                            self.configuration["columns"][table_name]:
                                calculate=False

                    #There is not function so take the value from the raw data
                    if calculate:
                        try:
                            #If the df is empty make it equal to the column
                            if table_df.empty:
                                table_df=df.loc[:,[raw_data_name]]
                            else:
                                table_df = pd.concat([table_df,
                                    df.loc[:,[raw_data_name]]],
                                    axis=1, join_axes=[table_df.index])
                            #Rename the column in the DataFrame
                            table_df.rename(columns=\
                                {raw_data_name: column_name}, inplace=True)
                            df[column_name]=table_df[column_name]
                        except KeyError:
                            # self.app_logger.warning(
                            #     'table {table_name}, \
                            #     column {raw_data_name}/{column_name}\
                            #     not found in the raw data'\
                            #     .format(raw_data_name=raw_data_name,
                            #         column_name=column_name,
                            #         table_name=table_name))
                            pass
                #Calculate the columns with a function defined in the conf file
                #concatename the ALL CS to the current table
                if "ALL" in self.configuration["columns"]:
                    d1=copy.copy(self.configuration["columns"]["ALL"])
                else:
                    d1={}
                if table_name in self.configuration["columns"]:
                    d2=copy.copy(self.configuration["columns"][table_name])
                else:
                    d2={}

                fields=[]
                for key,item in d1.items():
                    fields.append((key,item))
                for key,item in d2.items():
                    fields.append((key,item))
                
                for field in fields:
                    column_name=field[0]
                    column=field[1]
                    if column["source"] in ["filename","tag","constant"]:
                        column_value=self.calc_field(
                            column_name,
                            column,
                            file_name,
                            alldata,
                            df,
                            table_name)
                        #Because is only one value create a series with
                        #the df size
                        sr=pd.Series([column_value]*df_len)
                        table_df[column_name]=sr
                        df[column_name]=sr
                    elif  column["source"] in ["column","lookup"]:
                        #Need to evaluate the function per each record in df
                        column_values=self.calc_field(
                            column_name,
                            column,
                            file_name,
                            alldata,
                            df,
                            table_name)
                        sr=pd.Series(column_values)
                        table_df[column_name]=sr
                        df[column_name]=sr
                    else:
                        self.app_logger.error('ERROR {source} not valid:'\
                            .format(source=column["source"]))




                #Add final data to the transformed_data
                if ftable_name not in self.transformed_data:
                    self.transformed_data[ftable_name]={'data':table_df}
                else:
                    frames=[self.transformed_data[ftable_name]['data'],table_df]
                    self.transformed_data[ftable_name]['data']=pd.concat(frames)
                #set the datetime list
                self.transformed_data[ftable_name]['datetime_list']\
                    =set(self.transformed_data[ftable_name]['data']['DATETIME'])
        # self.get_datetime_list()
        # quit()

    def get_datetime_list(self):
        """
        Gets all the datetimes found in the raw data
        """
        datetime_list=[]
        for key,data in self.transformed_data.items():
            datetime_list.extend(data['DATETIME'])
        self.datetime_list=set(datetime_list)

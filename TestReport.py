""" chill.py:
 Description: generates the report with the result of the tests

 Created by : Daniel Jaramillo
 Creation Date: 11/01/2019
 Modified by:     Date:
 All rights(C) reserved to Teoco
"""
import os
import sys
from openpyxl import load_workbook
from LoggerInit import LoggerInit
from junit import TestCase, TestSuite, TestReport


class TestReportJunit:
    """
    Junit xml report
    """

    def __init__(self,report):
        self.app_logger=LoggerInit().get_logger("TestReport")
        self.report=report
        # print(self.report)

    def create_db_errors(self):
        """
        Reports DBL Errors
        """
        error_dict=self.report['error_list']
        self.dbl_tc_list=[]
        for table in self.report['table_list']:
            test_name='DBLoader Errors for table {table}'.format(table=table)
            if table in error_dict:
                #Error found for table
                for file_name,data in error_dict[table].items():
                    ora_error_list=data[0]
                    self.dbl_tc_list.append(TestCase(name=test_name,
                        failure=', '.join(list(ora_error_list)),
                        failure_message=table
                        ))
            else:
                #Test passed
                self.dbl_tc_list.append(TestCase(name=test_name))
        self.dbl_ts_list = TestSuite(self.dbl_tc_list,
            name='DBLoader Errors')


    def create_data_summary(self):
        """
        Fills number of records found in the database and in the raw data
        """
        self.data_sum_tc_list=[]
        for table,data in self.report['data_report'].items():
            test_name='Number of Records for table {table}'.format(table=table)
            message='Number of records: db {db}, rd {rd}'\
                .format(db=data['oracle_records'],
                    rd=data['raw_data_records']
                )
            if data['oracle_records']!=data['raw_data_records']:
                self.data_sum_tc_list.append(TestCase(name=test_name,
                    failure=message,
                    failure_message=table
                    ))
            elif data['oracle_records']==data['raw_data_records']:
                self.data_sum_tc_list.append(TestCase(name=test_name,
                    systemOut=message))
        self.data_sum_ts_list = TestSuite(self.data_sum_tc_list,
            name='Number of records comparison')


    def create_data_missing(self):
        """
        Fills the data missing in db and in the raw data
        """
        self.data_miss_tc_list=[]
        for table,data in self.report['data_report'].items():
            #No missing data found
            if not data['missing_oracle_records'] and \
                not data['missing_raw_data_records'] and\
                not data['missing_in_conf']:
                test_name='Missing data for table {table}'\
                    .format(table=table)
                self.data_miss_tc_list.append(TestCase(name=test_name))
            else:
                if data['missing_oracle_records']:
                    test_name='Missing data for table {table}'\
                        .format(table=table)
                    keys='\n'.join(data['missing_oracle_records'])
                    self.data_miss_tc_list.append(TestCase(name=test_name,
                        failure='missing keys in db:\n {keys}'\
                        .format(keys=keys),
                        failure_message=table
                        ))
                if data['missing_raw_data_records']:
                    test_name='Missing data for table {table}'\
                        .format(table=table)
                    keys='\n'.join(data['missing_raw_data_records'])
                    self.data_miss_tc_list.append(TestCase(name=test_name,
                        failure='missing keys in rd:\n {keys}'\
                        .format(keys=keys),
                        failure_message=table
                        ))
                if data['missing_in_conf']:
                    test_name='Configuration data for table {table}'\
                        .format(table=table)
                    # keys='\n'.join(data['missing_in_conf'])
                    keys=data['missing_in_conf']
                    self.data_miss_tc_list.append(TestCase(name=test_name,
                        failure='{cfg_table}: {keys}'\
                        .format(keys=keys,
                            cfg_table=data['cfg_table']),
                        failure_message=table
                        ))
        self.data_miss_ts_list = TestSuite(self.data_miss_tc_list,
            name='Missing Data')


    def create_data_diffs(self):
        """
        Reports teh differnces in data between DB and RD
        """
        self.data_diffs_tc_list=[]
        for table,data in self.report['data_report'].items():
            test_name='Data differences for table {table}'.format(table=table)
            #No differences found
            if not data['diffs']:
                self.data_diffs_tc_list.append(TestCase(name=test_name))
            else:
                message=""
                for key,diffs in data['diffs'].items():
                    message+='\n{key}: '.format(key=key)
                    for col_name,val in diffs.items():
                        message+=', {col_name}_DB={db} => {col_name}_RD={rd}'\
                            .format(col_name=col_name,
                                db=val['db'],
                                rd=val['rd'],
                                )
                self.data_diffs_tc_list.append(TestCase(name=test_name,
                    failure=message,
                    failure_message=table
                    ))
        self.data_diffs_ts_list = TestSuite(self.data_diffs_tc_list,
            name='Data Difference')

    def create_missing_cols(self):
        """
        Fills the data missing in db and in the raw data
        """
        self.missing_cols_tc_list=[]
        for table,data in self.report['data_report'].items():
            test_name='Missing columns for table {table}'.format(table=table)
            #No missing columns found
            if not data['missing_oracle_columns']:
                # and not data['missing_raw_data_columns']:
                self.missing_cols_tc_list.append(TestCase(name=test_name))
            else:
                #Write db missing columns
                message="Columns missing in db {cols}"\
                    .format(cols=', '.join(data['missing_oracle_columns']))
                self.missing_cols_tc_list.append(TestCase(name=test_name,
                    failure=message,
                    failure_message=table
                    ))
            # #Write rd missing columns
            # rd_row=row
            # for column_name in data['missing_raw_data_columns']:
            #     ws.cell(row=rd_row, column=3, value=column_name)
            #     rd_row+=1
            # row=max(db_row,rd_row)+1
        self.missing_cols_ts_list = TestSuite(self.missing_cols_tc_list,
            name='Missing Columns')

    def save_file(self,filename):
        """
        saves the final xml junit file.
        """
        report = TestReport([self.dbl_ts_list,
            self.data_sum_ts_list,
            self.data_miss_ts_list,
            self.data_diffs_ts_list,
            self.missing_cols_ts_list,
            ],
            name='ChillReport')
        # print report.toXml(prettyPrint=True)
        xmlReport=report.toXml(prettyPrint=True)
        with open(filename,'w') as f:
            f.write(xmlReport)
        self.app_logger.info('{filename} created'\
            .format(filename=filename))

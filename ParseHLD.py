import pandas as pd

class ParseHLD:
    def __init__(self,hld_file):
        self.hld_file=hld_file
        self.metadata={}

    def parse_front_page(self):
        """
        Description:  Parse the Front Page sheet
        Input Parametes:
            xl: Pandas excel file object
        """
        self.metadata['Front Page']={}
        df=self.xl.parse('Front Page')
        df=df.iloc[:,[0,1]].dropna(how='all')
        for index,row in df.iterrows():
            if row[0] == "Revision History":
                break
            self.metadata['Front Page'][row[0]]=row[1]


    def parse_library_info(self):
        """
        Description:  Parse the Library Info sheet
        Input Parametes:
            xl: Pandas excel file object
        """
        self.metadata['Library Info']={}
        df=self.xl.parse('Library Info')
        df=df.iloc[:,[1,2]].dropna(how='all')
        for index,row in df.iterrows():
            try:
                if row[0] == "Table Retention:":
                    break
                self.metadata['Library Info'][row[0]]=row[1]
            except IndexError:
                continue

    def parse_table(self,sheet_name):
        """
        Description:  Parse the sheet in table format
        Input Parametes:
            xl: Pandas excel file object
            sheet name
        """
        self.metadata[sheet_name]={}
        df=self.xl.parse(sheet_name)
        self.metadata[sheet_name]=df.iloc[2:,1:]


    def load_hld(self):
        """
        Description: Load the configuration from HLD file
        Input Parametes:
            hld_file: Excel containing the functional specification for the library
        """
        self.xl=pd.ExcelFile(self.hld_file)
        self.parse_front_page()
        self.parse_library_info()
        self.parse_table("Entities")
        self.parse_table("Tables")
        self.parse_table("Key_Counters_Kpis")

# imports 
# standard 
import time
import os 
#cleaning
import pandas as pd 
import numpy as np
import json
import re
import unicodedata

#debuggin
from pdb import set_trace as bp
import logging 
logging.basicConfig(filename='std.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logger=logging.getLogger('logger') 

# dirs
#TODO: might be nice to put this in a main function also maybe add dirs to a config file 
# Change these:
in_dir = '/Users/gchickering/OneDrive - American Institutes for Research in the Behavioral Sciences/Github/mrt_to_JSON/MT_MRT'
out_dir = '/Users/gchickering/OneDrive - American Institutes for Research in the Behavioral Sciences/Github/mrt_to_JSON/GC_JSON'
#in_dir = '/Users/ebuehler/American Institutes for Research in the Behavioral Sciences/NCES Table Scraping - MT_MRT' # sharepoint location 
#out_dir = '/Users/ebuehler/American Institutes for Research in the Behavioral Sciences/MRT_JSON' # write location

# classes and helpers -----------------------------------------------------------------------------------------------

class mrtConvert:
    def __init__(self, excel):
        self.excel = excel           
    json = None

    def convertColumnTypes(self):
        new_dict = self.json
       # print(new_dict['meta'])
        for row in range(0,len(new_dict['meta'])): # data 
            for key in new_dict['meta']:
                if key == 'digest_table_id':
                    value= new_dict['meta'][key]
                    value = str(value)
                    new_dict['meta'][key]= value
                if key == 'general_note':
                    value = new_dict['meta'][key]
                    value = str(value)
                    new_dict['meta'][key]= value



        for row in range(0,len(new_dict['data'])): # data 
            new_dict['data'][row] = {k: v for k, v in new_dict['data'][row].items() if not pd.isna(v)}
            for key in new_dict['data'][row]:
                if key == 'standard_error':
                    value= new_dict['data'][row][key]
                    value = str(value)
                    new_dict['data'][row][key]= value
                elif key == 'value':
                    value= new_dict['data'][row][key]
                    value = str(value)
                    new_dict['data'][row][key]= value
            

        self.json = new_dict


    def processXLSX(self):
        
        # grab sheets 
        mrt_meta = self.excel['meta']
        mrt_data = self.excel['data']
        new_dict = {}


        # use meta data as base dict 
        data_col_names = list(mrt_data.columns)
        keep_meta = ['digest_table_id', 'digest_table_year']
        data_col_names = [i for i in data_col_names if i not in keep_meta]
        mrt_meta = mrt_meta[mrt_meta.columns.difference(data_col_names)] # don't want data colnames in meta data 
        new_dict['meta'] = mrt_meta.to_dict(orient='index')[0]
        #print(new_dict['meta'])
        new_dict['data'] = "null"

        # more column cleanup
        mrt_data = mrt_data[mrt_data.columns.difference(keep_meta)]

        # move data to meta data dict 
        new_dict['data'] = mrt_data.to_dict(orient='records')

        # remove nan leaves 
        for row in range(0,len(new_dict['data'])): # data 
            new_dict['data'][row] = {k: v for k, v in new_dict['data'][row].items() if not pd.isna(v)}
        new_dict['meta'] = {k: v for k, v in new_dict['meta'].items() if not pd.isna(v)} # meta 

        self.json = new_dict

    def checkConversion(self):

        # seperate out meta/data
        mxl = self.excel['meta']
        mjs = pd.DataFrame(self.json['meta'], index = [0]) 
        xl = self.excel['data']
        js = pd.DataFrame(self.json['data'])

        #META data checks
        # edit mxl (meta data) so it meets processing assumptions if any other are missing there was a problem: 
        # 1) no columns that appears in data other than digest id and year 
        # 2) no nan columns
        # 3) remove repeated meta data row for sub_table id

        data_col_names = list(xl.columns)
        data_col_names = [i for i in data_col_names if i not in ['digest_table_id', 'digest_table_year']]
        mxl = mxl[mxl.columns.difference(data_col_names)] # don't want data colnames in meta data 
        mxl = mxl.dropna(axis=1, how='all')
        if(len(mxl.index) > 1):
            mxl = mxl.iloc[[0]]
  
        
        # check all columns in json are in xl 
        if(not len(mjs.columns.difference(mxl.columns)) == 0):
            logger.warning('Meta: Not all json columns are in xl. Returning False.')
            return(False)

        # check all columns in vice versa
        if(not len(mxl.columns.difference(mjs.columns)) == 0):
            logger.warning('Meta: Not all xl columns are in json. Returning False.')
            return(False)

        # check content
        if(not all((mxl == mjs).all())):
            logger.warning('Meta: Meta data values differ')
            return(False)
        
        #DATA checks
        # edit xl (data) so it meets processing assumptions if any other are missing there was a problem: 
        # 1) no column that appears in data other than digest id and year 
        # 2) no nan columns
        
        data_col_names = list(xl.columns)
        data_col_names = [i for i in data_col_names if i not in ['digest_table_id', 'digest_table_year']]
        xl = xl[data_col_names] # don't want data colnames in meta data 
        xl = xl.dropna(axis=1, how='all')

        # check all columns in json are in xl 
        if(not len(js.columns.difference(xl.columns)) == 0):
            logger.warning('Data: Not all json columns are in xl. Returning False.')
            return(False)

        # check all columns in xl are in json 
        if(not len(xl.columns.difference(js.columns)) == 0):
            logger.warning('Data: Not all json columns are in xl. Returning False.')
            return(False)

        # check row count 
        if(len(js.index) != len(xl.index)):
            logger.warning('Data: Different number of rows. Returning False.')
            return(False)
            
        # content Check
        xl = xl.reindex(sorted(xl.columns), axis=1) # sort for ording when comparing 
        js = js.reindex(sorted(js.columns), axis=1)
        # colsum check for continuous variables 
        # floats, these appear to typically be value and std error 
        if(not all(xl.loc[:, xl.dtypes == 'float64'].sum() == js.loc[:, js.dtypes == 'float64'].sum())):
            logger.warning('Data: Floating point valuas do not match')
            logger.warning(xl.loc[:, xl.dtypes == 'float64'].sum() == js.loc[:, js.dtypes == 'float64'].sum())
            return(False)
        # ints, these appear to typically be year values 
        if(not all(xl.loc[:, xl.dtypes == 'int64'].sum() == js.loc[:, js.dtypes == 'int64'].sum())):
            logger.warning('Data: Int valuas do not match')
            logger.warning(xl.loc[:, xl.dtypes == 'int64'].sum() == js.loc[:, js.dtypes == 'int64'].sum())
            return(False)
        # check for that categorical columns are equal
        if(not all((xl.loc[:, xl.dtypes == 'object'].fillna('999') == js.loc[:, js.dtypes == 'object'].fillna('999')).all())):
            logger.warning('Data: Obj column had difference')
            logger.warning((xl.loc[:, xl.dtypes == 'object'].fillna('999') == js.loc[:, js.dtypes == 'object'].fillna('999')).all())
            return(False)
        return(True)





# execution -----------------------------------------------------------------------------------------------
def main():
    start = time.time() # time 

    # for round of MRT
    for round in os.listdir(in_dir):
        if re.match('Round\d', round):
            round_dir = os.listdir(os.path.join(in_dir,round))
            # round = "Round2" 
            # for Date in MRT
            for date in round_dir:
                #print(date)
    #TODO: probably simplify some of these rules after checking with team which folders are essential 
                if re.match('\d{4}-\d{2}-\d{2}', date):
                    # date = '2021-07-06'  
                    path = os.path.join(in_dir,round,date)
                    date_dir = os.listdir(path)
                    if len(date_dir) != 0:
                        if date_dir[0] == 'AIR':
                            path = os.path.join(in_dir,round,date, 'AIR')
                            date_dir = os.listdir(path) 
                    else:
                        pass         
                    # fore File in MRT
                    #date_dir
                    for file in date_dir:
                        #print(file)
                        # read mrt 
                        # file = 'MRT_333_10.xlsx'
                        print('starting', round, date, file)
                    
                        try: # skip over summary files 
                            mrt_xlsx = pd.read_excel(os.path.join(path,file), sheet_name=None, dtype= {'digest_table_id':object})
                            #mrt_xlsx['meta']['general_note'] = mrt_xlsx['meta']['general_note'].astype('string')
                            #mrt_xlsx['meta']['general_note'] = mrt_xlsx['meta']['general_note'].replace("’", "'")
                        
                        except: 
                            pass 
                        # convert xl to json
                        mrt = mrtConvert(mrt_xlsx)
                        mrt.processXLSX()
                        # check converstion
                        if(mrt.checkConversion()):
                            mrt.convertColumnTypes()
                            logger.warning('conversion for ' + round + '/' + date + '/' + file + ' suceeded')
                            logger.warning('writing to JSON')
                            
                            # write to json
    #TODO: maybe make json writing part of the class, maybe add extra AIR subdirectory 
                            write_path = os.path.join(out_dir, round, date) # check if path exists 
                            if(not os.path.isdir(write_path)):
                                os.makedirs(os.path.join(out_dir, round, date))
                            afile = open(os.path.join(write_path, file[0:-5] + '.json'), 'w') # pop off xlsx
                            try: 
                                afile.write(json.dumps(mrt.json, indent=4, allow_nan = False,  ensure_ascii=False))
                            except: 
                                logger.warning('failed to write ' + round + '/' + date + '/' + file + ' to JSON')
                                
                            afile.close()
                        # else failed conversion QC
                        else:
                            logger.warning('conversion for ' + round + '/' + date + '/' + file + ' failed')
    end = time.time()  
    total = end - start
    total = str(total)
    logger.warning('time in seconds='+ total)


if __name__=="__main__":
    main()
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
import xlrd
import openpyxl
import xlsxwriter
from pandas.io.json import json_normalize


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
out_dir_excel = '/Users/gchickering/OneDrive - American Institutes for Research in the Behavioral Sciences/Github/mrt_to_JSON/Excel_Conversion'
#in_dir = '/Users/ebuehler/American Institutes for Research in the Behavioral Sciences/NCES Table Scraping - MT_MRT' # sharepoint location 
#out_dir = '/Users/ebuehler/American Institutes for Research in the Behavioral Sciences/MRT_JSON' # write location

# classes and helpers -----------------------------------------------------------------------------------------------

class mrtConvert:
    def __init__(self, excel):
        self.excel = excel 
        self.meta_columns = excel['meta'].columns
        self.data_columns = excel['data'].columns 
        self.digest_table_id = excel['data']['digest_table_id']
        self.digest_table_year = excel['data']['digest_table_year']
        self.json = None
        #print(self.meta_columns)      
    #json = None

    def convertColumnTypes(self):
        new_dict = self.json
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

        if 'digest_table_sub_id' in new_dict['meta']:
            del new_dict['meta']['digest_table_sub_id']
        #new_dict['meta']= new_dict['meta'].pop("digest_table_sub_id")

        for row in range(0,len(new_dict['data'])): # data 
            new_dict['data'][row] = {k: v for k, v in new_dict['data'][row].items() if not pd.isna(v)}

            sorted_keys = sorted(new_dict['data'][row].keys())
            new_dict['data'][row] = {key:new_dict['data'][row][key] for key in sorted_keys}
           

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
        keep_meta2 = ['digest_table_id', 'digest_table_year', 'digest_table_sub_id']
        data_col_names = [i for i in data_col_names if i not in keep_meta2]
        mrt_meta = mrt_meta[mrt_meta.columns.difference(data_col_names)] # don't want data colnames in meta data 

        contains_deflator = False
        if mrt_meta['deflator'].notnull().sum() != 0 :
            dict_deflator_values = dict(zip(mrt_meta.digest_table_sub_id, mrt_meta.deflator))
            mrt_meta= mrt_meta.drop(columns = ['deflator','digest_table_sub_id'])
            contains_deflator = True

        new_dict['meta'] = mrt_meta.to_dict(orient='index')[0]
        new_dict['data'] = "null"

        # more column cleanup
        mrt_data = mrt_data[mrt_data.columns.difference(keep_meta)]
        if contains_deflator == True:
            mrt_data["deflator"] = mrt_data['digest_table_sub_id'].map(dict_deflator_values)
        
        # move data to meta data dict 
        new_dict['data'] = mrt_data.to_dict(orient='records')
        # remove nan leaves 
        for row in range(0,len(new_dict['data'])): # data 
            new_dict['data'][row] = {k: v for k, v in new_dict['data'][row].items() if not pd.isna(v)}
        new_dict['meta'] = {k: v for k, v in new_dict['meta'].items() if not pd.isna(v)} # meta 

        self.json = new_dict

    def checkConversion(self, round, date, file, json_dict):
        
            #Bring in Dataframes from step 1
            mrt_meta = self.excel['meta']
            mrt_data = self.excel['data']
        
            #Perform conversations to get rid of NA values and columns that are not in json file for meta 
            data_col_names = list(mrt_meta.columns)
            data_col_names = [i for i in data_col_names if i in ['digest_table_sub_id','digest_table_sub_title', 'digest_table_sub_title_note','headnote']]
            mrt_meta = mrt_meta[mrt_meta.columns.difference(data_col_names)] # don't want data colnames in meta data 
            mrt_meta = mrt_meta.dropna(axis=1, how='all')
            if(len(mrt_meta.index) > 1):
                mrt_meta = mrt_meta.iloc[[0]]

            deflator_check = False
            if 'deflator' in mrt_meta.columns :
                mrt_meta= mrt_meta.drop(columns = ['deflator'])
                deflator_check = True


            ####Perform conversations to get rid of NA values and columns that are not in json file for regular data
            mrt_data = mrt_data.dropna(axis = 1, how = 'all')
            data_col_names = list(mrt_data.columns)
            data_col_names = [i for i in data_col_names if i not in ['digest_table_id', 'digest_table_year']]
            mrt_data = mrt_data[data_col_names] # don't want data colnames in meta data 

            #Meta Data from json file for step 5 
            json_df_meta = pd.json_normalize(json_dict, meta = ['meta'])
            json_df_meta.columns = json_df_meta.columns.str.replace(r'^meta.', '', regex=True)
            json_df_meta =json_df_meta.drop(['data'], axis = 1)

            #Regular Data from json file
            json_df_data = pd.json_normalize(json_dict['data'])

            json_d= json_df_data.columns.dtype
            mrt_d = mrt_data.columns.dtype

             #META data checks
            # edit mxl (meta data) so it meets processing assumptions if any other are missing there was a problem: 
            # 1) no columns that appears in data other than digest id and year 
            # 2) no nan columns
            # 3) remove repeated meta data row for sub_table id

            # check all columns in json are in df
            if(not len(json_df_meta.columns.difference(mrt_meta.columns)) == 0):
                logger.warning('Meta: Not all json columns are in xl. Returning False.')
                return(False)

             # check all columns in vice versa
            if(not len(mrt_meta.columns.difference(json_df_meta.columns)) == 0):
                logger.warning('Meta: Not all xl columns are in json. Returning False.')
                return(False)

            # check content
            if(not all((json_df_meta == mrt_meta).all())):
                logger.warning('Meta: Meta data values differ')
                return(False)
                
            #DATA checks
            # edit xl (data) so it meets processing assumptions if any other are missing there was a problem: 
            # 1) no column that appears in data other than digest id and year 
            # 2) no nan columns

             # check all columns in json are in xl 
            if(not len(json_df_data.columns.difference(mrt_data.columns)) == 0):
                logger.warning('Data: Not all json columns are in xl. Returning False.')
                return(False)

            # check all columns in xl are in json 
            if(not len(mrt_data.columns.difference(json_df_data.columns)) == 0):
                logger.warning('Data: Not all json columns are in xl. Returning False.')
                return(False)

            # check row count 
            if(len(json_df_data.index) != len(mrt_data.index)):
                print("in here 3")
                logger.warning('Data: Different number of rows. Returning False.')
                return(False)

             # content Check
            mrt_data = mrt_data.reindex(sorted(mrt_data.columns), axis=1) # sort for ording when comparing 
            json_df_data = json_df_data.reindex(sorted(json_df_data.columns), axis=1)
        
            if(not all(mrt_data.loc[:, mrt_data.dtypes == 'float64'].sum() == json_df_data.loc[:, json_df_data.dtypes == 'float64'].sum())):
                logger.warning('Data: Floating point valuas do not match')
                logger.warning(mrt_meta.loc[:, mrt_meta.dtypes == 'float64'].sum() == json_df_data.loc[:, json_df_data.dtypes == 'float64'].sum())
                return(False)
            # ints, these appear to typically be year values 
            if(not all(mrt_data.loc[:, mrt_data.dtypes == 'int64'].sum() == json_df_data.loc[:, json_df_data.dtypes == 'int64'].sum())):
                logger.warning('Data: Int valuas do not match')
                logger.warning(mrt_data.loc[:, mrt_data.dtypes == 'int64'].sum() == json_df_data.loc[:, json_df_data.dtypes == 'int64'].sum())
                return(False)
            # # check for that categorical columns are equal
            # if(not all((mrt_meta.loc[:, mrt_meta.dtypes == 'object'].fillna('999') == json_df_data.loc[:, json_df_data.dtypes == 'object'].fillna('999')).all())):
            #     logger.warning('Data: Obj column had difference')
            #     logger.warning((mrt_data.loc[:, mrt_data.dtypes == 'object'].fillna('999') == json_df_data.loc[:, json_df_data.dtypes == 'object'].fillna('999')).all())
            #     return(False)

            #META data checks
            # edit mxl (meta data) so it meets processing assumptions if any other are missing there was a problem: 
            # 1) no columns that appears in data other than digest id and year 
            # 2) no nan columns
            # 3) remove repeated meta data row for sub_table id

            # data_col_names = list(xl.columns)
            # data_col_names = [i for i in data_col_names if i not in ['digest_table_id', 'digest_table_year', 'digest_table_sub_id']]
            # mxl = mxl[mxl.columns.difference(data_col_names)] # don't want data colnames in meta data 
            # mxl = mxl.dropna(axis=1, how='all')
            # if(len(mxl.index) > 1):
            #     mxl = mxl.iloc[[0]]
            # deflator_check = False
            # if 'deflator' in mxl :
            #     dict_deflator_values = dict(zip(mxl.digest_table_sub_id, mxl.deflator))
            #     mxl= mxl.drop(columns = ['deflator','digest_table_sub_id'])
            #     deflator_check = True

            # check all columns in json are in xl 
            # if(not len(mjs.columns.difference(mxl.columns)) == 0):
            #     logger.warning('Meta: Not all json columns are in xl. Returning False.')
            #     return(False)

            # # check all columns in vice versa
            # if(not len(mxl.columns.difference(mjs.columns)) == 0):
            #     logger.warning('Meta: Not all xl columns are in json. Returning False.')
            #     return(False)

            # # check content
            # if(not all((mxl == mjs).all())):
            #     logger.warning('Meta: Meta data values differ')
            #     return(False)
            
            #DATA checks
            # edit xl (data) so it meets processing assumptions if any other are missing there was a problem: 
            # 1) no column that appears in data other than digest id and year 
            # 2) no nan columns
            
            # data_col_names = list(xl.columns)
            # data_col_names = [i for i in data_col_names if i not in ['digest_table_id', 'digest_table_year']]
            # xl = xl[data_col_names] # don't want data colnames in meta data 
            # xl = xl.dropna(axis=1, how='all')
            # if deflator_check == True:
            #     xl["deflator"]= xl['digest_table_sub_id'].map(dict_deflator_values)
            #     js["deflator"]= js['digest_table_sub_id'].map(dict_deflator_values)
            
            # # check all columns in json are in xl 
            # if(not len(js.columns.difference(xl.columns)) == 0):
            #     print("in here 1")
            #     logger.warning('Data: Not all json columns are in xl. Returning False.')
            #     return(False)

            # # check all columns in xl are in json 
            # if(not len(xl.columns.difference(js.columns)) == 0):
            #     print("in here 2")
            #     logger.warning('Data: Not all json columns are in xl. Returning False.')
            #     return(False)

            # # check row count 
            # if(len(js.index) != len(xl.index)):
            #     print("in here 3")
            #     logger.warning('Data: Different number of rows. Returning False.')
            #     return(False)
                
            # content Check
            # xl = xl.reindex(sorted(xl.columns), axis=1) # sort for ording when comparing 
            # js = js.reindex(sorted(js.columns), axis=1)
        
            # if(not all(xl.loc[:, xl.dtypes == 'float64'].sum() == js.loc[:, js.dtypes == 'float64'].sum())):
            #     logger.warning('Data: Floating point valuas do not match')
            #     logger.warning(xl.loc[:, xl.dtypes == 'float64'].sum() == js.loc[:, js.dtypes == 'float64'].sum())
            #     return(False)
            # # ints, these appear to typically be year values 
            # if(not all(xl.loc[:, xl.dtypes == 'int64'].sum() == js.loc[:, js.dtypes == 'int64'].sum())):
            #     logger.warning('Data: Int valuas do not match')
            #     logger.warning(xl.loc[:, xl.dtypes == 'int64'].sum() == js.loc[:, js.dtypes == 'int64'].sum())
            #     return(False)
            # # check for that categorical columns are equal
            # if(not all((xl.loc[:, xl.dtypes == 'object'].fillna('999') == js.loc[:, js.dtypes == 'object'].fillna('999')).all())):
            #     logger.warning('Data: Obj column had difference')
            #     logger.warning((xl.loc[:, xl.dtypes == 'object'].fillna('999') == js.loc[:, js.dtypes == 'object'].fillna('999')).all())
            #     return(False)
            #print("we got to the end")
            
            # ##Output the convert excel files
            # os.makedirs(os.path.join(out_dir_excel, round, date), exist_ok=True)
            # converted_excel_file = out_dir_excel + "/"+ round + "/" + date +  "/converted_" + file

            # ##Get columns in the same order as the original file
            # mjs = mjs.reindex(columns = self.meta_columns)
            # #print("mjs columns")
            # #print(mjs.columns)
            # js = js.reindex(columns =self.data_columns)
            # js['digest_table_year']= self.digest_table_year
            # js['digest_table_id']=self.digest_table_id
            
            #self.digest_table_id = excel['data']['digest_table_id']
            #self.digest_table_year = excel['data']['digest_table_year']
            #print(self.digest_table_year)

            ##Write to Excel file
            # Excelwriter = pd.ExcelWriter(converted_excel_file,engine="xlsxwriter")
            # mjs.to_excel(Excelwriter, sheet_name = 'meta',index=False)
            # js.to_excel(Excelwriter, sheet_name = 'data', index=False)
            # Excelwriter.save()
            
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
                        print(file)
                        if file == ".DS_Store":
                            continue
                        # read mrt 
                        # file = 'MRT_333_10.xlsx'
                        print('starting', round, date, file)
                    
                        try: # skip over summary files 
                            #Step 1: Convert excel to dataframe
                            mrt_xlsx = pd.read_excel(os.path.join(path,file), sheet_name=None, dtype= {'digest_table_id':object})
                        except: 
                            pass 
                        # convert dataframe to class object
                        mrt = mrtConvert(mrt_xlsx)
                        
                        # Step 2: Convert dataframe to dictionary 
                        mrt.processXLSX()
                        # Step 3 : Save dictionary as JSON output file
                        mrt.convertColumnTypes()
                        write_path = os.path.join(out_dir, round, date) 
                        # check if path exists 
                        if(not os.path.isdir(write_path)):
                            os.makedirs(os.path.join(out_dir, round, date))
                        afile = open(os.path.join(write_path, file[0:-5] + '.json'), 'w') # pop off xlsx
                        try: 
                            afile.write(json.dumps(mrt.json, indent=4, allow_nan = False,  ensure_ascii=False))
                        except: 
                            logger.warning('failed to write ' + round + '/' + date + '/' + file + ' to JSON')

                        #Step 4: Read JSON as dictionary
                        with open(os.path.join(write_path, file[0:-5] + '.json'), 'r') as openfile:
                            json_dict = json.load(openfile)
                            openfile.close()

                        # Step 5 and 6: Convert dictionary to dataframe and Compare Dataframe from step 1 and 5
                        outcome = mrt.checkConversion(round, date, file, json_dict)
                        
                        if outcome == True:
                            continue
                        else:
                            logger.warning("Files are not the same, please check where issue occured")
                            break
                        print(outcome)
                        #Step 6 : Compare df in step 1 to step 5
                        #check converstion
                        # if(mrt.checkConversion(round, date, file)):
                        # # else failed conversion QC
                        # else:
                        #     logger.warning('conversion for ' + round + '/' + date + '/' + file + ' failed')
                        
                        # check converstion
                        # if(mrt.checkConversion(round, date, file)):
                        #     mrt.convertColumnTypes()
                        #     #logger.warning('conversion for ' + round + '/' + date + '/' + file + ' suceeded')
                        #     #logger.warning('writing to JSON')
                            
                        #     # write to json
                        #     #To do: maybe make json writing part of the class, maybe add extra AIR subdirectory 
                        #     write_path = os.path.join(out_dir, round, date) # check if path exists 
                        #     if(not os.path.isdir(write_path)):
                        #         os.makedirs(os.path.join(out_dir, round, date))
                        #     afile = open(os.path.join(write_path, file[0:-5] + '.json'), 'w') # pop off xlsx
                        #     try: 
                        #         afile.write(json.dumps(mrt.json, indent=4, allow_nan = False,  ensure_ascii=False))
                        #     except: 
                        #         logger.warning('failed to write ' + round + '/' + date + '/' + file + ' to JSON')
                                
                        #     afile.close()
                        # # else failed conversion QC
                        # else:
                        #     logger.warning('conversion for ' + round + '/' + date + '/' + file + ' failed')
    end = time.time()  
    total = end - start
    total = str(total)
    logger.warning('time in seconds='+ total)


if __name__=="__main__":
    main()

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
import re
import sys
from sortedcontainers import SortedSet

#debuggin
from pdb import set_trace as bp
import logging 
logging.basicConfig(filename='std.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logger=logging.getLogger('logger') 

# dirs
#TODO: might be nice to put this in a main function also maybe add dirs to a config file 
# Change these:
in_dir = '/Users/gchickering/Library/CloudStorage/OneDrive-AIR/Github/ARIS/mrt_to_JSON/MT_MRT'
out_dir = '/Users/gchickering/Library/CloudStorage/OneDrive-AIR/Github/ARIS/mrt_to_JSON/GC_JSON'
out_dir_excel = '/Users/gchickering/Library/CloudStorage/OneDrive-AIR/Github/ARIS/mrt_to_JSON/Excel_Conversion'
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
                    if value == '†':
                        new_dict['data'][row][key]= value
                    elif re.match(r'\d+\.0$', value):
                        value = re.sub(r'\.0$', '', value)
                        new_dict['data'][row][key]= value
                    else:
                        new_dict['data'][row][key]= value

                if key == 'value':
                    value= new_dict['data'][row][key]
                    value = str(value)
                    if value == '†':
                        new_dict['data'][row][key]= value
                    elif re.match(r'\d+\.0$', value):
                        value = re.sub(r'\.0$', '', value)
                        new_dict['data'][row][key]= value
                    else:
                        new_dict['data'][row][key]= value
                elif any(keyword in key for keyword in ['row_level', 'column_level']):
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
        #keep_meta = ['digest_table_id', 'digest_table_year']
        keep_meta2 = ['digest_table_id', 'digest_table_year', 'digest_table_sub_id']
        data_col_names = [i for i in data_col_names if i not in keep_meta2]
        mrt_meta = mrt_meta[mrt_meta.columns.difference(data_col_names)] # don't want data colnames in meta data 
        #data_col_names_data = list(mrt_data.columns)
        # data_col_names_meta = list(mrt_meta.columns)
        # keep_meta = ['digest_table_id', 'digest_table_year', 'digest_table_sub_id']
        # data_col_names = [i for i in data_col_names_meta if i not in keep_meta]
        # mrt_meta = mrt_meta[mrt_meta.columns.difference(data_col_names)] # don't want data colnames in meta data 

        contains_deflator = False
        if mrt_meta['deflator'].notnull().sum() != 0 :
            dict_deflator_values = dict(zip(mrt_meta.digest_table_sub_id, mrt_meta.deflator))
            mrt_meta= mrt_meta.drop(columns = ['deflator','digest_table_sub_id'])
            contains_deflator = True

        new_dict['meta'] = mrt_meta.to_dict(orient='index')[0]
        new_dict['data'] = "null"

        # more column cleanup
        keep_data = ['digest_table_id', 'digest_table_year']
        mrt_data = mrt_data[mrt_data.columns.difference(keep_data)]
        if contains_deflator == True:
            print("contains deflator")
            mrt_data["deflator"] = mrt_data['digest_table_sub_id'].map(dict_deflator_values)
            # print(mrt_data)
            constant = False
            for index, row in mrt_data.iterrows():
                constant = False
                for column in mrt_data.columns:
                        if column =='column_level' or column == 'digest_table_sub_title' or column == "row_level" :
                            cell_value = str(row[column])
                            if re.search(r'\bconstant\b', cell_value) or re.search(r'\badjusted\b', cell_value) or re.search(r'\bConstant\b', cell_value) or re.search(r'\bAdjusted\b', cell_value)  or re.search(r'\bChained\b', cell_value)  or re.search(r'\bchained\b', cell_value): 
                                constant = True
                                break
                if constant == False:
                    mrt_data.loc[index, "deflator"] =  None
                    

        
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
            mrt_meta = mrt_meta.dropna(axis=1, how='all')
            data_col_names = [i for i in data_col_names if i in ['digest_table_sub_id','digest_table_sub_title', 'digest_table_sub_title_note']]
            mrt_meta = mrt_meta[mrt_meta.columns.difference(data_col_names)] # don't want data colnames in meta data 
            # mrt_meta = mrt_meta.dropna(axis=1, how='all')
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
            mrt_data['value'] = mrt_data['value'].astype(str)

            if "standard_error" in mrt_data.columns:
                mrt_data['standard_error'] = mrt_data['standard_error'].astype(str)
            
            keywords = ['row_level', 'column_level']
            columns_to_convert = [col for col in mrt_data.columns if any(keyword in col for keyword in keywords)]
            mrt_data[columns_to_convert] = mrt_data[columns_to_convert].astype(str)
            
           
               


            #Meta Data from json file for step 5 
            json_df_meta = pd.json_normalize(json_dict, meta = ['meta'])
            json_df_meta.columns = json_df_meta.columns.str.replace(r'^meta.', '', regex=True)
            json_df_meta =json_df_meta.drop(['data'], axis = 1)
            

            #Regular Data from json file
            json_df_data = pd.json_normalize(json_dict['data'])
            if "standard_error" in json_df_data.columns:
                json_df_data["standard_error"] = json_df_data["standard_error"].astype(str)
            json_df_data["value"] = json_df_data["value"].astype(str)

            if deflator_check == True:
                if 'deflator' in json_df_data.columns:
                    mrt_data["deflator"] = json_df_data['deflator']
               

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

            ##reorder columns for both dataframes so they are in the same order
            mrt_data = mrt_data.reindex(sorted(mrt_data.columns), axis=1) # sort for ording when comparing 
            json_df_data = json_df_data.reindex(sorted(json_df_data.columns), axis=1)
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
                logger.warning('Data: Different number of rows. Returning False.')
                return(False)

             # content Check
            mrt_data = mrt_data.reindex(sorted(mrt_data.columns), axis=1) # sort for ording when comparing 
            json_df_data = json_df_data.reindex(sorted(json_df_data.columns), axis=1)
            mrt_data = mrt_data.reset_index(drop=True)
            json_df_data  = json_df_data.reset_index(drop=True)
            
            # print(mrt_data.loc[:, mrt_data.dtypes == 'float64'].columns)
            # print(json_df_data.loc[:, json_df_data.dtypes == 'float64'].columns)
            if(not all(mrt_data.loc[:, mrt_data.dtypes == 'float64'].sum() == json_df_data.loc[:, json_df_data.dtypes == 'float64'].sum())):
                logger.warning('Data: Floating point valuas do not match')
                logger.warning(mrt_meta.loc[:, mrt_meta.dtypes == 'float64'].sum() == json_df_data.loc[:, json_df_data.dtypes == 'float64'].sum())
                return(False)
            # ints, these appear to typically be year values 
            if(not all(mrt_data.loc[:, mrt_data.dtypes == 'int64'].sum() == json_df_data.loc[:, json_df_data.dtypes == 'int64'].sum())):
                logger.warning('Data: Int valuas do not match')
                logger.warning(mrt_data.loc[:, mrt_data.dtypes == 'int64'].sum() == json_df_data.loc[:, json_df_data.dtypes == 'int64'].sum())
                return(False)

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



# Function to check if a value ends with ".0"
def ends_with_point_zero(value):
    if isinstance(value, str) and value.endswith(".0"):
        return True
    if isinstance(value, (int, float)):
        if int(value) == value and (isinstance(value, int) or value.is_integer()):
            return True
    return False

# Function to update values that end with ".0"
def update_value(value):
    if ends_with_point_zero(value):
        if isinstance(value, float):
            return int(value) 
        elif isinstance(value, str):
            value = value[:-2]
            return value
    return value


# Recursive function to update values in nested dictionaries and lists
def update_json(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            obj[key] = update_value(value)
            update_json(value)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            obj[i] = update_value(item)
            update_json(item)
   



def check_keys(data_dict, level_key, max_key_value, missing_keys, extra_keys):
    level_value = data_dict.get(level_key, '')
    count = len(re.findall(r':::', level_value)) + 1
    # if level_key =='row_level':
    #     count += 1
    
    for i in range(count):
        current_key = f"{level_key}_{i + 1}"
        if current_key in data_dict:
            #print(f"There is a '{current_key}' key in the sub-dictionary.")
            continue
        else:
            #print(f"KEY MISSING: There is no '{current_key}' key in the sub-dictionary .")
            missing_keys.add(data_dict['column_index'])

    for i in range(count+1, max_key_value):
        current_key = f"{level_key}_{i}"
        if current_key in data_dict:
            print(f"EXTRA KEY: There is a '{data_dict['column_index']}' extra key, '{current_key}',  key in the sub-dictionary.")
            extra_keys.add(data_dict['column_index'])
            
        else:
            continue
            #print(f"There is no '{current_key}' key in the sub-dictionary.")
    return missing_keys, extra_keys

# QC Checks to Make sure File is behaving properly
def qc_json(obj, filename, qc_fail_list):
    max_key_value = 5
    missing_keys = set()
    extra_keys = set()
    qc_fail = False
    data_list = obj.get("data", [])
    for data_dict in data_list:
        missing_keys, extra_keys = check_keys(data_dict, 'row_level', max_key_value, missing_keys, extra_keys )
        missing_keys, extra_keys = check_keys(data_dict, 'column_level', max_key_value, missing_keys, extra_keys )

    missing_keys = SortedSet(missing_keys)
    extra_keys = SortedSet(extra_keys)
    if len(missing_keys) > 0:
        print(f'Here are the keys that are missing a column or row level: {missing_keys}')
        qc_fail = True
    if len(extra_keys) > 0:
        print(f'Here are the keys that have an extra column or row level: {extra_keys}')
        qc_fail = True

    if qc_fail:
        qc_fail_list.add(filename)
        
    return qc_fail_list


# execution -----------------------------------------------------------------------------------------------
def main():
    start = time.time() # time 

    # for round of MRT
    print(os.listdir(in_dir))
    for round in os.listdir(in_dir):
        if re.match('Round\d', round):
            round_dir = os.listdir(os.path.join(in_dir,round))
            # round = "Round2" 
            # for Date in MRT
            for date in round_dir:
                #print(date)
                if re.match('\d{4}-\d{2}-\d{2}', date):
                    # date = '2021-07-06'  
                    path = os.path.join(in_dir,round,date)
                    date_dir = os.listdir(path)
                    for file in date_dir:
                        print(file)
                        if file == ".DS_Store":
                            continue
                        # # read mrt 
                        # # file = 'MRT_333_10.xlsx'
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

    ##QC Check to Make sure there are no ".0" values in the json files
    qc_fail_list = set()
    for filename in os.listdir(write_path):
    # Check if the file is a JSON file (ends with ".json")
        if filename.endswith('.json'):
            # Construct the full file path
            file_path = os.path.join(write_path, filename)
            print(file_path)
            # Open and read the JSON file
            with open(file_path, 'r') as json_file:
                data = json.load(json_file)
                
                # Now you can work with the JSON data as needed
                # For example, you can print it:
                update_json(data)
                qc_fail_list = qc_json(data, filename, qc_fail_list)
                #print(data['meta'])

                # Now you can overwrite the old JSON file with the modified data
                with open(file_path, 'w') as json_file:
                    json.dump(data, json_file, indent=4, allow_nan = False,  ensure_ascii=False)  # You can specify the indentation level as needed

    print(f'Here are the files that failed the QC Check: "{SortedSet(qc_fail_list)}"')

    end = time.time()  
    total = end - start
    total = str(total)
    logger.warning('time in seconds='+ total)




if __name__=="__main__":
    main()

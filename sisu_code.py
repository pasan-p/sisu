#
# SISU coding assesment
#
# Author: Pasan Punchihewa
# Date: 18/09/2021
# version: 1.0
#

import hashlib
import findspark
findspark.init()
from pyspark.sql.functions import regexp_extract, trim, lit 
import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_extract, trim, lit ,pandas_udf
import pandas as pd
from datetime import datetime


#getFiles function will loop through data and create seperate .json files

def getFiles(df,ct,st,en):

        for i in range(1,ct+2):

            df = df.iloc[ st:en , : ]
            name = i
            filename = str(name) + ".json"

            st = en + 1
            en = en + 1000
            # Change below directory path as required
            df.to_json("D:/Work/" + filename, orient = 'records')
        
        


def main():
    st = 0
    en = 1000
    #Change below directory path to where the file is located
    df = pd.read_csv("D:/Work/Transaction.csv")
    
    #Create new hash column with Account_ID 
    df['new_col'] = hashlib.md5('Account_ID '.encode('utf-8')).hexdigest()

    #Get Account_IDs with decimal values
    #Assumption: Account_IDs with decimal values are invalid records
    #Invalid records filtered out
    reject_df = df.loc[df['Account_ID ']%1 != 0 ]
    print("Invalid Account_ID records")
    print(reject_df)
    
    #Get Account_IDs without decimal values
    #Assumption: Account_IDs with decimal values are invalid records
    df = df.loc[df['Account_ID ']%1 == 0 ]
    
    #Count records and divide by 1000 to get how many files needed when splitup into 1000 record files
    count_row = df.shape[0]

    ct = count_row // 1000
    
    #call getFiles to generate multiple .json files
    #Pass data frame, count of files, start index and end index values
    getFiles(df,ct,st,en)

        
    #df5 below is to get dataframe for processing list of post codes based on fastest response
    #Sample of 50 records are selected for this task

    df5 = pd.DataFrame(d, columns = ['Implemented Date ', 'Request Date ','Post Code '])

    df5 = df5.head(50)
    
    #Convert string to date before calculate difference in days
    df5['dates1'] = pd.to_datetime(df5['Implemented Date '], format='%d/%m/%Y %H:%M')

    df5['dates2'] = pd.to_datetime(df5['Request Date '], format='%d/%m/%Y %H:%M')

    df5['difference_in_days'] = abs(df5['dates1'] - df5['dates2'])

    df5 = df5.sort_values('difference_in_days', ascending=False)
    
    print("List of post codes based on fastest response")
    print(df5)

    #df6 below is to get dataframe for listing Agents based on amount
    #sample of 10000 records are selected for this task
    
    df6 = pd.DataFrame(d, columns = ['Agent ID ', 'Post Code ','$ Amount '])
    df6.rename(columns={'$ Amount ': 'Amount'}, inplace=True)

    df6 = df6.head(10000)
    
    df6 = df6.groupby(['Agent ID ','Post Code '])['Amount'].sum().reset_index().sort_values('Amount', ascending=False)

    print("Agents based on Amount:")
    print(df6)

if __name__ == "__main__":
 main()

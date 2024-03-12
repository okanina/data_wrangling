import sqlite3
from sqlite3 import Error
import pandas as pd
import numpy as np
import config
from glob import glob

connection=sqlite3.connect(config.db_name)


def split_column(Contract):
    if "On Loan" in Contract or Contract=="Free":
        start_date=np.nan
        end_date=np.nan
        contract_length=0
    else:
        start_date, end_date=Contract.split("~")
        start_date=int(start_date[:4])
        end_date=int(end_date[:5])
        contract_length= end_date - start_date
            
    return start_date, end_date, contract_length


def convert_height(height):
    if "cm" in height:
        return int(height.strip('cm'))
    else:
        feet, inches=height.split("'")
        return round(int(feet)*30.48 + int(inches.split('"')[0])*2.54)

def convert_weight(weight):
    if "kg" in weight:
        return weight.replace("kg", "")
    else:
        pound= round(int(weight.strip("lbs")) /2.205)
        return pound

def wrangle(filepath):
     
    #read data into dataframe
    df=pd.read_csv(filepath, low_memory=False)

    pd.set_option("display.max_columns", None)

    #remove '\n' from column 'Club'
    df["Club"]=df["Club"].str.replace("\n", "")

    # removing '★' from column 'W/F', 'IR' and 'SM' and converting the dtype
    df["W/F"]=df["W/F"].str.replace("★", "").astype(int)
    df.SM=df.SM.str.replace("★", "").astype(int)
    df.IR=df.IR.str.replace("★", "").astype(int)

    #cleaning 'contract'
    new_cols=["start_date", "end_date"]
    new_data=df.Contract.apply(lambda x:pd.Series(split_column(x)))

    for i in range(len(new_cols)):
       df.insert(loc=df.columns.get_loc('Contract')+1 +i, column=new_cols[i], value=new_data[i])   

    #cleaning column 'height'
    df["Height"]=df["Height"].apply(lambda x: convert_height(x))

    # renaming the new column to show units
    df=df.rename(columns={"Height":"Height (cm)"})  

    df["Weight"]= df["Weight"].apply(lambda x:convert_weight(x))
    df=df.rename(columns={"Weight":"Weight (kg)"}) # rename col
        
    df["Value"]=df["Value"].replace({"€":"", " ":"", "K":"e+03", "M":"e+06"}, regex=True).astype(float).astype(int)

    df["Wage"]= df["Wage"].replace({"€":"", "K":"e+03"}, regex=True).astype(float).astype(int)

    n_inserted=df.to_sql("FIFA21", con=connection, if_exists="replace")

    return {"Transaction Successful": True, "Records Inserted":n_inserted}

report=wrangle(r"data\fifa21rawdata.csv")
print(report)
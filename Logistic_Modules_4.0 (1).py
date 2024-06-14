# Databricks notebook source
# MAGIC %md #Algorithm PreRequisites

# COMMAND ----------

pip install sympy

# COMMAND ----------

pip install pulp

# COMMAND ----------


import matplotlib.pyplot as plt
import statistics as stcs

import os
os.environ['PYTHONWARNINGS'] = 'ignore'
import warnings
warnings.simplefilter("ignore")

#import missingno as msno
import statsmodels as sm
#import dask.array as da
#import lightgbm as lgb
import seaborn as sns
import sympy as sym
import pandas as pd
import scipy as sci
import numpy as np
import warnings
import pyspark
import pulp
import random
import math
import csv
from sympy import *
from pulp import *


# COMMAND ----------

# MAGIC %md #Algorithm's Pre Stage

# COMMAND ----------


# Matching Data

Joint_Q4_Table_SO_SPARK = spark.sql("SELECT DISTINCT A.*, B.*, C.Clasificacion_tienda FROM cpfr_solution.tb_cpfr_si_pbi A INNER JOIN cpfr_solution.tb_cpfr_dim_products B ON A.PRODUCT_KEY = B.PRODUCT_KEY LEFT JOIN cpfr_solution.tb_cpfr_clasif_stores C ON A.RET_ID = C.RET_ID where New_order = 1")

display(Joint_Q4_Table_SO_SPARK)


# COMMAND ----------


# Partitioning by RETAILER we'll get the orders separated

Joint_Q4_Table_SO_SPARK = Joint_Q4_Table_SO_SPARK.orderBy(['RET_ID'], ascending = [True])
#display(Joint_Q4_Table_SO_SPARK)


# COMMAND ----------

# Pandas environment

Joint_Q4_Table_SO = Joint_Q4_Table_SO_SPARK.toPandas()


# COMMAND ----------

'''%sql
CREATE TABLE IF NOT EXISTS cpfr_solution.tb_cpfr_dim_products_backup DEEP CLONE cpfr_solution.tb_cpfr_dim_products'''

# COMMAND ----------


########################################### Taken from Mary's/Fabio's revision's code patch ###############################################

# Matching Data
Joint_Q4_Table_SO_SPARK = spark.sql("SELECT DISTINCT A.*, B.*, C.Clasificacion_tienda \
                                    FROM cpfr_solution.tb_cpfr_si_pbi A \
                                    INNER JOIN cpfr_solution.tb_cpfr_dim_products B ON A.PRODUCT_KEY = B.PRODUCT_KEY \
                                    LEFT JOIN cpfr_solution.tb_cpfr_clasif_stores C ON A.RET_ID = C.RET_ID \
                                    where New_order = 1")

# Partitioning by RETAILER we'll get the orders separated
Joint_Q4_Table_SO_SPARK = Joint_Q4_Table_SO_SPARK.orderBy(['RET_ID'], ascending = [True])

# Pandas environment
Joint_Q4_Table_SO = Joint_Q4_Table_SO_SPARK.toPandas()


# COMMAND ----------

Joint_Q4_Table_SO.head(10)

# COMMAND ----------

Joint_Q4_Table_SO.shape

# COMMAND ----------


# Remaining Columns Filtering

Joint_Q4_Table_SO_Copy = Joint_Q4_Table_SO.copy()

list_2_drop = []

for x in Joint_Q4_Table_SO_Copy.columns:

  NaNratio = Joint_Q4_Table_SO_Copy[x].isna().sum()/len(Joint_Q4_Table_SO_Copy) * 100

  if 'purina' in x or 'Purina' in x or 'supply' in x: 
   
       list_2_drop.append(x)

print('\n')
print(list_2_drop)
print('\n')


Joint_Q4_Table_SO_Copy = Joint_Q4_Table_SO_Copy.drop(list_2_drop, axis = 1) 

Joint_Q4_Table_SO_Copy = pd.DataFrame(Joint_Q4_Table_SO_Copy)


list_3_drop = []

for x in Joint_Q4_Table_SO_Copy.columns:

    if (x != "SKU_ID") and (x != "PRODUCT_KEY") and (x != "RET_ID"):
      
      NaNratio = float(Joint_Q4_Table_SO_Copy[x].isna().sum()/len(Joint_Q4_Table_SO_Copy) * 100)

      if NaNratio > 75:
            print(x, NaNratio)
            list_3_drop.append(x)  

print('\n')
print(list_3_drop)
print('\n')

Joint_Q4_Table_SO_Copy = Joint_Q4_Table_SO_Copy.drop(list_3_drop, axis = 1)
Joint_Q4_Table_SO_Copy = pd.DataFrame(Joint_Q4_Table_SO_Copy)
Joint_Q4_Table_SO_Copy


# COMMAND ----------


#List of remaining columns

List_Uniques = []
list_2_Drop = []

for y in Joint_Q4_Table_SO_Copy.columns:
    
    if (y not in List_Uniques) and (len(Joint_Q4_Table_SO[[y]].columns) == 1):
        List_Uniques.append(y)
    else:
        if y not in list_2_Drop:
         list_2_Drop.append(y)
        list_2_Drop.append(y)


# COMMAND ----------


#Reduced DataFrame

columns_list = []
synthesized_dataframe = pd.DataFrame()

Working_Frame = Joint_Q4_Table_SO_Copy.copy()


for j, x in enumerate(sorted(List_Uniques), 1):   # Avoiding the repeated dataframe columns

  if len(Working_Frame[[x]].columns) == 1:
  
   columns_list.append(Working_Frame[x])
   synthesized_dataframe = synthesized_dataframe.join(Working_Frame[x], how = 'right')
  
  
  else:
  
    if x == 'PRODUCT_KEY':
      len_list = []
      old_list_names_x = []
      new_list_names_x = []
      df_x = Working_Frame[[x]].copy()

      for j,x in enumerate(df_x.columns, 1):   #renaming columns all with the same name
        
        old_list_names_x.append(x)
        df_x[x]
        y = f"{x}{'_'}{j}"
        print(y)
        new_list_names_x.append(y)
        len_list.append(j-1)

      df_x.set_axis(len_list, axis=1, inplace = True) #removes columns names, set instead columns numbers

      for i in len_list:
        df_x = df_x.rename(columns = {i : new_list_names_x[i]} , inplace = True)

      #df_x.drop([new_list_names_x[1]], axis=1, inplace = True)
      
      synthesized_dataframe = synthesized_dataframe.join(df_x, how = 'right')

    if x == 'SKU_ID':
      
      len_list = []
      old_list_names_x = []
      new_list_names_x = []
      df_x = Working_Frame[[x]].copy()

      for j,x in enumerate(df_x.columns, 1):   #renaming columns all with the same name
        
        old_list_names_x.append(x)
        df_x[x]
        y = f"{x}{'_'}{j}"
        print(y)
        new_list_names_x.append(y)
        len_list.append(j-1)

      df_x.set_axis(len_list, axis=1, inplace = True) #removes columns names, set instead columns numbers

      for i in len_list:
        df_x = df_x.rename(columns = {i : new_list_names_x[i]} , inplace = True)
        
      #df_x.drop([new_list_names_x[1]], axis=1, inplace = True)
      
      synthesized_dataframe = synthesized_dataframe.join(df_x, how = 'right')

synthesized_dataframe.head(5)


# COMMAND ----------


for j, x in enumerate(synthesized_dataframe.columns, 1):
    print(j, '_', x)
print('\n')


# COMMAND ----------

synthesized_dataframe[['InvOpt', 'InvProy', 'New_order', 'OO']].head(5)

# COMMAND ----------

# MAGIC %md #MILESTONE & STARTING POINT FROM STORED DF IN DATABRICKS TABLE FORMAT

# COMMAND ----------


#STORED DF CALL
df_pivot_1 = spark.table("hive_metastore.default.synthesized_dataframe_spark")
synthesized_dataframe = df_pivot_1.toPandas()
synthesized_dataframe


# COMMAND ----------


#STORED DF CALL
df_pivot_1 = spark.table("fragment_1_df")
synthesized_dataframe = df_pivot_1.toPandas()
synthesized_dataframe


# COMMAND ----------


# RETAILER'S LOGISTIC DATAFRAME EXTRACTION

LIST_RETAILERS = []

for j,x in enumerate(sorted(synthesized_dataframe['RET_ID'].unique()), 1):
 
 if j < 1024:
  LIST_RETAILERS.append(x)

dbutils.widgets.dropdown("RETAILER", LIST_RETAILERS[0], LIST_RETAILERS)

RETAILER_ID = dbutils.widgets.get("RETAILER")

#dbutils.widgets.removeAll()


# COMMAND ----------


#Taking to ceiling integer the "Order_Size" column

synthesized_dataframe_Copy = synthesized_dataframe.copy()
synthesized_dataframe_Copy['Order_Size'] = synthesized_dataframe_Copy['Order_Size'].apply(np.ceil, convert_dtype=True, args=())
synthesized_dataframe_Copy['Order_Size'] = synthesized_dataframe_Copy['Order_Size']
synthesized_dataframe['Order_Size_Ceil_Value'] = synthesized_dataframe_Copy['Order_Size']
Column_1 = pd.DataFrame(synthesized_dataframe['Order_Size'])
Column_2 = pd.DataFrame(synthesized_dataframe['Order_Size_Ceil_Value'])
Comparison_df = Column_1.join(Column_2, how = 'left')
Comparison_df.head(5)


# COMMAND ----------


#synthesized_dataframe['Fecha_inicio'] = pd.to_datetime(synthesized_dataframe['Fecha_inicio'])
synthesized_dataframe['Fecha_fin'] = pd.to_datetime(synthesized_dataframe['Fecha_fin'])


# COMMAND ----------


for j,x in enumerate(synthesized_dataframe.columns, 1):
    print(j, '_', x,  '----- (', (synthesized_dataframe[x].dtype), ')')
print('\n')


# COMMAND ----------

#what was loaded as synthesized_dataframe
display(df_pivot_1)

# COMMAND ----------


#Retailers Orders details (REDUCED FRAME - TESTS PURPOSES)

synthesized_dataframe['CS_UNITS'] = synthesized_dataframe['CS_UNITS'].astype('float64') 
synthesized_dataframe['CS_VOLUME'] = synthesized_dataframe['CS_VOLUME'].astype('float64')
synthesized_dataframe['CS_GROSS_WEIGHT'] = synthesized_dataframe['CS_GROSS_WEIGHT'].astype('float64')
synthesized_dataframe['CS_WEIGHT_UNIT'] = synthesized_dataframe['CS_WEIGHT_UNIT'].astype('str')

MAX_VOLUME_Refrig = 90000   #liters
MAX_WEIGHT_Refrig = 28000   #Kg

MAX_VOLUME_NoN_Refrig = 90000   #liters
MAX_WEIGHT_NoN_Refrig = 28000   #Kg

Block_df = pd.DataFrame()
Boxes_df_Refrig = pd.DataFrame()
Boxes_df_NoN_Refrig = pd.DataFrame()


for j,x in enumerate(sorted(synthesized_dataframe['RET_ID'].unique()), 1):
  
  if j <= 100:
    
    print(j)

    Boxes_df = synthesized_dataframe[['RET_ID', 'SC', 'SC_W', 'LT', 'DemandaHastaLTSC','DemandaHastaLTSC_Dias','Fecha_fin','idProduct', 'Order_Size','Order_Size_Ceil_Value','CS_DIMENSION_UNIT', 'CS_HEIGHT', 'CS_LENGTH', 'CS_GROSS_WEIGHT', 'CS_STACKABLE', 'CS_SUPPORTED_WEIGHT', 'CS_UNITS', 'CS_VOLUME', 'CS_VOLUME_UNIT', 'CS_WEIGHT_UNIT', 'CS_WIDTH', 'DESCRIPCION_DSC', 'GRUPO_NEGOCIO', 'categorianestec', 'tasty']].loc[synthesized_dataframe['RET_ID'] == x].sort_values('idProduct')  
    

    #Separating into Refrigerated and Non-Refrigerated
    Boxes_df_Refrig = Boxes_df.loc[(Boxes_df['categorianestec'] == 'Chocolate') | (Boxes_df['categorianestec'] == 'AMBIENT DAIRY')]
    Boxes_df_NoN_Refrig = Boxes_df.loc[(Boxes_df['categorianestec'] != 'Chocolate') & (Boxes_df['categorianestec'] != 'AMBIENT DAIRY')]
    

    if len(Boxes_df_Refrig) > 0:
      
      #Cataloging of the case
      Boxes_df_Refrig['REFRIGERATED'] = str('yes')  
      Boxes_df_Refrig['NON-REFRIGERATED'] = str('no')

      #Re-scaling into [Kg] 
      Boxes_df_Refrig.loc[Boxes_df_Refrig['CS_WEIGHT_UNIT'] == 'G', 'CS_GROSS_WEIGHT'] = Boxes_df_Refrig['CS_GROSS_WEIGHT'].loc[Boxes_df_Refrig['CS_WEIGHT_UNIT'] == 'G'] / 1000

       #Re-scaling into [liters == CDM (Cubic Decimeters)]
      Boxes_df_Refrig.loc[Boxes_df_Refrig['CS_VOLUME_UNIT'] == 'CCM', 'CS_VOLUME'] = Boxes_df_Refrig['CS_VOLUME'].loc[Boxes_df_Refrig['CS_VOLUME_UNIT'] == 'CCM'] / 1000
      
      #Resetting Weight Units
      Boxes_df_Refrig['CS_WEIGHT_UNIT'] = Boxes_df_Refrig['CS_WEIGHT_UNIT'].replace(str('G'),str('KG'))

      #Resetting Volume Units
      Boxes_df_Refrig['CS_VOLUME_UNIT'] = Boxes_df_Refrig['CS_VOLUME_UNIT'].replace(str('CCM'),str('CDM'))
      
      #Defining the New Columns
      Boxes_df_Refrig['Boxes_Order_Size'] = Boxes_df_Refrig['Order_Size_Ceil_Value'] / Boxes_df_Refrig['CS_UNITS']
      Boxes_df_Refrig['Order_Size_#_Boxes'] = Boxes_df_Refrig['Order_Size_Ceil_Value'] #Boxes_df_Refrig['Boxes_Order_Size'].apply(np.ceil, convert_dtype=True, args=())
      
      #Refrigerated Cases
      Boxes_df_Refrig['Equivalent_Order_Volume/Product [ltr]'] = Boxes_df_Refrig['Order_Size_#_Boxes'] * Boxes_df_Refrig['CS_VOLUME']
      Boxes_df_Refrig['Equivalent_Order_Weight/Product [Kg]'] = Boxes_df_Refrig['Order_Size_#_Boxes'] * Boxes_df_Refrig['CS_GROSS_WEIGHT']

      Boxes_df_Refrig['Equivalent_Order_Volume [ltr]'] = sum(Boxes_df_Refrig['Equivalent_Order_Volume/Product [ltr]'].values)
      Boxes_df_Refrig['Equivalent_Order_Weight [Kg]'] = sum(Boxes_df_Refrig['Equivalent_Order_Weight/Product [Kg]'].values)

      Boxes_df_Refrig['Truck_Volume_Completeness_%'] = Boxes_df_Refrig['Equivalent_Order_Volume [ltr]'] / MAX_VOLUME_Refrig * 100
      Boxes_df_Refrig['Truck_Weight_Completeness_%'] = Boxes_df_Refrig['Equivalent_Order_Weight [Kg]'] / MAX_WEIGHT_Refrig * 100
      
      Block_df = pd.concat([Block_df, Boxes_df_Refrig])

      #Boxes_df_Refrig = pd.DataFrame()


    if len(Boxes_df_NoN_Refrig) > 0:

      #Cataloging of the case
      Boxes_df_NoN_Refrig['REFRIGERATED'] = str('no')  
      Boxes_df_NoN_Refrig['NON-REFRIGERATED'] = str('yes')

      #Re-scaling into [Kg]
      Boxes_df_NoN_Refrig.loc[Boxes_df_NoN_Refrig['CS_WEIGHT_UNIT'] == 'G', 'CS_GROSS_WEIGHT'] = Boxes_df_NoN_Refrig['CS_GROSS_WEIGHT'].loc[Boxes_df_NoN_Refrig['CS_WEIGHT_UNIT'] == 'G'] / 1000
      
      #Re-scaling into [liters == CDM (Cubic Decimeters)]
      Boxes_df_NoN_Refrig.loc[Boxes_df_NoN_Refrig['CS_VOLUME_UNIT'] == 'CCM', 'CS_VOLUME'] = Boxes_df_NoN_Refrig['CS_VOLUME'].loc[Boxes_df_NoN_Refrig['CS_VOLUME_UNIT'] == 'CCM'] / 1000

      #Resetting Weight Units
      Boxes_df_NoN_Refrig['CS_WEIGHT_UNIT'] = Boxes_df_NoN_Refrig['CS_WEIGHT_UNIT'].replace(str('G'),str('KG'))
      
      #Resetting Volume Units
      Boxes_df_NoN_Refrig['CS_VOLUME_UNIT'] = Boxes_df_NoN_Refrig['CS_VOLUME_UNIT'].replace(str('CCM'),str('CDM'))

      #Defining the New Columns
      Boxes_df_NoN_Refrig['Boxes_Order_Size'] = Boxes_df_NoN_Refrig['Order_Size_Ceil_Value'] / Boxes_df_NoN_Refrig['CS_UNITS']
      Boxes_df_NoN_Refrig['Order_Size_#_Boxes'] = Boxes_df_NoN_Refrig['Order_Size_Ceil_Value'] #Boxes_df_NoN_Refrig['Boxes_Order_Size'].apply(np.ceil, convert_dtype=True, args=())   

      #NoN-Refrigerated Cases
      Boxes_df_NoN_Refrig['Equivalent_Order_Volume/Product [ltr]'] = Boxes_df_NoN_Refrig['Order_Size_#_Boxes'] * Boxes_df_NoN_Refrig['CS_VOLUME']
      Boxes_df_NoN_Refrig['Equivalent_Order_Weight/Product [Kg]'] = Boxes_df_NoN_Refrig['Order_Size_#_Boxes'] * Boxes_df_NoN_Refrig['CS_GROSS_WEIGHT'] 

      Boxes_df_NoN_Refrig['Equivalent_Order_Volume [ltr]'] = sum(Boxes_df_NoN_Refrig['Equivalent_Order_Volume/Product [ltr]'].values)
      Boxes_df_NoN_Refrig['Equivalent_Order_Weight [Kg]'] = sum(Boxes_df_NoN_Refrig['Equivalent_Order_Weight/Product [Kg]'].values)      

      Boxes_df_NoN_Refrig['Truck_Volume_Completeness_%'] = (Boxes_df_NoN_Refrig['Equivalent_Order_Volume [ltr]'] / MAX_VOLUME_NoN_Refrig) * 100
      Boxes_df_NoN_Refrig['Truck_Weight_Completeness_%'] = (Boxes_df_NoN_Refrig['Equivalent_Order_Weight [Kg]'] / MAX_WEIGHT_NoN_Refrig) * 100  

      Block_df = pd.concat([Block_df, Boxes_df_NoN_Refrig])
      
      #Boxes_df_NoN_Refrig = pd.DataFrame()

    elif (len(Boxes_df_Refrig) == 0) and ((len(Boxes_df_NoN_Refrig) == 0)):

      #Cataloging of the case
      Boxes_df_Refrig['REFRIGERATED'] = str('Null df')  
      Boxes_df_Refrig['NON-REFRIGERATED'] = str('Null df')


Block_df.head(60)


# COMMAND ----------


for j,x in enumerate(Block_df, 1):
    print(j, '_', x)
print('\n')


# COMMAND ----------

#TESTING
Block_df['tasty'].unique()

# COMMAND ----------

#TESTING
for x in Block_df['tasty'].unique():

  print('len (', x, ') type =', len(Block_df.loc[Block_df['tasty'] == x]))

print('\n')


# COMMAND ----------

#TESTING
#Dfs sizes depending on the REFRIGERATED or the NON-REFRIGERATED case

REFRIGERATED_COUNT = (Block_df['REFRIGERATED'] == 'yes').sum()
NON_REFRIGERATED_COUNT = (Block_df['NON-REFRIGERATED'] == 'yes').sum()

print(REFRIGERATED_COUNT)
print(NON_REFRIGERATED_COUNT)
print('\n')


# COMMAND ----------

Block_df.tail(60)

# COMMAND ----------

block_df_spark = spark.createDataFrame(Block_df)

# COMMAND ----------

#Removing special characters from columns' names - Mount Point Saving Format requirement

New_List_of_columns = []
Old_List_of_columns = []

for j, column in enumerate(sorted(block_df_spark.columns), 0):
    
    Old_List_of_columns.append(column)

    if '__' or '/' or '(' or '%' or ' ' or '\'' in column:
        column = column.replace('(', '_')
        column = column.replace(')', '_')
        column = column.replace(' ', '_')
        column = column.replace('%', 'PERCENT')
        column = column.replace('\'', '_')
        column = column.replace('__', '_')
        column = column.replace('-', '_')
        column = column.replace('/', '_')
        New_List_of_columns.append(column)
    
    #if ')' in column:
        #column = column.replace('(', '_')
        #column = column.replace(')', '_')
        #New_List_of_columns.append(column)
    
    else:
        New_List_of_columns.append(column)
    
    print(j+1, ')', Old_List_of_columns[j])
    print(j+1, ')', New_List_of_columns[j])
    print('\n')
    
   
    block_df_spark = block_df_spark.withColumnRenamed(Old_List_of_columns[j], New_List_of_columns[j])


# COMMAND ----------

display(block_df_spark)

# COMMAND ----------


#Table's saving
block_df_spark.write.saveAsTable("block_dataframe_spark")


# COMMAND ----------

# MAGIC %md #Load ordering restrictions inside the truck

# COMMAND ----------


#The first Calculation is for the Qi (Box Stack Weight/Height Limit for Product "i") 

# Variables considered:
# CS := Box

# 1) CS_Weihgt_support
# 2) CS_weight
# 3) Truck_Max_Weight
# 4) CS_Height
# 5) Truck_Max_Height
# 6) CS_Volume
# 7) Truck_Max_Volume

#This will be a threshold that will indicate the % of load from the Order, that will be possible for each truck
#After getting the Top Limits Constraints for each product "i", there is going to be an inequation being part of the Linear Programming #System


# COMMAND ----------


# Start point DataFrame = Block_df --> locking it by the 'idProduct' feature, so as to make the Qi threshold inherent to each Product 'i'

MAX_TRUCK_HEIGHT = 2800   #Assuming that the "CS_HEIHGT_UNIT" are milimeters: [mm], because of a praticity the truck's Height is setted in 
                          #[mm] --- both cases (Refrigerated and NoN-Refrigerated trucks) have the same Max Height
MAX_TRUCK_WIDTH = 2400    #[mm] ~ 2,4 [m]

MAX_TRUCK_WEIGHT = 28000  #[Kg] ~ 28 [tn]
MAX_TRUCK_VOLUME = 90000  #[liters] ~ 90 [m3]

Block_df_Copy = Block_df.copy()

Block_df_Copy['CS_SUPPORTED_WEIGHT'] = Block_df_Copy['CS_SUPPORTED_WEIGHT'].astype('float64') #Columns' Casting 
Block_df_Copy['CS_LENGTH'] = Block_df_Copy['CS_LENGTH'].astype('float64')
Block_df_Copy['CS_WIDTH'] = Block_df_Copy['CS_WIDTH'].astype('float64')
Block_df_Copy['CS_HEIGHT'] = Block_df_Copy['CS_HEIGHT'].astype('float64')
Block_df_Copy['CS_GROSS_WEIGHT'] = Block_df_Copy['CS_GROSS_WEIGHT'].astype('float64')

All_Qi_df = pd.DataFrame()

for x in Block_df_Copy['idProduct'].unique():
 

     piece_df = Block_df_Copy.loc[Block_df_Copy['idProduct'] == x]
     

     piece_df['MAX_TRUCK_WEIGHT [Kg]'] = 28000 
     piece_df['MAX_TRUCK_VOLUME [m3]'] = 90 
     piece_df['MAX_TRUCK_WIDTH [m]'] = 2.4 
     piece_df['MAX_TRUCK_HEIGHT [m]'] = 2.8
     piece_df['MAX_TRUCK_LENGTH_Refrig [m]'] = 15 
     piece_df['MAX_TRUCK_LENGTH_NoN_Refrig [m]'] = 16 
  

     piece_df['Max_#_Boxes_MAX_TRUCK_WEIGHT'] = (piece_df['MAX_TRUCK_WEIGHT [Kg]'] / piece_df['CS_GROSS_WEIGHT']).apply(np.floor, convert_dtype=True, args=())

     piece_df['Max_#_Boxes_MAX_TRUCK_VOLUME'] = (1000 * piece_df['MAX_TRUCK_VOLUME [m3]'] / piece_df['CS_VOLUME']).apply(np.floor, convert_dtype=True, args=())  # CS_VOLUME [ltr] ~ [dm3]

     piece_df['Max_#_Boxes_in_Stack_WEIGHT'] = (piece_df['CS_SUPPORTED_WEIGHT'] / piece_df['CS_GROSS_WEIGHT']).apply(np.floor, convert_dtype=True, args=())   # (#_Boxes) on stack due to Box Weight
                                                                                                         
     piece_df['Max_#_Boxes_in_Stack_HEIGHT'] = (1000 * piece_df['MAX_TRUCK_HEIGHT [m]'] / piece_df['CS_HEIGHT']).apply(np.floor, convert_dtype=True, args=()) # (#_Boxes) on stack 'cause Box Height

     
     if (piece_df['categorianestec'].values[0] == 'Chocolate') or (piece_df['categorianestec'].values[0] == 'AMBIENT DAIRY'):
      
       LENGTH = piece_df['MAX_TRUCK_LENGTH_Refrig [m]']
       WIDTH =  piece_df['MAX_TRUCK_WIDTH [m]']
       
       piece_df['Max_#_Boxes/Product_Truck_s_Layer'] = (piece_df['Max_#_Boxes_MAX_TRUCK_VOLUME'] / piece_df['Max_#_Boxes_in_Stack_HEIGHT']).apply(np.floor, convert_dtype=True, args=())   #[ltr]

       #piece_df['Max_#_Boxes/Product_Truck_s_Layer'] = ((piece_df['MAX_TRUCK_VOLUME [m3]'] / piece_df['CS_VOLUME']) / (piece_df['MAX_TRUCK_HEIGHT [m]'] / piece_df['CS_HEIGHT'])).apply(np.floor, convert_dtype=True, args=())   #[ltr]


     elif (piece_df['categorianestec'].values[0] != 'Chocolate') and (piece_df['categorianestec'].values[0] != 'AMBIENT DAIRY'):
       
       LENGTH = piece_df['MAX_TRUCK_LENGTH_NoN_Refrig [m]']
       WIDTH =  piece_df['MAX_TRUCK_WIDTH [m]']  

       piece_df['Max_#_Boxes/Product_Truck_s_Layer'] = (piece_df['Max_#_Boxes_MAX_TRUCK_VOLUME'] / piece_df['Max_#_Boxes_in_Stack_HEIGHT']).apply(np.floor, convert_dtype=True, args=()) #[ltr]  Restriction & Possibility as a Geometrical 
                                                                                     #       matter

       #piece_df['Max_#_Boxes/Product_Truck_s_Layer'] = ((piece_df['MAX_TRUCK_VOLUME [m3]'] / piece_df['CS_VOLUME']) / (piece_df['MAX_TRUCK_HEIGHT [m]'] / piece_df['CS_HEIGHT'])).apply(np.floor, convert_dtype=True, args=())   #[ltr]

     if piece_df['CS_SUPPORTED_WEIGHT'].values[0] == 0:
       
       j = 1

       piece_df['Max_Number_Of_Layers_Allowed_for_Product_i'] = j
       print("j =", j)

       piece_df['Max_#_Boxes_on_Truck_of_Product_i'] = piece_df['Max_#_Boxes/Product_Truck_s_Layer']


     elif (0 < piece_df['CS_SUPPORTED_WEIGHT'].values[0]):   #In case there exists a Supported Weight for each Product "i" box, see
                                                         #how many layers of product are possible on the truck
       
       j = 1
      
       while ((0 < (piece_df['CS_GROSS_WEIGHT'].values[0] * j) <= piece_df ['CS_SUPPORTED_WEIGHT'].values[0])) and (j <= (piece_df['Max_#_Boxes_in_Stack_HEIGHT'].values[0] - 1)): #Considering Height and incremental Weight for the Stack (how many product layers)
         j = j + 1
       
       piece_df['Max_Number_Of_Layers_Allowed_for_Product_i'] = j
       print("j =", j)
       
       piece_df['Max_#_Boxes_on_Truck_of_Product_i'] = piece_df['Max_#_Boxes/Product_Truck_s_Layer'] * j
      
       if j < piece_df['Max_#_Boxes_in_Stack_HEIGHT'].values[0]:
         
        print('j =', j, 'and Max_Layers =', piece_df['Max_#_Boxes_in_Stack_HEIGHT'].values[0])
        print('\n')


     elif(piece_df['CS_SUPPORTED_WEIGHT'].values[0] <= piece_df['CS_GROSS_WEIGHT'].values[0] * (piece_df['Max_#_Boxes_in_Stack_HEIGHT'].values[0])):
       
       print('Qi*')
       print(piece_df['CS_SUPPORTED_WEIGHT'].values[0])
       print(piece_df['CS_GROSS_WEIGHT'].values[0] * (piece_df['Max_#_Boxes_in_Stack_HEIGHT'].values[0] - 1))
       print(piece_df['CS_GROSS_WEIGHT'].values[0])
       
       BOXES_NUMBER = (piece_df['CS_SUPPORTED_WEIGHT'] / piece_df['CS_GROSS_WEIGHT']).apply(np.floor, convert_dtype = True, args = ()) * piece_df['Max_#_Boxes/Product_Truck_s_Layer'].values[0]
       
       if BOXES_NUMBER.values[0] < piece_df['Max_#_Boxes_MAX_TRUCK_WEIGHT'].values[0]:
        
          piece_df['Max_#_Boxes_on_Truck_of_Product_i'] = BOXES_NUMBER
       
       else:
         
          piece_df['Max_#_Boxes_on_Truck_of_Product_i'] = piece_df['Max_#_Boxes_MAX_TRUCK_WEIGHT']

       print('\n')
       print('Max_#_Boxes_MAX_TRUCK_WEIGH =', piece_df['Max_#_Boxes_MAX_TRUCK_WEIGHT'].values[0])
       print('BOXES_NUMBER =', BOXES_NUMBER.values[0])
       print('Max_#_Boxes_on_Truck_of_Product_i =' , piece_df['Max_#_Boxes_on_Truck_of_Product_i'].values[0])
       print('\n')

     All_Qi_df = pd.concat([All_Qi_df, piece_df])

All_Qi_df.head(60)


# COMMAND ----------


# Introduction of fake Retailer Classification for filling purposes 

retailer_fake_classification = ["B", "C", "D", "E", "F"]

for j in range(len(All_Qi_df)): 
  
  classification = str(All_Qi_df['tasty'].values[j])
  
  if classification == '-':    # hyphen '-' sustitution for a fake retailer classification ('B', 'C', 'D', 'E' or 'F')
        
    random_classification = random.choice(retailer_fake_classification)
    
    All_Qi_df['tasty'].values[j] = All_Qi_df['tasty'].values[j].replace('-', random_classification)
    
All_Qi_df['tasty'].unique()


# COMMAND ----------


#Format Transformation

All_Qi_df_spark = spark.createDataFrame(All_Qi_df)
display(All_Qi_df_spark)


# COMMAND ----------

#Removing special characters from columns' names - Mount Point Saving Format requirement

New_List_of_columns = []
Old_List_of_columns = []

for j, column in enumerate(sorted(All_Qi_df_spark.columns), 0):
    
    Old_List_of_columns.append(column)

    if '__' or '/' or '(' or '%' or ' ' or '\'' in column:
        column = column.replace('(', '_')
        column = column.replace(')', '_')
        column = column.replace(' ', '_')
        column = column.replace('%', 'PERCENT')
        column = column.replace('\'', '_')
        column = column.replace('__', '_')
        column = column.replace('-', '_')
        column = column.replace('/', '_')
        New_List_of_columns.append(column)
    
    #if ')' in column:
        #column = column.replace('(', '_')
        #column = column.replace(')', '_')
        #New_List_of_columns.append(column)
    
    else:
        New_List_of_columns.append(column)
    
    print(j+1, ')', Old_List_of_columns[j])
    print(j+1, ')', New_List_of_columns[j])
    print('\n')
    
   
    All_Qi_df_spark = All_Qi_df_spark.withColumnRenamed(Old_List_of_columns[j], New_List_of_columns[j])


# COMMAND ----------


#Table's saving
All_Qi_df_spark.write.saveAsTable("All_Qi_df_spark")


# COMMAND ----------

"""
# when recovering the stored table 

#STORED DF CALL
df_pivot_2 = spark.table("hive_metastore.default.All_Qi_df_spark")
All_Qi_df = df_pivot_2.toPandas()
All_Qi_df


# COMMAND ----------


# Arranging DataFrame by "RET_ID" and "idProd" - Getting the "Xji" ORDER QUANTITIES of each PRODUCT for each RETAILER  

COLLECTOR_DF = pd.DataFrame ()

All_Qi_df_Copy = All_Qi_df.copy()

All_Qi_df_Copy.rename(columns = {'Max_#_Boxes_on_Truck_of_Product_i' : '* Max_#_Boxes_on_Truck_of_Product_i'}, inplace = True)

for x in All_Qi_df_Copy['RET_ID'].unique():

  All_Qi_df_Copy_Retailer = All_Qi_df_Copy.loc[All_Qi_df_Copy['RET_ID'] == x]

  for y in All_Qi_df_Copy_Retailer['idProduct'].unique():

    All_Qi_df_Copy_Retailer_Product = All_Qi_df_Copy_Retailer.loc[All_Qi_df_Copy_Retailer['idProduct'] == y]

    All_Qi_df_Copy_Retailer_Product['* TOTAL_#_BOXES_RET_j_PRODUCT_i'] = sum(All_Qi_df_Copy_Retailer_Product['Order_Size_Ceil_Value'].values)
    
    POSSIBLE =  All_Qi_df_Copy_Retailer_Product['* Max_#_Boxes_on_Truck_of_Product_i'].values[0]
    NECESSARY = All_Qi_df_Copy_Retailer_Product['* TOTAL_#_BOXES_RET_j_PRODUCT_i'].values[0]

    All_Qi_df_Copy_Retailer_Product['* Q_i'] = min(NECESSARY, POSSIBLE)

    COLLECTOR_DF = pd.concat([COLLECTOR_DF, All_Qi_df_Copy_Retailer_Product])

COLLECTOR_DF.head(60)


# COMMAND ----------


for j,x in enumerate(COLLECTOR_DF.columns,1):
    print(j, '_', x)
print('\n')


# COMMAND ----------

COLLECTOR_DF.shape

# COMMAND ----------

#TESTING
#Mismatch between what is called non stackable and the amount of allowed layers for the "i-product" due to the supported weight on it

COLLECTOR_DF['Max_Number_Of_Layers_Allowed_for_Product_i'].loc[COLLECTOR_DF['CS_STACKABLE'] == 'N'].unique() 

#It doesn't fit the information of non-stackable products within the input table, with the columns that gives the weight this same product resists on it

# COMMAND ----------

#TESTING
# 1 possible layer products that are both stackable and non stackable 

print('STACKABLE =', len(COLLECTOR_DF['CS_STACKABLE'].loc[(COLLECTOR_DF['Max_Number_Of_Layers_Allowed_for_Product_i'] == 1) & (COLLECTOR_DF['CS_STACKABLE'] == 'Y')]))

print('NON-STACKABLE =', len(COLLECTOR_DF['CS_STACKABLE'].loc[(COLLECTOR_DF['Max_Number_Of_Layers_Allowed_for_Product_i'] == 1) & (COLLECTOR_DF['CS_STACKABLE'] == 'N')]))

print('BOTH', len( COLLECTOR_DF['CS_STACKABLE'].loc[COLLECTOR_DF['Max_Number_Of_Layers_Allowed_for_Product_i'] == 1]))

#How many of each type are there in each class (STACKABLE or NON-STACKABLE)

# COMMAND ----------

#TESTING
len(COLLECTOR_DF.loc[COLLECTOR_DF['CS_STACKABLE'] == 'N'])
#These are the ones in the NON-STACKABLE class, and with diverse number of layers allowed, not just .loc into only the 1 layer class

# COMMAND ----------

#TESTING
class_cases_list = (COLLECTOR_DF['Max_Number_Of_Layers_Allowed_for_Product_i'].loc[COLLECTOR_DF['CS_STACKABLE'] == 'N']).unique()

for x in class_cases_list:
    
    class_case_length = len(COLLECTOR_DF['Max_Number_Of_Layers_Allowed_for_Product_i'].loc[(COLLECTOR_DF['CS_STACKABLE'] == 'N') & (COLLECTOR_DF['Max_Number_Of_Layers_Allowed_for_Product_i'] == x)])

    percentage_ratio = len(COLLECTOR_DF['Max_Number_Of_Layers_Allowed_for_Product_i'].loc[(COLLECTOR_DF['CS_STACKABLE'] == 'N') & (COLLECTOR_DF['Max_Number_Of_Layers_Allowed_for_Product_i'] == x)]) / len(COLLECTOR_DF['Max_Number_Of_Layers_Allowed_for_Product_i'].loc[COLLECTOR_DF['CS_STACKABLE'] == 'N']) * 100

    print('class case:', x, 'layer/s allowed on truck \'cause of stackable weight & with Non-Stackable denomination, Df Length =', class_case_length, ', ','Relative Ratio = ', "%.3f" % percentage_ratio, '%' )
    
#These are the ones in the NON-STACKABLE class, and with diverse number of layers allowed, not just .loc into only the 1 layer class


# COMMAND ----------

#TESTING
COLLECTOR_DF.loc[COLLECTOR_DF['CS_STACKABLE'] == 'N']
#These are the ones in the NON-STACKABLE class, and with diverse number of layers allowed, not just .loc into only the 1 layer class

# COMMAND ----------


#Format Transformation

COLLECTOR_SPARK = spark.createDataFrame(COLLECTOR_DF)
display(COLLECTOR_SPARK)


# COMMAND ----------

len(COLLECTOR_DF)

# COMMAND ----------

#Removing special characters from columns' names - Mount Point Saving Format requirement

New_List_of_columns = []
Old_List_of_columns = []

for j, column in enumerate(sorted(COLLECTOR_SPARK.columns), 0):
    
    Old_List_of_columns.append(column)

    if '__' or '/' or '(' or '%' or ' ' or '\'' in column:
        column = column.replace('(', '_')
        column = column.replace(')', '_')
        column = column.replace(' ', '_')
        column = column.replace('%', 'PERCENT')
        column = column.replace('\'', '_')
        column = column.replace('__', '_')
        column = column.replace('-', '_')
        column = column.replace('/', '_')
        New_List_of_columns.append(column)
    
    #if ')' in column:
        #column = column.replace('(', '_')
        #column = column.replace(')', '_')
        #New_List_of_columns.append(column)
    
    else:
        New_List_of_columns.append(column)
    
    print(j+1, ')', Old_List_of_columns[j])
    print(j+1, ')', New_List_of_columns[j])
    print('\n')

   
    COLLECTOR_SPARK = COLLECTOR_SPARK.withColumnRenamed(Old_List_of_columns[j], New_List_of_columns[j])


# COMMAND ----------


#Table's saving
COLLECTOR_SPARK.write.saveAsTable("COLLECTOR_DF_SPARK")


# COMMAND ----------

"""
# when recovering the stored table 

#STORED DF CALL
df_pivot_3 = spark.table("hive_metastore.default.COLLECTOR_DF_SPARK")
COLLECTOR_DF = df_pivot_3.toPandas()
COLLECTOR_DF



# COMMAND ----------


#Constructing the REFRIGERATED Df by keeping the REFRIGERATED cases from the general table

COLLECTOR_DF_REFRIGERATED = COLLECTOR_DF.loc[(COLLECTOR_DF["REFRIGERATED"] == 'yes') & (COLLECTOR_DF["NON-REFRIGERATED"] == 'no')]
COLLECTOR_DF_REFRIGERATED


# COMMAND ----------


#Constructing the NON-REFRIGERATED Df by keeping the NON-REFRIGERATED cases from the general table

COLLECTOR_DF_NON_REFRIGERATED = COLLECTOR_DF.loc[(COLLECTOR_DF["REFRIGERATED"] == 'no') & (COLLECTOR_DF["NON-REFRIGERATED"] == 'yes')]
COLLECTOR_DF_NON_REFRIGERATED


# COMMAND ----------

# MAGIC %md #Linear Programming Implementation - Optimized Truck Load

# COMMAND ----------

# MAGIC %md FUNCTIONS DEFINITONS 

# COMMAND ----------


############# Iterably Called OPTIMIZATION FUNCTION ###############

def Optimization_funct(MOCK_DF, t, PARAMETER_LIST, RET_ID, REFRIG):

    #Listing Products and its' Loading Limits on Truck ---------------------------------------------------------------------------------

    idProduct_list = []
    Prod_Descript_list = []
    LIST_PRODUCTS_LIMITS = []
    Product_Classification_list = []

    for x in sorted(MOCK_DF['idProduct'].unique()):

        LIMIT = MOCK_DF['* Q_i'].loc[MOCK_DF['idProduct'] == x].values[0]   #Representative element for each idProduct Max Limit on Truck
        LIST_PRODUCTS_LIMITS.append(LIMIT)  #building the list for these elements
        Prod_Descript = MOCK_DF['DESCRIPCION_DSC'].loc[MOCK_DF['idProduct'] == x].values[0]
        idProduct_list.append(x)
        Prod_Descript_list.append(Prod_Descript)
        Product_Classification = MOCK_DF['tasty'].loc[MOCK_DF['idProduct'] == x].values[0]
        Product_Classification_list.append(Product_Classification)

    print('idProduct_list =')
    print(idProduct_list)
    print('\n')

    #-----------------------------------------------------------------------------------------------------------------------------------
    
    print('Order\'s Boxes # =', sum(LIST_PRODUCTS_LIMITS))   #Greatest Possible Quantity for the elements of the Products Order/ Considering Max Load of each 
    print('\n')                        #Prod on Truck
     

    #Products Weights and Volumes Lists ------------------------------------------------------------------------------------------------

    # Weights
    LIST_WEIGHTS = []

    for x in sorted(MOCK_DF['idProduct'].unique()):

        LIMIT_WEIGHT = MOCK_DF['CS_GROSS_WEIGHT'].loc[MOCK_DF['idProduct'] == x].values[0]
        LIST_WEIGHTS.append(LIMIT_WEIGHT)


    # Volumes
    LIST_VOLUMES = []

    for x in sorted(MOCK_DF['idProduct'].unique()):

        LIMIT_VOLUME = MOCK_DF['CS_VOLUME'].loc[MOCK_DF['idProduct'] == x].values[0]
        LIST_VOLUMES.append(LIMIT_VOLUME)

    #-----------------------------------------------------------------------------------------------------------------------------------

    #Maximum Truck Load Optimization

    Volume_Weight = 2
    N_Products = len(MOCK_DF['idProduct'].unique())


    # Quantities Weighing Matrix

    list_j = [1 for x in range(N_Products)]

    #cost_matrix = np.array([LIST_WEIGHTS, LIST_VOLUMES])

    cost_matrix = np.array([list_j]) #, list_j])

    # Products Logistic Limits
    LIST_PRODUCTS_LIMITS = []
    LIST_PRODUCTS_ORDER_NECESSARY = []
    
    TOTAL_Num_BOXES_RET_j_PRODUCT_i = PARAMETER_LIST  #putting the updater list inside the algorithm //
                                                      #Product Order's Values Representants List

    
    for x in sorted(MOCK_DF['idProduct'].unique()):

        LIMIT_TRUCK = MOCK_DF['* Q_i'].loc[MOCK_DF['idProduct'] == x].values[0]
        LIMIT_LOAD  = TOTAL_Num_BOXES_RET_j_PRODUCT_i[(sorted(MOCK_DF['idProduct'].unique())).index(x)]
        CONSTRAINT = min(LIMIT_TRUCK, LIMIT_LOAD)
        LIST_PRODUCTS_LIMITS.append(2*CONSTRAINT) #[Taking the Double of the "Q_i" allowing symmetry between Volume and Weight] 

        #ORDER_NECESSARY = MOCK_DF['* TOTAL_#_BOXES_RET_j_PRODUCT_i'].loc[MOCK_DF['idProduct'] == x].values[0]
        #LIST_PRODUCTS_ORDER_NECESSARY.append(ORDER_NECESSARY) 

    Products_Qi_s = np.array(LIST_PRODUCTS_LIMITS) 
    #TOTAL_Num_BOXES_RET_j_PRODUCT_i = np.array(LIST_PRODUCTS_ORDER_NECESSARY)
    

    # Weight and Volume Constraints
    Volume_Weight_Constraints = np.array([28000, 90000]) #// Volume_Weight_Constraints

    #-----------------------------------------------------------------------------------------------------------------------------------
   
    model = pulp.LpProblem("Truck Logistic Loading", LpMaximize)

    #Indexes list according to table's notation: (Weight/Volume Limits --> rows), (Products --> columns) -------------------------------

    variable_names = [int(j) for j in range(1, N_Products+1)] #+int(j) for i in range(1, Volume_Weight+1)
    variable_names.sort()
    print("Variable Indices =")
    print(variable_names)
    print('\n')

    #-----------------------------------------------------------------------------------------------------------------------------------
  
    #Decision variables

    DV_variables = LpVariable.matrix("X", variable_names, cat = "Integer", lowBound = 0)
    allocation = np.array(DV_variables).reshape(1,N_Products)
    print("Decision Variable/Allocation Matrix =")
    print(allocation)
    print('\n')


    #-----------------------------------------------------------------------------------------------------------------------------------
          
    #Target function

    obj_func = lpSum(allocation*cost_matrix)
    print(obj_func)
    print('\n')
    model +=  obj_func
    print(model)
    print('\n')

    #-----------------------------------------------------------------------------------------------------------------------------------
    
    allocation_1 = [allocation[0]*LIST_WEIGHTS, allocation[0]*LIST_VOLUMES]  
    
    #Volume_Weight Constraints ---------------------------------------------------------------------------------------------------------

    for i in range(Volume_Weight):
        print(lpSum(allocation_1[0][j] for j in range(N_Products)) <= Volume_Weight_Constraints[i])
        print('\n')
        model += lpSum(allocation_1[0][j] for j in range(N_Products)) <= Volume_Weight_Constraints[i] , "Volume-Weight Constraints " + str(i)
    print('\n')

    #-----------------------------------------------------------------------------------------------------------------------------------
    
    # Products Constraints

    for j in range(N_Products):
        print(lpSum(allocation[0][j] for i in range(Volume_Weight)) <= Products_Qi_s[j])  
        model += lpSum(allocation[0][j] for i in range(Volume_Weight)) <= Products_Qi_s[j] , "Products Constraints " + str(j)  
    print('\n')                                                                                 #for i in range (Volume_Weight)
                                                                                                #for i in range(Volume_Weight)


    #-----------------------------------------------------------------------------------------------------------------------------------
    
    #Model's execution

    model.solve(PULP_CBC_CMD())

    status =  LpStatus[model.status]

    print(status)
    print('\n')


    #Total Products Units printing -----------------------------------------------------------------------------------------------------

    print("Qi_Boxes =")
    print(model.objective.value())
    print('\n')

    # Decision Variables

    NEW_NAMES_LIST = []

    for j,v in enumerate(model.variables(),1):
        #try:
        print(j, '_ ',v.name,"=", v.value())
        
        if 'X_' in v.name:
            new_name = int(v.name.replace('X_', ''))
            #print(j, '_ ',new_name,"=", v.value())
            NEW_ELEMENT = (new_name, v.value())
            NEW_NAMES_LIST.append(NEW_ELEMENT)
    
    print('\n')
    print('Cycle # =', t)
    print('\n')
    print(NEW_NAMES_LIST)
    print('\n')
    


    #-----------------------------------------------------------------------------------------------------------------------------------
    #-----------------------------------------------------------------------------------------------------------------------------------
    #-----------------------------------------------------------------------------------------------------------------------------------
    #-----------------------------------------------------------------------------------------------------------------------------------

    

    #Comparing Optimization Results with Order's Product Quantities --------------------------------------------------------------------

    RESULTS_COMPARISON_DF = pd.DataFrame()

    x_VALUES_LIST = [v for w, v in sorted(NEW_NAMES_LIST)]
    x_NAMES_LIST = [w for w, v in sorted(NEW_NAMES_LIST)]
    
    Z = f"{'TRUCK_'}{t}{' [boxes]'}"

    RESULTS_COMPARISON_DF[Z] = x_VALUES_LIST

    W_Equiv = f"{'Equiv.Weight-TRUCK_'}{t}{'_[%]'}"
    V_Equiv = f"{'Equiv.Volume-TRUCK_'}{t}{'_[%]'}"
    
    RESULTS_COMPARISON_DF['Product_i_Load_Limit [boxes]'] = Products_Qi_s/2                      #This comes from Truck Weight-Volume Constraints

    #TOTAL_Num_BOXES_RET_j_PRODUCT_i = Order_Size_Updating(RESULTS_COMPARISON_DF) #Call of this Function Inside the Optimization 
                                                                                  #Function

    RESULTS_COMPARISON_DF = RESULTS_COMPARISON_DF[[Z]].join(RESULTS_COMPARISON_DF['Product_i_Load_Limit [boxes]'], how = 'left' )
    
    RESULTS_COMPARISON_DF['Order_Size [boxes]'] = TOTAL_Num_BOXES_RET_j_PRODUCT_i

    RESULTS_COMPARISON_DF['id_Product'] = idProduct_list

    RESULTS_COMPARISON_DF['Product_Description'] = Prod_Descript_list

    #-----------------------------------------------------------------------------------------------------------------------------------
      

    RESULTS_COMPARISON_DF['Order_s_Products_Left [boxes]'] = RESULTS_COMPARISON_DF['Order_Size [boxes]'] - RESULTS_COMPARISON_DF[Z]
    
    # Columns Addition to the Original MOCK_DF so as to get the final DF with the products remaining for the Truck Load
    
    RESULTS_COMPARISON_DF['Weight_Products_i'] = LIST_WEIGHTS

    # Residual Order Consideration - Counting differences and taking aim of Load left to transport -------------------------------------

    RESULTS_COMPARISON_DF['CS_Weights'] = LIST_WEIGHTS

    RESULTS_COMPARISON_DF['Order_s_Products_Left_Weight'] = LIST_WEIGHTS*RESULTS_COMPARISON_DF['Order_s_Products_Left [boxes]']

    RESULTS_COMPARISON_DF['Order_s_Products_Left_Volume'] = LIST_VOLUMES*RESULTS_COMPARISON_DF['Order_s_Products_Left [boxes]'] 

    Remaining_Load_Weight = sum(RESULTS_COMPARISON_DF['Order_s_Products_Left [boxes]']*LIST_WEIGHTS)
    Remaining_Load_Volume = sum(RESULTS_COMPARISON_DF['Order_s_Products_Left [boxes]']*LIST_VOLUMES)

    RESULTS_COMPARISON_DF['RET_ID'] = RET_ID
    
    RESULTS_COMPARISON_DF['REFRIGERATION'] = REFRIG

    RESULTS_COMPARISON_DF['Product_Classification'] = Product_Classification_list

    
    #, Truck_Volume   #obviously is the same than /280 or /900, but to make clear that it refers to the percentage of Truck's Max Weight 28000 Kg or Truck's Max Volume 90000 ltr supported
    Truck_Weight = sum(RESULTS_COMPARISON_DF[Z] * LIST_WEIGHTS) / 28000 * 100
    Truck_Volume = sum(RESULTS_COMPARISON_DF[Z] * LIST_VOLUMES) / 90000 * 100
    
    RESULTS_COMPARISON_DF[W_Equiv] = Truck_Weight
    RESULTS_COMPARISON_DF[V_Equiv] = Truck_Volume
        
 
    #RESULTS_COMPARISON_DF[W_V_Equiv] = sum(x_VALUES_LIST * LIST_WEIGHTS)
    
    #Updating Process Stage ------------------------------------------------------------------------------------------------------------
    
    #print(RESULTS_COMPARISON_DF[['RET_ID' ,'id_Product', 'Product_Description', 'Order_Size [boxes]', 'Product_i_Load_Limit [boxes]' ,Z , 'Order_s_Products_Left [boxes]']]) 
    #print('\n')
    
    #Generating the updating column in the original MOCK_DF ----------------------------------------------------------------------------
    
    TOTAL_Num_BOXES_RET_j_PRODUCT_i = RESULTS_COMPARISON_DF['Order_s_Products_Left [boxes]']

    """
    MOCK_DF['Order_s_Products_Left [boxes]'] = 0

    for x in sorted(MOCK_DF['idProduct'].unique()):

        UPDATING_VALUE = RESULTS_COMPARISON_DF['Order_s_Products_Left [boxes]'].values[idProduct_list.index(x)] #inputting Product_i residual into
                                                                                                      #the old #Boxes per Order Ret j
        MOCK_DF.loc[MOCK_DF['idProduct'] == x, 'Order_s_Products_Left [boxes]'] = UPDATING_VALUE
        
        #MOCK_DF.loc[MOCK_DF['idProduct'] == x, '* TOTAL_#_BOXES_RET_j_PRODUCT_i'] = UPDATING_VALUE
    """
    #-----------------------------------------------------------------------------------------------------------------------------------

    return [Remaining_Load_Weight, Remaining_Load_Volume, RESULTS_COMPARISON_DF[['RET_ID' ,'id_Product', 'Product_Description',  'REFRIGERATION', 'Product_Classification', 'Order_Size [boxes]','Product_i_Load_Limit [boxes]', Z, W_Equiv, V_Equiv, 'Order_s_Products_Left [boxes]']], TOTAL_Num_BOXES_RET_j_PRODUCT_i, t]


# COMMAND ----------

#Anticipating the iteration for every Retailer
y = 'CALIMAX1'
MOCK_DF = COLLECTOR_DF.loc[COLLECTOR_DF['RET_ID'] == y]


# COMMAND ----------

# MAGIC %md Iteration beginning

# COMMAND ----------

def completion_function():

# COMMAND ----------


#ITERATING BY RETAILERS - REFRIGERATED CASE

CASES = ['REFRIGERATED', 'NON-REFRIGERATED']
CASES_DFS = [COLLECTOR_DF_REFRIGERATED, COLLECTOR_DF_NON_REFRIGERATED]

REFRIG = str('YES')

LIST_RETAILERS = []
LIST_LOGISTICS_DFS = []
LIST_OF_REFRIG_INCOMPLETE_ONES = []

COLLECTOR_DF_1 = COLLECTOR_DF_REFRIGERATED.copy()

print('REFRIGERATED CASES DF\'s SHAPE')
print(COLLECTOR_DF_1.shape)
print('\n')

for y in sorted(COLLECTOR_DF_1['RET_ID'].unique()):
    
    #######################################################################################################################################

    MOCK_DF = COLLECTOR_DF_1.loc[COLLECTOR_DF_1['RET_ID'] == y]

    #######################################################################################################################################

    #Products Weights and Volumes Lists 

    # Weights
    LIST_WEIGHTS = []

    for x in sorted(MOCK_DF['idProduct'].unique()):

        LIMIT_WEIGHT = MOCK_DF['CS_GROSS_WEIGHT'].loc[MOCK_DF['idProduct'] == x].values[0]
        LIST_WEIGHTS.append(LIMIT_WEIGHT)


    # Volumes
    LIST_VOLUMES = []

    for x in sorted(MOCK_DF['idProduct'].unique()):

        LIMIT_VOLUME = MOCK_DF['CS_VOLUME'].loc[MOCK_DF['idProduct'] == x].values[0]
        LIST_VOLUMES.append(LIMIT_VOLUME)

    print(LIST_WEIGHTS)
    print('\n')
    print(LIST_VOLUMES)
    print('\n')

    #######################################################################################################################################

    # Optimization Function Iteration

    # First necessary step in stablishing the state of Load for the particular order ---------------------------------------------------------

    LIST_PRODUCTS_ORDER_NECESSARY = []

    for x in sorted(MOCK_DF['idProduct'].unique()):

        ORDER_NECESSARY = MOCK_DF['* TOTAL_#_BOXES_RET_j_PRODUCT_i'].loc[MOCK_DF['idProduct'] == x].values[0]
        LIST_PRODUCTS_ORDER_NECESSARY.append(ORDER_NECESSARY) 

    TOTAL_Num_BOXES_RET_j_PRODUCT_i = np.array(LIST_PRODUCTS_ORDER_NECESSARY)

    Remaining_Load_Weight = sum(TOTAL_Num_BOXES_RET_j_PRODUCT_i*LIST_WEIGHTS)
    Remaining_Load_Volume = sum(TOTAL_Num_BOXES_RET_j_PRODUCT_i*LIST_VOLUMES)

    print('\n')
    print('################################################################################################################################################')
    print('\n')
    print('BEGINNING OF CASE : (', y,')')
    print('\n')
    print('Weight and Volume Load\'s Initial Characteristics:') 
    print('Load\'s Equivalent Weight =', "%.3f" % Remaining_Load_Weight, 'Kg', '(', "%.3f" % (Remaining_Load_Weight/28000 * 100), '%  Truck\'s Max Weight)')
    print('Load\'s Equivalent Volume =', "%.3f" % Remaining_Load_Volume, 'ltr', '(', "%.3f" % (Remaining_Load_Volume/90000 * 100), '%  Truck\'s Max Volume)')
    print('\n')
    
    INITIAL_WEIGHT = Remaining_Load_Weight/28000 * 100
    INITIAL_VOLUME = Remaining_Load_Volume/90000 * 100
   
    if ((50 <= INITIAL_WEIGHT) or (50 <= INITIAL_VOLUME)) and ((INITIAL_VOLUME < 100) and (INITIAL_WEIGHT < 100)):
        
        LIST_OF_REFRIG_INCOMPLETE_ONES.append(y)

    #-----------------------------------------------------------------------------------------------------------------------------------------

    local_df_list = []
    t = 1
    
    Weight_Threshold = Remaining_Load_Weight/28000 * 100
    Volume_Threshold = Remaining_Load_Volume/90000 * 100

    while(50 <= Weight_Threshold) or (50 <= Volume_Threshold): #(([x >= 0 for x in TOTAL_Num_BOXES_RET_j_PRODUCT_i] == True) 
                                                                            #and Considering the 50% of Maximum Truck Weight or the 50% of #Max Truck Volume still over passing the minimum threshold
                                                                                
        OUT = Optimization_funct(MOCK_DF, t, TOTAL_Num_BOXES_RET_j_PRODUCT_i, y, REFRIG)     # y = RET_ID

        Remaining_Load_Weight = OUT[0]
        Remaining_Load_Volume = OUT[1]
        
        Weight_Threshold = Remaining_Load_Weight/28000 * 100
        Volume_Threshold = Remaining_Load_Volume/90000 * 100

        print('Remaining_Load_Weight =', "%.3f" % Remaining_Load_Weight, 'Kg', '(', "%.3f" % (Remaining_Load_Weight/28000 * 100), '%  Truck\'s Max Weight)')
        print('Remaining_Load_Volume =', "%.3f" % Remaining_Load_Volume, 'ltr', '(', "%.3f" % (Remaining_Load_Volume/90000 * 100), '%  Truck\'s Max Volume)')
        print('\n')
        
        TOTAL_Num_BOXES_RET_j_PRODUCT_i = OUT[3]
        t = t + 1

        #if[x >= 0 for x in TOTAL_Num_BOXES_RET_j_PRODUCT_i] == True:            #Condition Updating

        local_df_list.append(OUT[2])

        #else:
            
            #TOTAL_Num_BOXES_RET_j_PRODUCT_i = TOTAL_Num_BOXES_RET_j_PRODUCT_i


    #OUT = Optimization_funct(MOCK_DF,t, PARAMETER_LIST)
    t = OUT[4]
    print('Remaining Weight =', "%.3f" % OUT[0], 'Kg', '(', "%.3f" % (OUT[0]/28000 * 100), '%  Truck\'s Max Weight)')
    print('Remaining Volume =', "%.3f" % OUT[1], 'ltr', '(', "%.3f" % (OUT[1]/90000 * 100), '%  Truck\'s Max Volume)')
    print('\n')
    print(OUT[2])
    print('\n')
    print('Final Count for "t" =', OUT[4])
    print('\n')
    print('Local dfs List', local_df_list)
    print('\n')
    print('END OF CASE : (', y,')')
    print('################################################################################################################################################')
    print('\n')
    
    FINAL_WEIGHT = OUT[0]/28000 * 100
    FINAL_VOLUME = OUT[1]/90000 * 100

    #######################################################################################################################################
   
    #FINAL DATAFRAME PER RETAILER ORDER

    if len(local_df_list) > 0:

        FINAL_DF = pd.DataFrame()

        FINAL_DF[['RET_ID' ,'id_Product', 'Product_Description','REFRIGERATION', 'Product_Classification', 'Order_Size [boxes]' ,'Product_i_Load_Limit [boxes]']] = local_df_list[0][['RET_ID' ,'id_Product', 'Product_Description', 'REFRIGERATION', 'Product_Classification','Order_Size [boxes]' ,'Product_i_Load_Limit [boxes]']]

        for i in range(1,t+1):

            FINAL_DF[f"{'TRUCK_'}{i}{' [boxes]'}"] = local_df_list[i-1][f"{'TRUCK_'}{i}{' [boxes]'}"]
            FINAL_DF[f"{'Equiv.Weight-TRUCK_'}{i}{'_[%]'}"] = local_df_list[i-1][f"{'Equiv.Weight-TRUCK_'}{i}{'_[%]'}"]
            FINAL_DF[f"{'Equiv.Volume-TRUCK_'}{i}{'_[%]'}"] = local_df_list[i-1][f"{'Equiv.Volume-TRUCK_'}{i}{'_[%]'}"]
            
        FINAL_DF['Order_s_Products_Left [boxes]'] = local_df_list[t-1]['Order_s_Products_Left [boxes]']
        
        FINAL_DF['Initial Weight %'] = INITIAL_WEIGHT
        FINAL_DF['Initial Volume %'] = INITIAL_VOLUME

        FINAL_DF['Final Weight %'] = FINAL_WEIGHT
        FINAL_DF['Final Volume %'] = FINAL_VOLUME

    #######################################################################################################################################


    #LISTS FOR ITERATION OVER MARKET RETAILERS



    LIST_RETAILERS.append(y)
    LIST_LOGISTICS_DFS.append(FINAL_DF)
 

print('len(LIST_RETAILERS) =',len(LIST_RETAILERS))
print('\n')


# COMMAND ----------


for j,x in enumerate(LIST_OF_REFRIG_INCOMPLETE_ONES, 1):
    print(j, '_', x)
print('\n')


# COMMAND ----------

DF_REFRIGERATED = LIST_LOGISTICS_DFS[LIST_RETAILERS.index(RETAILER_ID)]
DF_REFRIGERATED

# COMMAND ----------

DF_REFRIGERATED = DF_REFRIGERATED.sort_values(['Product_Classification', 'Product_i_Load_Limit [boxes]'] , ascending = [True, False])
DF_REFRIGERATED

# COMMAND ----------

DF_REFRIGERATED_SPARK = spark.createDataFrame(DF_REFRIGERATED)
display(DF_REFRIGERATED_SPARK)

# COMMAND ----------


#ITERATING BY RETAILERS - NON-REFRIGERATED CASE

CASES = ['REFRIGERATED', 'NON-REFRIGERATED']
CASES_DFS = [COLLECTOR_DF_REFRIGERATED, COLLECTOR_DF_NON_REFRIGERATED]

REFRIG = str('NO')

LIST_RETAILERS_2 = []
LIST_LOGISTICS_DFS_2 = []
LIST_OF_NON_REFRIG_INCOMPLETE_ONES = []

COLLECTOR_DF_2 = COLLECTOR_DF_NON_REFRIGERATED.copy()
 
print('NON-REFRIGERATED CASES DF\'s SHAPE')
print(COLLECTOR_DF_2.shape)
print('\n')

for y in sorted(COLLECTOR_DF_2['RET_ID'].unique()):
    
    #######################################################################################################################################

    MOCK_DF = COLLECTOR_DF_2.loc[COLLECTOR_DF_2['RET_ID'] == y]

    #######################################################################################################################################

    #Products Weights and Volumes Lists 

    # Weights
    LIST_WEIGHTS = []

    for x in sorted(MOCK_DF['idProduct'].unique()):

        LIMIT_WEIGHT = MOCK_DF['CS_GROSS_WEIGHT'].loc[MOCK_DF['idProduct'] == x].values[0]
        LIST_WEIGHTS.append(LIMIT_WEIGHT)


    # Volumes
    LIST_VOLUMES = []

    for x in sorted(MOCK_DF['idProduct'].unique()):

        LIMIT_VOLUME = MOCK_DF['CS_VOLUME'].loc[MOCK_DF['idProduct'] == x].values[0]
        LIST_VOLUMES.append(LIMIT_VOLUME)

    print(LIST_WEIGHTS)
    print('\n')
    print(LIST_VOLUMES)
    print('\n')

    #######################################################################################################################################

    # Optimization Function Iteration

    # First necessary step in stablishing the state of Load for the particular order ---------------------------------------------------------

    LIST_PRODUCTS_ORDER_NECESSARY = []

    for x in sorted(MOCK_DF['idProduct'].unique()):

        ORDER_NECESSARY = MOCK_DF['* TOTAL_#_BOXES_RET_j_PRODUCT_i'].loc[MOCK_DF['idProduct'] == x].values[0]
        LIST_PRODUCTS_ORDER_NECESSARY.append(ORDER_NECESSARY) 

    TOTAL_Num_BOXES_RET_j_PRODUCT_i = np.array(LIST_PRODUCTS_ORDER_NECESSARY)

    Remaining_Load_Weight = sum(TOTAL_Num_BOXES_RET_j_PRODUCT_i*LIST_WEIGHTS)
    Remaining_Load_Volume = sum(TOTAL_Num_BOXES_RET_j_PRODUCT_i*LIST_VOLUMES)

    print('\n')
    print('################################################################################################################################################')
    print('\n')
    print('BEGINNING OF CASE : (', y,')')
    print('\n')
    print('Weight and Volume Load\'s Initial Characteristics:') 
    print('Load\'s Equivalent Weight =', "%.3f" % Remaining_Load_Weight, 'Kg', '(', "%.3f" % (Remaining_Load_Weight/28000 * 100), '%  Truck\'s Max Weight)')
    print('Load\'s Equivalent Volume =', "%.3f" % Remaining_Load_Volume, 'ltr', '(', "%.3f" % (Remaining_Load_Volume/90000 * 100), '%  Truck\'s Max Volume)')
    print('\n')

    INITIAL_WEIGHT = Remaining_Load_Weight/28000 * 100
    INITIAL_VOLUME = Remaining_Load_Volume/90000 * 100

    
    if ((50 <= INITIAL_WEIGHT) or (50 <= INITIAL_VOLUME)) and ((INITIAL_VOLUME < 100) and (INITIAL_WEIGHT < 100)):
        
        LIST_OF_NON_REFRIG_INCOMPLETE_ONES.append(y)

    #-----------------------------------------------------------------------------------------------------------------------------------------

    local_df_list = []
    t = 1
    
    Weight_Threshold = Remaining_Load_Weight/28000 * 100
    Volume_Threshold = Remaining_Load_Volume/90000 * 100
    
    while(50 <= Weight_Threshold) or (50 <= Volume_Threshold): #(([x >= 0 for x in TOTAL_Num_BOXES_RET_j_PRODUCT_i] == True) 
                                                                            #and Considering the 50% of Maximum Truck Weight or the 50% of #Max Truck Volume still over passing the minimum threshold
                                                                                
        OUT = Optimization_funct(MOCK_DF, t, TOTAL_Num_BOXES_RET_j_PRODUCT_i, y, REFRIG)     # y = RET_ID

        Remaining_Load_Weight = OUT[0]
        Remaining_Load_Volume = OUT[1]
        
        Weight_Threshold = Remaining_Load_Weight/28000 * 100
        Volume_Threshold = Remaining_Load_Volume/90000 * 100

        print('Remaining_Load_Weight =', "%.3f" % Remaining_Load_Weight, 'Kg', '(', "%.3f" % (Remaining_Load_Weight/28000 * 100), '%  Truck\'s Max Weight)')
        print('Remaining_Load_Volume =', "%.3f" % Remaining_Load_Volume, 'ltr', '(', "%.3f" % (Remaining_Load_Volume/90000 * 100), '%  Truck\'s Max Volume)')
        print('\n')
        
        TOTAL_Num_BOXES_RET_j_PRODUCT_i = OUT[3]
        t = t + 1

        #if[x >= 0 for x in TOTAL_Num_BOXES_RET_j_PRODUCT_i] == True:            #Condition Updating

        local_df_list.append(OUT[2])

        #else:
            
            #TOTAL_Num_BOXES_RET_j_PRODUCT_i = TOTAL_Num_BOXES_RET_j_PRODUCT_i


    #OUT = Optimization_funct(MOCK_DF,t, PARAMETER_LIST)
    t = OUT[4]
    print('Remaining Weight =', "%.3f" % OUT[0], 'Kg', '(', "%.3f" % (OUT[0]/28000 * 100), '%  Truck\'s Max Weight)')
    print('Remaining Volume =', "%.3f" % OUT[1], 'ltr', '(', "%.3f" % (OUT[1]/90000 * 100), '%  Truck\'s Max Volume)')
    print('\n')
    print(OUT[2])
    print('\n')
    print('Final Count for "t" =', OUT[4])
    print('\n')
    print('Local dfs List', local_df_list)
    print('\n')
    print('END OF CASE : (', y,')')
    print('################################################################################################################################################')
    print('\n')
    
    FINAL_WEIGHT = OUT[0]/28000 * 100
    FINAL_VOLUME = OUT[1]/90000 * 100

    #######################################################################################################################################
   
    #FINAL DATAFRAME PER RETAILER ORDER

    if len(local_df_list) > 0:

        FINAL_DF = pd.DataFrame()

        FINAL_DF[['RET_ID' ,'id_Product', 'Product_Description', 'REFRIGERATION', 'Product_Classification', 'Order_Size [boxes]' ,'Product_i_Load_Limit [boxes]']] = local_df_list[0][['RET_ID' ,'id_Product', 'Product_Description', 'REFRIGERATION', 'Product_Classification', 'Order_Size [boxes]' ,'Product_i_Load_Limit [boxes]']]

        for i in range(1,t+1):

            FINAL_DF[f"{'TRUCK_'}{i}{' [boxes]'}"] = local_df_list[i-1][f"{'TRUCK_'}{i}{' [boxes]'}"]
            FINAL_DF[f"{'Equiv.Weight-TRUCK_'}{i}{'_[%]'}"] = local_df_list[i-1][f"{'Equiv.Weight-TRUCK_'}{i}{'_[%]'}"]
            FINAL_DF[f"{'Equiv.Volume-TRUCK_'}{i}{'_[%]'}"] = local_df_list[i-1][f"{'Equiv.Volume-TRUCK_'}{i}{'_[%]'}"]

        FINAL_DF['Order_s_Products_Left [boxes]'] = local_df_list[t-1]['Order_s_Products_Left [boxes]']
        
        FINAL_DF['Initial Weight %'] = INITIAL_WEIGHT
        FINAL_DF['Initial Volume %'] = INITIAL_VOLUME

        FINAL_DF['Final Weight %'] = FINAL_WEIGHT
        FINAL_DF['Final Volume %'] = FINAL_VOLUME

    #######################################################################################################################################


    #LISTS FOR ITERATION OVER MARKET RETAILERS

    LIST_RETAILERS_2.append(y)
    LIST_LOGISTICS_DFS_2.append(FINAL_DF)
 
print('\n')
print(len(LIST_RETAILERS_2))
print('\n')
 

# COMMAND ----------


for j,x in enumerate(LIST_OF_NON_REFRIG_INCOMPLETE_ONES, 1):
    print(j, '_', x)
print('\n')


# COMMAND ----------

DF_NON_REFRIGERATED = LIST_LOGISTICS_DFS_2[LIST_RETAILERS_2.index(RETAILER_ID)]
DF_NON_REFRIGERATED

# COMMAND ----------

DF_NON_REFRIGERATED = DF_NON_REFRIGERATED.sort_values(['Product_Classification', 'Product_i_Load_Limit [boxes]'] , ascending = [True, False])
DF_NON_REFRIGERATED

# COMMAND ----------

DF_NON_REFRIGERATED_SPARK = spark.createDataFrame(DF_NON_REFRIGERATED)
display(DF_NON_REFRIGERATED_SPARK)

# COMMAND ----------

# MAGIC %md #Algorithm continuation...

# COMMAND ----------


for j,x in enumerate(LIST_RETAILERS,1):
    
    print(j,'_ ', x)

print('\n')


# COMMAND ----------


for j,x in enumerate(LIST_LOGISTICS_DFS,1):

   if len(x) != 0: 
    print(j,'_ ', x, '-->', len(x))

print('\n')

# COMMAND ----------

'''
#Products Weights and Volumes Lists 

# Weights
LIST_WEIGHTS = []

for x in sorted(MOCK_DF['idProduct'].unique()):

    LIMIT_WEIGHT = MOCK_DF['CS_GROSS_WEIGHT'].loc[MOCK_DF['idProduct'] == x].values[0]
    LIST_WEIGHTS.append(LIMIT_WEIGHT)


# Volumes
LIST_VOLUMES = []

for x in sorted(MOCK_DF['idProduct'].unique()):

    LIMIT_VOLUME = MOCK_DF['CS_VOLUME'].loc[MOCK_DF['idProduct'] == x].values[0]
    LIST_VOLUMES.append(LIMIT_VOLUME)

print(LIST_WEIGHTS)
print('\n')
print(LIST_VOLUMES)
print('\n')
'''

# COMMAND ----------

'''
# Optimization Function Iteration

# First necessary step in stablishing the state of Load for the particular order ---------------------------------------------------------

LIST_PRODUCTS_ORDER_NECESSARY = []

for x in sorted(MOCK_DF['idProduct'].unique()):

    ORDER_NECESSARY = MOCK_DF['* TOTAL_#_BOXES_RET_j_PRODUCT_i'].loc[MOCK_DF['idProduct'] == x].values[0]
    LIST_PRODUCTS_ORDER_NECESSARY.append(ORDER_NECESSARY) 

TOTAL_Num_BOXES_RET_j_PRODUCT_i = np.array(LIST_PRODUCTS_ORDER_NECESSARY)

Remaining_Load_Weight = sum(TOTAL_Num_BOXES_RET_j_PRODUCT_i*LIST_WEIGHTS)
Remaining_Load_Volume = sum(TOTAL_Num_BOXES_RET_j_PRODUCT_i*LIST_VOLUMES)

print('\n')
print('################################################################################################################################################')
print('\n')
print('BEGINNING OF CASE : (', y,')')
print('\n')
print('Weight and Volume Load\'s Initial Characteristics:') 
print('Load\'s Equivalent Weight =', "%.3f" % Remaining_Load_Weight, 'Kg', '(', "%.3f" % (Remaining_Load_Weight/28000 * 100), '%  Truck\'s Max Weight)')
print('Load\'s Equivalent Volume =', "%.3f" % Remaining_Load_Volume, 'ltr', '(', "%.3f" % (Remaining_Load_Volume/90000 * 100), '%  Truck\'s Max Volume)')
print('\n')

#-----------------------------------------------------------------------------------------------------------------------------------------

local_df_list = []
t = 1

while(14000 <= Remaining_Load_Weight) or (45000 <= Remaining_Load_Volume): #(([x >= 0 for x in TOTAL_Num_BOXES_RET_j_PRODUCT_i] == True) 
                                                                           #and Considering the 50% of Maximum Truck Weight or the 50% of #Max Truck Volume still over passing the minimum threshold
                                                                            
   OUT = Optimization_funct(MOCK_DF, t, TOTAL_Num_BOXES_RET_j_PRODUCT_i)

   Remaining_Load_Weight = OUT[0]
   Remaining_Load_Volume = OUT[1]
   print('Remaining_Load_Weight =', "%.3f" % Remaining_Load_Weight, 'Kg', '(', "%.3f" % (Remaining_Load_Weight/28000 * 100), '%  Truck\'s Max Weight)')
   print('Remaining_Load_Volume =', "%.3f" % Remaining_Load_Volume, 'ltr', '(', "%.3f" % (Remaining_Load_Volume/90000 * 100), '%  Truck\'s Max Volume)')
   print('\n')
   TOTAL_Num_BOXES_RET_j_PRODUCT_i = OUT[3]
   t = t + 1

   #if[x >= 0 for x in TOTAL_Num_BOXES_RET_j_PRODUCT_i] == True:            #Condition Updating

   local_df_list.append(OUT[2])

   #else:
     
     #TOTAL_Num_BOXES_RET_j_PRODUCT_i = TOTAL_Num_BOXES_RET_j_PRODUCT_i


#OUT = Optimization_funct(MOCK_DF,t, PARAMETER_LIST)
t = OUT[4]
print('Remaining Weight =', "%.3f" % OUT[0], 'Kg', '(', "%.3f" % (OUT[0]/28000 * 100), '%  Truck\'s Max Weight)')
print('Remaining Volume =', "%.3f" % OUT[1], 'ltr', '(', "%.3f" % (OUT[1]/90000 * 100), '%  Truck\'s Max Volume)')
print('\n')
print(OUT[2])
print('\n')
print('Final Count for "t" =', OUT[4])
print('\n')
print('Local dfs List', local_df_list)
print('\n')
print('END OF CASE : (', y,')')
print('################################################################################################################################################')
print('\n')
'''

# COMMAND ----------

'''#FINAL DATAFRAME PER RETAILER ORDER------------------------------------------------------------------------------------------------------- 

FINAL_DF = pd.DataFrame()

FINAL_DF[['id_Product', 'Product_Description', 'Order_Size [boxes]' ,'Product_i_Load_Limit [boxes]']] = local_df_list[0][['id_Product', 'Product_Description', 'Order_Size [boxes]' ,'Product_i_Load_Limit [boxes]']]

for i in range(1,t+1):

    FINAL_DF[f"{'TRUCK_'}{i}{' [boxes]'}"] = local_df_list[i-1][f"{'TRUCK_'}{i}{' [boxes]'}"]

FINAL_DF['Order_s_Products_Left [boxes]'] = local_df_list[t-1]['Order_s_Products_Left [boxes]']
FINAL_DF
#-----------------------------------------------------------------------------------------------------------------------------------------'''

# COMMAND ----------

# MAGIC %md #AFTER DATAFRAMES LIST GENERATION ---> SELECTING THE SEARCHED RETAILER'S DF

# COMMAND ----------


#LISTS FOR ITERATION OVER MARKET RETAILERS

LIST_RETAILERS = []
LIST_LOGISTICS_DFS = []

LIST_RETAILERS.append(y)
LIST_LOGISTICS_DFS.append(FINAL_DF)


# COMMAND ----------

LIST_LOGISTICS_DFS[LIST_RETAILERS.index(RETAILER_ID)]

# COMMAND ----------


local_df_list[0]['TRUCK_2 [boxes]'] = local_df_list[1]['TRUCK_2 [boxes]']
local_df_list[0]['Order_s_Products_Left [boxes]'] = local_df_list[1]['Order_s_Products_Left [boxes]']

local_df_list[0][['id_Product', 'Product_Description', 'Order_Size [boxes]' ,'Product_i_Load_Limit [boxes]', 'TRUCK_1 [boxes]', 'TRUCK_2 [boxes]','Order_s_Products_Left [boxes]']]


# COMMAND ----------

OUT = Optimization_funct(MOCK_DF, t, TOTAL_Num_BOXES_RET_j_PRODUCT_i)
OUT[2]

# COMMAND ----------

# MAGIC %md #Trucks Number Management

# COMMAND ----------


#Whole Volume and Weight Values Selection Df - Fixing by the Load Completeness Different Values

Block_Selection_df = pd.DataFrame()
slice_df = pd.DataFrame()
row_df = pd.DataFrame()
list_percents = []


for j,x in enumerate(Block_df['Truck_Volume_Completeness_%'].unique(), 1):   #Selecting by a "Truck's Completeness" consideration
  
   slice_df = Block_df.loc[Block_df['Truck_Volume_Completeness_%'] == x]

   if len(slice_df) > 0 :
    slice_df.reset_index(inplace = True)
    row_df = (pd.DataFrame(slice_df.loc[0]).transpose())

    Block_Selection_df = pd.concat([Block_Selection_df, row_df])

   else:
    print(Block_df['RET_ID'].loc[Block_df['Truck_Volume_Completeness_%'] == x])
     
Block_Selection_df.reset_index(inplace= True)
Block_Selection_df.head(50)


# COMMAND ----------

Block_Selection_df.drop(['level_0', 'index'], axis = 1, inplace= True)

# COMMAND ----------


# Restoring Columns Data Types

for z in Block_df.columns:
    
    Dtype = Block_df[z].dtypes
    Block_Selection_df[z] = Block_Selection_df[z].astype(Dtype)


# COMMAND ----------

Block_Selection_df.shape

# COMMAND ----------

Block_Selection_df_spark = spark.createDataFrame(Block_Selection_df)
display(Block_Selection_df_spark)

# COMMAND ----------


# Remaining Truck Loading Management

list_name_constraint = []
list_constraint = []
list_completeness_percent = []
list_number_trucks = []
list_Amount_For_Extra_Truck = []
list_Amount_For_Extra_Truck_Rounded = []
Modified_list_Extra_Truck = []

Block_Selection_df_Copy = Block_Selection_df.copy()   #Source Cycles Change by Recommendation because of Truck0's Completeness

Block_Selection_df_Copy['Recommended_SC(due to Volume) [days]'] = (100 * Block_Selection_df_Copy['SC'] / Block_Selection_df_Copy['Truck_Volume_Completeness_%']).apply(np.ceil, convert_dtype=True, args=())

Block_Selection_df_Copy['Recommended_SC + LT (due to Volume) [days]'] = Block_Selection_df_Copy['Recommended_SC(due to Volume) [days]'] + Block_Selection_df_Copy['LT']

Block_Selection_df_Copy['Recommended_SC (due to Weight) [days]'] = (100 * Block_Selection_df_Copy['SC'] / Block_Selection_df_Copy['Truck_Weight_Completeness_%']).apply(np.ceil, convert_dtype=True, args=())

Block_Selection_df_Copy['Recommended_SC + LT (due to Weight) [days]'] = Block_Selection_df_Copy['Recommended_SC (due to Weight) [days]'] + Block_Selection_df_Copy['LT']


for j in range(len(Block_Selection_df_Copy)):    #Deciding the greatest constraint (Weight or Volume)
  
  if (Block_Selection_df_Copy['Truck_Weight_Completeness_%'] - Block_Selection_df_Copy['Truck_Volume_Completeness_%']).values[j] >= 0:
      
      list_constraint.append(Block_Selection_df_Copy['Truck_Weight_Completeness_%'].values[j])
      list_name_constraint.append('WEIGHT')

  else:
      list_constraint.append(Block_Selection_df_Copy['Truck_Volume_Completeness_%'].values[j])
      list_name_constraint.append('VOLUME')


Block_Selection_df_Copy['Truck_Completeness_Constraint [%]'] = list_constraint
Block_Selection_df_Copy['Constraint_Type'] = list_name_constraint



k = 0
print('\n')
print('     Retailer    #Load_%   Load/Truck  #_Trucks')

for j in range(len(Block_Selection_df_Copy)):
    
    i = 1      #Number of Trucks

    while(129 < Block_Selection_df_Copy['Truck_Completeness_Constraint [%]'].values[j] / i):       # Only 1 Truck Threshold (129 [%])
                                                                                                   # Business constraint
        i = i + 1   #Number of Trucks                                                                                        
        
    x = Block_Selection_df_Copy['Truck_Completeness_Constraint [%]'].values[j] / i   

    if(50 <= x < 70):
        x = 70     # each 1 Truck
  
    if(70 <= x < 80):
        x = 80     # each 1 Truck

    if(80 <= x <= 100):
        x = x       # each 1 Truck

    if(100 < x <= 129):
        x = (100)  # each 1 Truck
    
    elif((x < 50) or (129 < x)):   #Wouldn't be necessary to consider the (129% < x) case due to the counting trucks decision-making   
        k = k + 1                  #process, but because of a sturdiness matter is also written down also in this decision-making process 
        x = x
        print(k, '_', Block_Selection_df_Copy['RET_ID'].iloc[j] , '  ',"%.2f" % Block_Selection_df_Copy['Truck_Completeness_Constraint [%]'].values[j] , '    ', "%.2f" % x, '       ',i)
    

    if 50 <= (Block_Selection_df_Copy['Truck_Completeness_Constraint [%]'].values[j] - x * i):
            
            y = Block_Selection_df_Copy['Truck_Completeness_Constraint [%]'].values[j] - x * i  
            j = i + 1
            list_Amount_For_Extra_Truck.append(y)
            Modified_list_Extra_Truck.append(j)

            if(50 <= y < 70):
                z = 70     # each 1 Truck
                list_Amount_For_Extra_Truck_Rounded.append(z)

            if(70 <= y < 80):
                z = 80     # each 1 Truck
                list_Amount_For_Extra_Truck_Rounded.append(z)

            if(80 <= y <= 100):
                z = y       # each 1 Truck
                list_Amount_For_Extra_Truck_Rounded.append(z)

            if(100 < y <= 129):
                z = 100     # each 1 Truck
                list_Amount_For_Extra_Truck_Rounded.append(z)
            
            elif((y < 50) or (129 < y)):
                k = k + 1
                z = y
                list_Amount_For_Extra_Truck_Rounded.append(z)

                print('Extra Amount Truck Case--------------------------------------------------------------')
                print(k, '_', Block_Selection_df_Copy['RET_ID'].iloc[j] , '  ',"%.2f" % Block_Selection_df_Copy['Truck_Completeness_Constraint [%]'].values[j] , '    ', "%.2f" % z, '       ',i)
                print('-------------------------------------------------------------------------------------') 



    else: 
            list_Amount_For_Extra_Truck.append(0)
            list_Amount_For_Extra_Truck_Rounded.append(0)
            Modified_list_Extra_Truck.append(0)

    list_completeness_percent.append(x)
    list_number_trucks.append(i)

print('\n')
print(list_Amount_For_Extra_Truck)

Block_Selection_df_Copy['Each_Truck_Completeness'] = list_completeness_percent
Block_Selection_df_Copy['Amount_justifies_an_Extra_Truck'] = list_Amount_For_Extra_Truck
Block_Selection_df_Copy['Load_%_For_The_Extra_Truck'] = list_Amount_For_Extra_Truck_Rounded
Block_Selection_df_Copy['#_Trucks'] = list_number_trucks
Block_Selection_df_Copy['Modified_#_Trucks'] = Modified_list_Extra_Truck

print('\n')

Block_Selection_df_Copy.tail(60)


# COMMAND ----------


# Format Transformation
Block_Selection_df_Copy_SPARK = spark.createDataFrame(Block_Selection_df_Copy)
display(Block_Selection_df_Copy_SPARK)


# COMMAND ----------

#Removing special characters from columns' names - Mount Point Saving Format requirement

New_List_of_columns = []
Old_List_of_columns = []

for j, column in enumerate(sorted(Block_Selection_df_Copy_SPARK.columns), 0):
    
    Old_List_of_columns.append(column)

    if '__' or '/' or '(' or '%' or ' ' or '\'' in column:
        column = column.replace('(', '_')
        column = column.replace(')', '_')
        column = column.replace(' ', '_')
        column = column.replace('%', 'PERCENT')
        column = column.replace('\'', '_')
        column = column.replace('__', '_')
        column = column.replace('-', '_')
        column = column.replace('/', '_')
        New_List_of_columns.append(column)
    
    #if ')' in column:
        #column = column.replace('(', '_')
        #column = column.replace(')', '_')
        #New_List_of_columns.append(column)
    
    else:
        New_List_of_columns.append(column)
    
    print(j+1, ')', Old_List_of_columns[j])
    print(j+1, ')', New_List_of_columns[j])
    print('\n')
    
   
    Block_Selection_df_Copy_SPARK = Block_Selection_df_Copy_SPARK.withColumnRenamed(Old_List_of_columns[j], New_List_of_columns[j])

# COMMAND ----------

# MAGIC %md #María Isabel's Contribuitions 2

# COMMAND ----------

# Guardado final de los datos
w_trucks_table = 'cpfr_solution.tb_cpfr_trucks_and_opt'

Block_Selection_df_Copy_SPARK.write.format("delta").mode("overwrite").option('mergeSchema', True).saveAsTable(w_trucks_table, path="/mnt/cpfr/solutions/tb_cpfr_trucks_and_opt")

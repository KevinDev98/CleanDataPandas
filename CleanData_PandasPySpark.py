# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 15:34:55 2023

@author: kevin.gutierrez
"""
import sys
import findspark
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, BooleanType, DecimalType, DoubleType, DateType
import pandas as pd
import re
from datetime import date
from datetime import datetime
import pyspark.sql.functions as pyf #funciones para maniputar df de pyspark

index=0
pathIn="C:\\Users\\kevin.gutierrez\\Documents\\PROYECTOS\\IDS AZURE\\FASE 3\\"
pathOut="C:\\Users\\kevin.gutierrez\\Documents\\PROYECTOS\\IDS AZURE\\FASE 3\\out\\"
#-----------------------Conexión a spark
def FindSparkValidate():
    FS=False
    try:                
       findspark.init()
       FS=True
    except Exception as ex:
        print("Ocurrio un error en el proceso {}".format(ex))
    return FS
def Spark_Session():
    FS1=FindSparkValidate()
    if (FS1==False):
        sys.exit()
    else:           
       try:
           SessionSpark=SparkSession.builder.master("local[*]").appName("SessionCat").getOrCreate()
       except Exception as ex:
           print("Error Creando sesion SQL: {}".format(ex))        
    return SessionSpark
def SparkSql(spark):
    try:        
        sparksql = SQLContext(spark)        
    except Exception as ex:
        print("Error Creando contexto SQL: {}".format(ex))
    return sparksql
#------------------------DEPURACIÓN DE DATOS
def Remove_duplicates(df):
    non_dup_df = df.dropDuplicates()
    return non_dup_df
def Remove_null_zero(df):
    df_not_nan = df.dropna()
    return df_not_nan    
def Fill_null_values(s):
    s=str(s) #Transform to string so that value can be read
    try:
        pass
        if len(s)==0:
            s="---"
        elif s=="":
            s="---"
        elif s is None:
            s="---"
        elif s=="None":
            s="---"
        elif s=="":
            s="---"
    except Exception as e:
        pass
        s=e
    finally:
        pass
        #print("nulos: " + str(s))
    return s    
def Remove_spacewhite(s): #this method receives a string for spaces white remove
    try: 
        s=str(s) #Transform to string so that value can be read
        s=s.lstrip().rstrip().strip()#remove white spaces of string
    except Exception as ex:
        s='Error' #Error is returned
        print('Error Cleanning: {}' .format(ex))
    return s
def Remove_Special_Characters(s):#this method receives a string for special characteres remove
    s=str(s)#Transform to string so that value can be read
    #s=s.replace(" ","")    
    CaracteresEspeciales=["#",'$','%','&','!','|','[',']','{','}','/','_',';',':',',','*',"-","'"] #Special Characteres List   
    try:
        for z in s: #loop through each character of recived word
            if(z in CaracteresEspeciales):  #Validate if the current character is in the Special Characteres list
                try:                    
                    s=s.replace(z,'')#the special character is removed
                except Exception as ex:                
                    print('error replace: {}'.format(ex))
        return s
    except Exception as ex:
        print('error replace 0: {}'.format(ex)) 

def GenerateDate(dte, formatdateorigin, formatdatedestiny,index):
    formatdatedestiny=str(formatdatedestiny)
    formatdateorigin=str(formatdateorigin)
    #print("origen: "+formatdateorigin)
    #print("destino: "+formatdatedestiny)
    #try:
        #pass
    if formatdatedestiny=="dd/mm/yyyy":
        pass        
        if formatdateorigin=="ddmmyyyy":
            pass
            dte = datetime.strptime(dte, '%d%m%Y').strftime('%d/%m/%Y')
        elif formatdateorigin=="yyyymmdd":
            pass 
            dte = datetime.strptime(dte, '%Y%m%d').strftime('%d/%m/%Y')
    elif formatdatedestiny=="yyyy/mm/dd":
        pass
        if formatdateorigin=="ddmmyyyy":
            pass
            dte = datetime.strptime(dte, '%d%m%Y').strftime('%Y/%m/%d')
        elif formatdateorigin=="yyyymmdd":
            pass 
            dte = datetime.strptime(dte, '%Y%m%d').strftime('%Y/%m/%d')
    elif formatdatedestiny=="dd-mm-yyyy":
        pass
        if formatdateorigin=="ddmmyyyy":
            pass
            dte = datetime.strptime(dte, '%d%m%Y').strftime('%Y-%m-%d')
        elif formatdateorigin=="yyyymmdd":
            pass 
            dte = datetime.strptime(dte, '%Y%m%d').strftime('%Y-%m-%d')
    elif formatdatedestiny=="yyyy-mm-dd":
        pass
        if formatdateorigin=="ddmmyyyy":
            pass
            dte = datetime.strptime(dte, '%d%m%Y').strftime('%Y-%m-%d')
        elif formatdateorigin=="yyyymmdd":
            pass 
            dte = datetime.strptime(dte, '%Y%m%d').strftime('%Y-%m-%d')
    elif formatdatedestiny=="mm-dd-yyyy":
        pass
        if formatdateorigin=="ddmmyyyy":
            pass
            dte = datetime.strptime(dte, '%d%m%Y').strftime('%m-%d-%Y')
        elif formatdateorigin=="yyyymmdd":
            pass 
            dte = datetime.strptime(dte, '%Y%m%d').strftime('%m-%d-%Y')
    elif formatdatedestiny=="mm/dd/yyyy":
        pass
        if formatdateorigin=="ddmmyyyy":
            pass
            dte = datetime.strptime(dte, '%d%m%Y').strftime('%m/%d/%Y')
        elif formatdateorigin=="yyyymmdd":
            pass 
            dte = datetime.strptime(dte, '%Y%m%d').strftime('%m/%d/%Y')
    ######################################################
    elif formatdatedestiny=="dd/mm/yyyy hh:mm:ss":
        pass        
        if formatdateorigin=="ddmmyyyy hhmmss":
            pass
            dte = datetime.strptime(dte, '%d%m%Y%H%M%S').strftime('%d/%m/%Y %H:%M:%S')
        elif formatdateorigin=="yyyymmdd hhmmss":
            pass 
            dte = datetime.strptime(dte, '%Y%m%d%H%M%S').strftime('%d/%m/%Y %H:%M:%S')
    elif formatdatedestiny=="yyyy/mm/dd hh:mm:ss":
        pass
        if formatdateorigin=="ddmmyyyy hhmmss":
            pass
            dte = datetime.strptime(dte, '%d%m%Y%H%M%S').strftime('%Y/%m/%d %H:%M:%S')
        elif formatdateorigin=="yyyymmdd hhmmss":
            pass 
            dte = datetime.strptime(dte, '%Y%m%d%H%M%S').strftime('%Y/%m/%d %H:%M:%S')
    elif formatdatedestiny=="dd-mm-yyyy hh:mm:ss":
        pass
        if formatdateorigin=="ddmmyyyy hhmmss":
            pass
            dte = datetime.strptime(dte, '%d%m%Y%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        elif formatdateorigin=="yyyymmdd hhmmss":
            pass 
            dte = datetime.strptime(dte, '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
    elif formatdatedestiny=="yyyy-mm-dd hh:mm:ss":
        pass
        if formatdateorigin=="ddmmyyyy hhmmss":
            pass
            dte = datetime.strptime(dte, '%d%m%Y%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        elif formatdateorigin=="yyyymmdd hhmmss":
            pass 
            dte = datetime.strptime(dte, '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
    elif formatdatedestiny=="mm-dd-yyyy hh:mm:ss":
        pass
        if formatdateorigin=="ddmmyyyy hhmmss":
            pass
            dte = datetime.strptime(dte, '%d%m%Y%H%M%S').strftime('%m-%d-%Y %H:%M:%S')
        elif formatdateorigin=="yyyymmdd hhmmss":
            pass 
            dte = datetime.strptime(dte, '%Y%m%d%H%M%S').strftime('%m-%d-%Y %H:%M:%S')
    elif formatdatedestiny=="mm/dd/yyyy hh:mm:ss":
        pass
        if formatdateorigin=="ddmmyyyy hhmmss":
            pass
            dte = datetime.strptime(dte, '%d%m%Y%H%M%S').strftime('%m/%d/%Y %H:%M:%S')
        elif formatdateorigin=="yyyymmdd hhmmss":
            pass 
            dte = datetime.strptime(dte, '%Y%m%d%H%M%S').strftime('%m/%d/%Y %H:%M:%S')
    #except Exception as ex:
        #pass        
        # errordata.append(index) #index is added to the error list
        # desc_errores.append("Formato de fecha invalido. " + str(dte))#description is added to the desc_error list
        #print('error fechas: {}'.format(ex))      
    return dte
def Format_Decimal(z):
    
    z=str(z).replace(" ","")# first is transformed to string so thye removes whitespace
    z=float(z)#is transformed to "float" to format as decimal
    z=round(z,2)#limited to 2 decimal places
    return z        

def Validate_Names(name, index): # They can only be letters, "-" and "'"
    name=str(name)#Transform to string so that value can be read
    name=name.replace(' ','').replace('-','').replace("'",'') #remove these values so that the name isn't invalid
    TorF=name.isalpha()#Validate if the name contains only letters
    if(TorF==False):
        errordata.append(index) #index is added to the error list
        desc_errores.append("Nombre con formato incorrecto")#description is added to the desc_error list
    return TorF
def Validate_phone(s, lngt, index):#This method receives a string, a length, and an index number to validate a phone number.
    try:
        s=str(s)#Transform to string so that value can be read
        s=s.replace(" ","").replace("-","") #hyphens and spaces whites are removed
        TorF=True
        for z in s:#loop through each digit of the phone number
            if (z.isnumeric()==False):
                #print("Número telefonico incorrecto, solo numeros:",z,'--',s)  
                TorF=False
        lngt=int(lngt)
        if (len(s)!=lngt):
            #print("Número telefonico incorrecto, debe tener una longitud de ", lngt)
            TorF=False
        if (TorF==False):
            errordata.append(index) #index is added to the error list
            desc_errores.append("Numero telefonico incorrecto")#description is added to the desc_error list
        return s
    except Exception as ex:
        print('error tel: {}'.format(ex))
def Validate_Email(s, index): #This method receives a string and an index number to validate an email
    s=str(s)
    regex = r"(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])"    
    #if re.match('^[(a-z0-9\_\-\.)]+@[(a-z0-9\_\-\.)]+\.[(a-z)]{2,15}$',s.lower()):
    if re.match(regex, s.lower()): #loop through each letter of the email
        arroba=0
        for a in s:
            if (a=="@"):#validate if have a @
                arroba=arroba+1 #count the number of @ that have the email
        if (arroba==0 or arroba>1): #if has more than a @ or none at all, return false
            errordata.append(index) #index is added to the error list
            desc_errores.append("email incorrecto @")#description is added to the desc_error list
            return False
        return True
    else:
        errordata.append(index) #index is added to the error list
        desc_errores.append("email con formato incorrecto")   #description is added to the desc_error list     
        return False
def Validate_rfc(z, index): #his method receives a string and an index number to validate an RFC
    z=str(z)
    regex_str="^([A-ZÑ\x26]{3,4}([0-9]{2})(0[1-9]|1[0-2])(0[1-9]|1[0-9]|2[0-9]|3[0-1]))([A-Z\d]{3})?$"
    if (len(z)<11 or len(z)>13 or len(z)==12): #validate length
        errordata.append(index) #index is added to the error list
        desc_errores.append("RFC con formato incorrecto")#description is added to the desc_error list
        return False
    if re.match(regex_str, z): #Validate Format
        return True
    else:
        errordata.append(index) #index is added to the error list
        desc_errores.append("RFC con formato incorrecto")#description is added to the desc_error list
        return False

    TorF=True
    index=+1
    try:
        z=float(z)# Convert Number to float    
        if z<0: #if the value is less that zero than, returns False
            TorF=False
            errordata.append(index) #index is added to the error list
            desc_errores.append("Monto menor a 0")#description is added to the desc_error list
        return TorF 
    except Exception as ex:
        errordata.append(index) #index is added to the error list
        desc_errores.append("monto menor a 0:", ex)#description is added to the desc_error list
        print('Error flotante: {}'.format(ex))    
def Validate_Amounts(z,index):
    TorF=True
    try:
        z=float(z)# Convert Number to float    
        if z<0: #if the value is less that zero than, returns False
            TorF=False
            errordata.append(index) #index is added to the error list
            desc_errores.append("Monto menor a 0")#description is added to the desc_error list
        return TorF 
    except Exception as ex:
        errordata.append(index) #index is added to the error list
        desc_errores.append("monto menor a 0:", ex)#description is added to the desc_error list
        print('Error flotante: {}'.format(ex))

#------------------FUNCIONES SPARK
def create_df(spark_s, path_f):
    try:
        pass
        if (path_f.endswith(".csv")):
            DF_info=spark_s.read.option('header','true').option('delimiter', ',').option('inferSchema','true').csv(path_f)
        if(path_f.endswith(".parquet")):
            DF_info=spark_s.read.parquet(path_f)
        return DF_info
    except Exception as ex:
        pass    
        print('error creando dataframe con spark: {}'.format(ex)) 
        spark_s.stop()           
    
#-------------------------INICIA SPARK     
try:        
    sparksession=Spark_Session()
    print("Sesion de spark iniciada")
    sprksql=SparkSql(sparksession) 
    print("Contexto sparksql creado")
except Exception as ex:
    print('error iniciando spark: {}'.format(ex))  
#-----------------------INICIA PROCESO
try:
    pass    
    errordata=[]
    desc_errores=[]
    select_col=[]
    
    Remove_spacewhite_udf=udf(lambda z: Remove_spacewhite(z), StringType())    
    Remove_Special_Characters_udf=udf(lambda z: Remove_Special_Characters(z), StringType())
    Fill_null_values_udf=udf(lambda z: Fill_null_values(z), StringType())
    
    df_data_spk_pd=create_df(sparksession,pathIn+'Merge_data_raw.csv')
    print("conteo original: "+str(df_data_spk_pd.count()))
    df_data_spk_pd=Remove_duplicates(df_data_spk_pd)        
    print("conteo sin duplicados: "+str(df_data_spk_pd.count()))
      
    row_num=df_data_spk_pd.count()
    #---------------------apply udf
    df_data_spk_pd=df_data_spk_pd.select([Remove_spacewhite_udf(column).alias(str(column)) for column in df_data_spk_pd.columns])#.show(1000,truncate=False)           
    df_data_spk_pd=df_data_spk_pd.select([Remove_Special_Characters_udf(column).alias(str(column)) for column in df_data_spk_pd.columns])
    #df_data_spk_pd=df_data_spk_pd.select([Fill_null_values_udf(column).alias(str(column)) for column in df_data_spk_pd.columns])
    
    coldatatype=df_data_spk_pd.dtypes #
    #print(coldatatype)
    df_data_spk_pd=df_data_spk_pd.toPandas() #Convert DF Spark to DF Pandas

    
    for colu in coldatatype:        
        pass         
        colname=str(colu[0])
        datatype=str(colu[1])
        #print(str(colu) + "col "+ colname + " datyp " + datatype)  
        #print(colu)
        index1=0   #reset index
        try:
            pass
            for row in df_data_spk_pd.iterrows(): #loop through each line of the dataframe                                                 
                try:
                    data=df_data_spk_pd.loc[index1,colname]  
                    data=str(data)
                    data=Fill_null_values(data)
                    
                    if colname.__contains__("fecha") | colname.__contains__("date"):
                        if data!="---":                            
                            try:
                                pass
                                df_data_spk_pd.loc[index1,colname]=GenerateDate(data, "ddmmyyyy","dd/mm/yyyy",index1)
                            except Exception as e:
                                pass
                                try:
                                    pass
                                    print("fallo en el primer intento")
                                    df_data_spk_pd.loc[index1,colname]=GenerateDate(data, "yyyymmdd","dd/mm/yyyy",index1)
                                except Exception as e:
                                    pass
                                    print("fallo en el segundo intento")
                                    errordata.append(index) #index is added to the error list
                                    desc_errores.append("Formato de fecha invalido. " + str(data))#description is added to the desc_error list                            
                                    print("Formato de fecha invalido. " + str(data))
                        else:
                            errordata.append(index) #index is added to the error list
                            desc_errores.append("Formato de fecha invalido. " + str(data))#description is added to the desc_error list                            
                    elif colname.__contains__("tel") | colname.__contains__("phone"):
                        if(Validate_phone(data, 10, index1)==False):
                            print('Malformed Phone number: ', data)                        
                    elif colname.__contains__("nombre") | colname.__contains__("name"):
                        if(Validate_Names(data, index1)==False):
                            print('Malformed name: ', data)
                    elif colname.__contains__("correo") | colname.__contains__("email"):
                        if(Validate_Email(data, index1)==False):
                            print('Malformed email: ', data) 
                    elif colname.__contains__("rfc"):
                        if(Validate_rfc(data, index1)==False):
                            print('Malformed rfc: ', data)
                    
                    if datatype in ["int", "long", "float", "double"]:
                        if(Validate_Amounts(data, index1)==False):
                            print('Malformed amount: ', data)
                    elif datatype in ["float", "double"]:
                        df_data_spk_pd.loc[index1,colname]=Format_Decimal(data)
                    
                    index1=index1+1   
                except Exception as ex:
                    pass      
                    #print('error validando datos con pandas: {}'.format(ex) +" col: " + colname +" dty: "+ datatype + " data: "+data + " index: " + str(index1))
                    print('error validando datos con pandas: {}'.format(ex) +" col: " + colname + " data: "+data + " index: " + str(index1))
                    sparksession.stop()
                    #break
                        
        except Exception as ex:            
            pass        
            print('error validando columnas con pandas: {}'.format(ex) +" col: " + colname + " index: " + str(index1))
    now = str(date.today())
    #now=str(datetime.now())
    now=now.replace('-','').replace(':','').replace('.', '').replace(' ','_')
    
    try:
        pass        
        df_clean=df_data_spk_pd.drop(df_data_spk_pd.index[errordata])
        df_dirty=df_data_spk_pd.loc[errordata] #Only the rows that have some erroneous data are saved
        df_dirty["Desc_error"]=desc_errores
        
        if (df_clean.shape[0]>0):
            print("DATA CLEAN")
            df_clean.to_csv(pathOut+"outfile_DataClean_"+now+".csv", index=False)#the csv file is generated with the corresponding data
        if (df_dirty.shape[0]>0):
            print("DATA REJECTED")
            df_dirty.to_csv(pathOut+"outfile_Data_DirtyRejected"+now+".csv", index=False)#the csv file is generated with the corresponding data    
        
        print("Reglas aplicadas correctamente")
    except Exception as ex:
        pass
        print('error generando archivos: {}'.format(ex))    
        sparksession.stop()
    sparksession.stop()
except Exception as ex:
    pass    
    print('error depurando datos con spark: {}'.format(ex))
    sparksession.stop()

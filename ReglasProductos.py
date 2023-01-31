# -*- coding: utf-8 -*-
"""
@author: kevin.gutierrez
"""

import pandas as pd
import sys
from datetime import date
from datetime import datetime

pathIn="C:\\Users\\kevin.gutierrez\\Documents\\PROYECTOS\\IDS AZURE\\Reglas\\Input\\"
pathOut="C:\\Users\\kevin.gutierrez\\Documents\\PROYECTOS\\IDS AZURE\\Reglas\\Output\\"
df_reglas=pd.read_csv(pathIn+'Productos2.csv')
df_reglas=pd.DataFrame(df_reglas)
"""
VALIDACIONES GENERALES
"""
def Remove_spacewith(s): #this method receives a string for spaces white remove
    try: 
        s=str(s) #Transform to string so that value can be read
        s=s.lstrip().rstrip().strip()#remove white spaces of string
    except Exception as ex:
        s='Error' #Error is returned
        print('Error Cleanning: {}' .format(ex))
    return s

def IsNumeric(number,index): #this method receives a number and the index number for Validate if it is number
    try:
        number=int(number)#trie to convert the recived data to int,to validate if the data is number
        if type(number)==int:#If data type is number then returned true
            return True
    except Exception as ex:#if data type isn't number then returned False and the error is add to the error list
        errordata.append(index) #index is added to the error list
        desc_errores.append("dato no númerico")#description is added to the desc_error list
        return False
        print('Error numerico: {}'.format(ex))

def Remove_Special_Characters(s):#this method receives a string for special characteres remove
    s=str(s)#Transform to string so that value can be read
    #s=s.replace(" ","")
    CaracteresEspeciales=["#",'$','%','&','!','|','[',']','{','}','/','_',';',':',',','*'] #Special Characteres List
    try:
        for z in s: #loop through each character of recived word
            if(z in CaracteresEspeciales):  #Validate if the current character is in the Special Characteres list
                try:                    
                    s=s.replace(z,'')#the special character is removed
                except Exception as ex:                
                    print('error replace: {}'.format(ex))
                    sys.exit()
        return s
    except Exception as ex:
        print('error replace 0: {}'.format(ex))

def Validate_not_greater_today(dte,index): #This method receives a index number and a date to validate that it is not greater than today
    try:
        now=str(date.today())#it is converted to string to give it the format dd/MM/yyyy
        now=datetime.strptime(now, "%Y-%m-%d").strftime("%d/%m/%Y")#it give format dd/MM/yyyy
        now=datetime.strptime(now, "%d/%m/%Y")#convert to date data type again so than comparative
        dte=datetime.strptime(dte, "%d/%m/%Y")#convert to date data type again so than comparative
        if(dte>now):
            errordata.append(index) #index is added to the error list
            desc_errores.append("Fecha invalida, no puede ser mayor a hoy")#description is added to the desc_error list
    except Exception as ex:
        errordata.append(index) #index is added to the error list
        desc_errores.append("Fecha invalida, no puede ser mayor a hoy")#description is added to the desc_error list
        print('error comparando fechas:', ex)

def Change_Date_Format(dte, index):#This method receives a index number and a date for transformate the format date    
    try:
        dte=str(dte)#convert to string
        dte=Remove_Special_Characters(Remove_spacewith(dte))#data dirty is remove
        dte=datetime.strptime(dte, "%Y-%m-%d").strftime("%d/%m/%Y")#str(datetime.strptime(dte, "%Y-%m-%d").strftime("%d/%m/%Y"))
    except Exception as ex:
        errordata.append(index) #index is added to the error list
        desc_errores.append("Formato de fecha invalido. Debe tener formato dd/MM/yyyy")#description is added to the desc_error list
        print('error fechas: {}'.format(ex))        
    return str(dte)

def Validate_Decimal(s,index):
    try:
        s=type(s)
        return s
    except Exception as ex:
        errordata.append(index) #index is added to the error list
        desc_errores.append("dato no decimal")#description is added to the desc_error list
        #return False
        print('error decimal: {}'.format(ex))
    except Exception as ex:
        errordata.append(index) #index is added to the error list
        desc_errores.append("dato no númerico")#description is added to the desc_error list
        print('Error numerico: {}'.format(ex))

"""
--------------------------------------------
"""


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
        
def Format_Decimal(z):
    z=str(z).replace(" ","")# first is transformed to string so thye removes whitespace
    z=float(z)#is transformed to "float" to format as decimal
    z=round(z,2)#limited to 2 decimal places
    return z

###########################################################

try:
     index1=0   
     errordata=[]
     desc_errores=[]
     for index, row in df_reglas.iterrows(): #loop through each line of the csv
         try:                             
             #VALIDA ID
             df_reglas.loc[index1,"IdProducto"]=Remove_spacewith(row.IdProducto)
             if (IsNumeric(df_reglas.loc[index1,"IdProducto"],index1)==False):
                 print('non-numeric id:**', df_reglas.loc[index1,"IdProducto"],'**')
                 #sys.exit()
             
             #VALIDATE NOMBRE
             df_reglas.loc[index1,"NombreProd"]=Remove_Special_Characters(Remove_spacewith(row.NombreProd))                                                      
             #VALIDATE MARCA    
             df_reglas.loc[index1,"marca"]=Remove_Special_Characters(Remove_spacewith(row.marca))
             #VALIDATE CONTENIDO
             df_reglas.loc[index1,"Contenido"]=Remove_Special_Characters(Remove_spacewith(row.Contenido))
             
             #VALIDATE PRECIO
             df_reglas.loc[index1,"Precio"]=Remove_Special_Characters(Remove_spacewith(row.Precio))  
             try:
                 price=df_reglas.loc[index1,"Precio"]
                 df_reglas.loc[index1,"Precio"]=Format_Decimal(df_reglas.loc[index1,"Precio"])#if the decimal is valid, then limit the amount of decimal                 
                 if Validate_Amounts(df_reglas.loc[index1,"Precio"],index1)==False:
                     print("No se permiten valores menor a 0")
             except Exception as ex:
                 print('Malformed Precio:',df_reglas.loc[index1,"Precio"],'data type:', type(price),'error: ', ex) 
                          
             try:
                 df_reglas.loc[index1,"FechaRegistro"]=Change_Date_Format(row.FechaRegistro,index1)
                 Validate_not_greater_today(df_reglas.loc[index1,"FechaRegistro"],index1)
                 #print('row.FechaRegistro:',row.FechaRegistro)
             except Exception as ex:
                 errordata.append(index1) #index is added to the error list
                 desc_errores.append("Formato de fecha invalido. Debe tener formato dd/MM/yyyy")#description is added to the desc_error list
                 print('error fechas: {}'.format(ex))
                 
             index1=index1+1
         except Exception as ex:
             print('Proceso interrumpido Error data: {}' .format(ex))  
             sys.exit()
     try:
         df_clean=df_reglas.drop(df_reglas.index[errordata])
         df_dirty=df_reglas.loc[errordata] #Only the rows that have some erroneous data are saved
         df_dirty["Desc_error"]=desc_errores
         now = str(date.today())
         now=now.replace('-','').replace(':','').replace('.', '').replace(' ','_')
         if (df_clean.shape[0]>0):
             df_clean.to_csv(pathOut+"outf_ProductosLimpios"+now+".csv", index=False)#the csv file is generated with the corresponding data
         if (df_dirty.shape[0]>0):
             df_dirty.to_csv(pathOut+"outf_ProductosSucios"+now+".csv", index=False)#the csv file is generated with the corresponding data
         print("Reglas aplicadas correctamente")
     except Exception as ex:
         print('error rules: {}'.format(ex))
except Exception as ex:    
    print('Error : {}' .format(ex)) 
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

df_reglas=pd.read_csv(pathIn+'Ventas2.csv')
df_reglas=pd.DataFrame(df_reglas)
"""
VALIDACIONES GENERALES
"""
def Remove_spacewhite(s): #this method receives a string for spaces white remove
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
        print('error comparando fechas:', ex)

def Change_Date_Format(dte, index):#This method receives a index number and a date for transformate the format date    
    try:
        dte=str(dte)#convert to string
        dte=Remove_Special_Characters(Remove_spacewhite(dte))#data dirty is remove
        dte=datetime.strptime(dte, "%Y-%m-%d").strftime("%d/%m/%Y")#str(datetime.strptime(dte, "%Y-%m-%d").strftime("%d/%m/%Y"))
    except Exception as ex:
        errordata.append(index) #index is added to the error list
        desc_errores.append("Formato de fecha invalido. Debe tener formato dd/MM/yyyy")#description is added to the desc_error list
        print('error fechas: {}'.format(ex))        
    return str(dte)
"""
--------------------------------------------
"""

def Validate_Names(a, index): # They can only be letters, "-" and "'"
    a=str(a)#Transform to string so that value can be read
    a=a.replace(' ','').replace('-','').replace("'",'') #remove these values so that the name isn't invalid
    TorF=a.isalpha()#Validate if the name contains only letters
    if(TorF==False):
        errordata.append(index) #index is added to the error list
        desc_errores.append("Nombre con formato incorrecto")#description is added to the desc_error list
    return TorF

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

###################################
try:
     index1=0   
     errordata=[]
     desc_errores=[]
     for index, row in df_reglas.iterrows(): #loop through each line of the csv
         try:                             
             #VALIDA ID
             df_reglas.loc[index1,"IdVenta"]=Remove_spacewhite(row.IdVenta)
             if (IsNumeric(df_reglas.loc[index1,"IdVenta"], index1)==False): #Validate if it is numeric
                 print('non-numeric id:**', df_reglas.loc[index1,"IdVenta"],'**')
                 #sys.exit()
             
             try:
                 df_reglas.loc[index1,"FechaVenta"]=Change_Date_Format(row.FechaVenta,index1)                 
                 Validate_not_greater_today(df_reglas.loc[index1,"FechaVenta"],index1)                
                 
             except Exception as ex:
                 errordata.append(index1) #index is added to the error list
                 desc_errores.append("Formato de fecha invalido. Debe tener formato dd/MM/yyyy")#description is added to the desc_error list
                 print('error fechas: {}'.format(ex))
             
             #VALIDATE MontoVenta
             df_reglas.loc[index1,"MontoVenta"]=Remove_Special_Characters(Remove_spacewhite(row.MontoVenta))  
             try:
                 MontoVenta=df_reglas.loc[index1,"MontoVenta"]
                 df_reglas.loc[index1,"MontoVenta"]=Format_Decimal(df_reglas.loc[index1,"MontoVenta"])#if the decimal is valid, then limit the amount of decimal                 
                 if Validate_Amounts(df_reglas.loc[index1,"MontoVenta"],index1)==False: #Negative numbers are not allowed
                     print("No se permiten valores menor a 0")
             except Exception as ex:
                 print('Malformed MontoVenta:',df_reglas.loc[index1,"MontoVenta"],'data type:', type(MontoVenta),'error: ', ex) 
             
             #VALIDATE CantidadVenta
             df_reglas.loc[index1,"CantidadVenta"]=Remove_Special_Characters(Remove_spacewhite(row.CantidadVenta))  
             try:
                 CantidadVenta=df_reglas.loc[index1,"CantidadVenta"]
                 df_reglas.loc[index1,"CantidadVenta"]=Format_Decimal(df_reglas.loc[index1,"CantidadVenta"])#if the decimal is valid, then limit the amount of decimal                 
                 if Validate_Amounts(df_reglas.loc[index1,"CantidadVenta"],index1)==False: #Negative numbers are not allowed
                     print("No se permiten valores menor a 0")
             except Exception as ex:
                 print('Malformed CantidadVenta:',df_reglas.loc[index1,"CantidadVenta"],'data type:', type(CantidadVenta),'error: ', ex)    
             
             #VALIDATE Descuento
             df_reglas.loc[index1,"Descuento"]=Remove_Special_Characters(Remove_spacewhite(row.Descuento))  
             try:
                 Descuento=df_reglas.loc[index1,"Descuento"]
                 df_reglas.loc[index1,"Descuento"]=Format_Decimal(df_reglas.loc[index1,"Descuento"])#if the decimal is valid, then limit the amount of decimal                 
                 if Validate_Amounts(df_reglas.loc[index1,"Descuento"],index1)==False:#Negative numbers are not allowed
                     print("No se permiten valores menor a 0")
                 
                 if float(Descuento)>100:
                     errordata.append(index) #index is added to the error list
                     desc_errores.append("No se permite descuentos mayores al 100%")#description is added to the desc_error list
                     print("No se permiten valores mayores a 100")
                     
             except Exception as ex:
                 print('Malformed Descuento:',df_reglas.loc[index1,"Descuento"],'data type:', type(Descuento),'error: ', ex)    
             
             #VALIDATE MontoFinalVenta
             df_reglas.loc[index1,"MontoFinalVenta"]=Remove_Special_Characters(Remove_spacewhite(row.MontoFinalVenta))  
             try:
                 MontoFinalVenta=df_reglas.loc[index1,"MontoFinalVenta"]
                 df_reglas.loc[index1,"MontoFinalVenta"]=Format_Decimal(df_reglas.loc[index1,"MontoFinalVenta"])#if the decimal is valid, then limit the amount of decimal                 
                 if Validate_Amounts(df_reglas.loc[index1,"MontoFinalVenta"],index1)==False:#Negative numbers are not allowed
                     print("No se permiten valores menor a 0")
             except Exception as ex:
                 print('Malformed MontoFinalVenta:',df_reglas.loc[index1,"MontoFinalVenta"],'data type:', type(MontoFinalVenta),'error: ', ex)              
             
             #VALIDA fk_Producto
             df_reglas.loc[index1,"fk_producto"]=Remove_spacewhite(row.fk_producto)
             if (IsNumeric(df_reglas.loc[index1,"fk_producto"], index1)==False):#Validate if it is numeric
                 print('non-numeric fk_producto:**', df_reglas.loc[index1,"fk_producto"],'**')
             
             #VALIDA fk_cliente
             df_reglas.loc[index1,"fk_cliente"]=Remove_spacewhite(row.fk_cliente)
             if (IsNumeric(df_reglas.loc[index1,"fk_cliente"], index1)==False):#Validate if it is numeric
                 print('non-numeric fk_cliente:**', df_reglas.loc[index1,"fk_cliente"],'**')
             
             #VALIDA fk_sucursal
             df_reglas.loc[index1,"fk_sucursal"]=Remove_spacewhite(row.fk_sucursal)
             if (IsNumeric(df_reglas.loc[index1,"fk_sucursal"], index1)==False):#Validate if it is numeric
                 print('non-numeric fk_sucursal:**', df_reglas.loc[index1,"fk_sucursal"],'**')
                                                                                                                                                  
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
             df_clean.to_csv(pathOut+"outf_VentasLimpios"+now+".csv", index=False)#the csv file is generated with the corresponding data
         if (df_dirty.shape[0]>0):
             df_dirty.to_csv(pathOut+"outf_VentasSucios"+now+".csv", index=False)#the csv file is generated with the corresponding data
         print("Reglas aplicadas correctamente")
     except Exception as ex:
         print('error rules: {}'.format(ex))
except Exception as ex:    
    print('Error : {}' .format(ex)) 
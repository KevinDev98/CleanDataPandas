# -*- coding: utf-8 -*-
"""
@author: kevin.gutierrez
"""

import pandas as pd
import sys
import re
from datetime import date
from datetime import datetime

pathIn="C:\\Users\\kevin.gutierrez\\Documents\\PROYECTOS\\IDS AZURE\\Reglas\\Input\\"
pathOut="C:\\Users\\kevin.gutierrez\\Documents\\PROYECTOS\\IDS AZURE\\Reglas\\Output\\"

df_reglas=pd.read_csv(pathIn+'Clientes2.csv')
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
        errordata.append(index) #index is added to the error list
        desc_errores.append("Fecha invalida, no puede ser mayor a hoy")#description is added to the desc_error list
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
        for z in s:#loop through each digit of the phone number
            if (z.isnumeric()==False):
                #print("Número telefonico incorrecto, solo numeros:",z,'--',s)  
                s="x1000"
        lngt=int(lngt)
        if (len(s)!=lngt):
            #print("Número telefonico incorrecto, debe tener una longitud de ", lngt)
            s="x1000"
        if (s=="x1000"):
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

###################################
try:
     index1=0   
     errordata=[]
     desc_errores=[]
     for index, row in df_reglas.iterrows(): #loop through each line of the csv
         try:                             
             #VALIDA ID
             df_reglas.loc[index1,"IdCliente"]=Remove_spacewhite(row.IdCliente)
             if (IsNumeric(df_reglas.loc[index1,"IdCliente"], index1)==False):#Validate if it is numeric
                 print('non-numeric id:**', df_reglas.loc[index1,"IdCliente"],'**')
                 #sys.exit()
             
             #VALIDA NOMBRE
             df_reglas.loc[index1,"Nombre"]=Remove_Special_Characters(Remove_spacewhite(row.Nombre))            
             if (Validate_Names(df_reglas.loc[index1,"Nombre"],index1)==False):
                 print('Malformed Name:**',df_reglas.loc[index1,"Nombre"],'**')                 
             
             #VALIDA NÚMERO TELEFONICO
             df_reglas.loc[index1,"NumeroContacto"]=Remove_Special_Characters(Remove_spacewhite(row.NumeroContacto))
             if(Validate_phone(df_reglas.loc[index1,"NumeroContacto"],10,index1)=="x1000"):
                 print('Malformed Phone number: ', df_reglas.loc[index1,"NumeroContacto"])
                 #sys.exit()
                 
             df_reglas.loc[index1,"Estado"]=Remove_Special_Characters(Remove_spacewhite(row.Estado))
             df_reglas.loc[index1,"AlcaldiaMunicipio"]=Remove_Special_Characters(Remove_spacewhite(row.AlcaldiaMunicipio))
             
             #VALIDA RFC
             df_reglas.loc[index1,"RFC"]=Remove_Special_Characters(Remove_spacewhite(row.RFC))  
             if(Validate_rfc(df_reglas.loc[index1,"RFC"],index1)==False):
                 print('Malformed RFC: ',df_reglas.loc[index1,"RFC"])
             
             #VALIDA EMAIL
             df_reglas.loc[index1,"email"]=Remove_Special_Characters(Remove_spacewhite(row.email))
             if(Validate_Email(df_reglas.loc[index1,"email"],index1)==False):
                 print('Malformed correo: ', df_reglas.loc[index1,"email"])                        
             
             #VALIDA FECHA DE NACIMIENTO
             df_reglas.loc[index1,"FechaNacimiento"]=Change_Date_Format(row.FechaNacimiento,index1)
             Validate_not_greater_today(df_reglas.loc[index1,"FechaNacimiento"],index1)
            
             #VALIDA FECHA DE REGISTRO
             df_reglas.loc[index1,"FechaRegistro"]=Change_Date_Format(row.FechaRegistro,index1)
             Validate_not_greater_today(df_reglas.loc[index1,"FechaRegistro"],index1)
                 
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
             df_clean.to_csv(pathOut+"outf_ClientesLimpios"+now+".csv", index=False)#the csv file is generated with the corresponding data
         if (df_dirty.shape[0]>0):
             df_dirty.to_csv(pathOut+"outf_clientesSucios"+now+".csv", index=False)#the csv file is generated with the corresponding data
         print("Reglas aplicadas correctamente")
     except Exception as ex:
         print('error rules: {}'.format(ex))
except Exception as ex:    
    print('Error : {}' .format(ex)) 
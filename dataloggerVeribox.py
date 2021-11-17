'''
Created on 7 abr. 2020
@author: usuario
'''
from configparser import ConfigParser
import csv
import csv
from datetime import timedelta
from datetime import datetime
import logging
from nt import getcwd
from os import walk, getcwd, path
import os
import re
import shutil
import sys
import time

import psycopg2


def config(filename='database.ini', section='postgresql'):
    ''' The following config() function reads in the database.ini file and returns the connection
     parameters as a dictionary'''
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
    
    # get section, default to postgresql
    db = {}
    
    # Checks to see if section (postgresql) parser exists
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
         
    # Returns an error if a parameter is called that is not listed in the initialization file
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
        logging.warning('Section {0} not found in the {1} file'.format(section, filename))
    return db

def buscar_archivo(regex = '', ruta = getcwd()+'\\input\\'):
    pat = re.compile(regex)
    resultado = []
      
    for (dir, _, archivos) in walk(ruta):
        #ordeno en el tiempo los archivos
        #archivos.sort(key = path.getmtime, reverse=False)
        #agrego archivos filtrados por el nombre
        resultado.extend([ path.join(dir,arch) for arch in 
                              filter(pat.search, archivos) ])
        break  # habilitar si no se busca en subdirectorios
    
    return resultado

def leerDatosCsv(archivo,pdm):
    try:
        
        csvarchivo = open(archivo)  # Abrir archivo csv
        entrada = csv.reader(csvarchivo,delimiter=';')  # Leer todos los registros
        
        resultado = list()
        #leo
        for linea in entrada:
            
            if (21>len(linea)>18):
                print(linea)
            
                if pdm.es_activo(int(linea[0])):
             
                    nombre= pdm.get_nombre(int(linea[0]))
                    pdm_id= pdm.get_pdm_id(int(linea[0]))
                    pca= pdm.get_pca(int(linea[0]))
        
                    #punto_de_medicion= get_punto_de_medicion(linea[0])
                    #numero_de_clente = get_numero_de_clente(linea[0])
                    #nombre_de_estacion = get_nombre_de_estacion(linea[0])
        
                    if linea[3] == "Switch":
                        if linea[5] == "Pressure":
                            if linea[9] == "Batt":
                                #format date 
                                fecha_aux = linea[1].replace('.','-')                        
                                fecha_aux = fecha_aux[6]+fecha_aux[7]+fecha_aux[8]+fecha_aux[9]+fecha_aux[2:6]+fecha_aux[0]+fecha_aux[1]+' ' +linea[2]
                                #devuelvo lista con todos los valores 
                                resultado.append((linea[0], fecha_aux,linea[6],linea[4],linea[8],linea[10],nombre,pdm_id,pca))
           
                else:
                    print ("no se encuetra ",linea[0])
                    pass
            
            else:
                print("ERROR: Failed in Length of list %s",linea)
                logging.error("Failed in Length of list %s", linea)
        return resultado
    except (Exception, psycopg2.Error) as error :
        print("Failed to read csv data ", error,archivo)   
        logging.error("Failed to fetch record data %s %s", error, archivo)
        
def updateVolumenAnterior(datos):
    print("updating VolAn....")
    for row in datos:
        horalogger=str(row[1])
        
        #busco el texto de 6AM en cada linea
        if ( horalogger.find(" 6:00:00")>5 or horalogger.find(" 06:00:00")>5):
            #formateo si lo encuentro
            dateFormatHoraLogger = datetime.strptime(horalogger, '%Y-%m-%d %H:%M:%S')

            #calculo el dia anterior a las 6AM 
            dateLoggerDiaAnterior= dateFormatHoraLogger-timedelta(days=1)
            # tomo el numero de serie
            veribox_sn=str(str(row[0]))
            
            print(row[1])
            #calculo el volumen anterior  corregido
            sintax="UPDATE ucv.ucv_data \
                    SET vol_c_an =subquery.cierre\
                        FROM ( \
                            SELECT \
                                t.datetime, \
                                (vol_c - LEAD(vol_c) OVER (ORDER BY t.datetime DESC)) AS CIERRE \
                            FROM ucv.ucv_data as t \
                            WHERE  ( \
                                t.datetime= '" + str(dateFormatHoraLogger) + "' \
                                OR t.datetime= '"+ str(dateLoggerDiaAnterior) +"') \
                                AND t.veribox_sn= " + veribox_sn + " \
                                AND t.batt_vc<>0 \
                            GROUP BY  t.datetime,t.vol_c \
                        ) AS subquery \
                    WHERE \
                        ucv.ucv_data.datetime=subquery.datetime AND \
                        subquery.cierre >= 0 AND \
                        ucv.ucv_data.veribox_sn=" + veribox_sn
                
            #qry_postgres(sintax)

            #calculo el volumen anterior no corregido
            sintax=" UPDATE ucv.ucv_data \
                        SET vol_nc_an =subquery.cierre\
                        FROM (SELECT t.datetime,(vol_nc - LEAD(vol_nc) \
                            OVER (ORDER BY t.datetime DESC)) AS CIERRE \
                            FROM ucv.ucv_data as t \
                            WHERE  ( t.datetime= '" + str(dateFormatHoraLogger) + "' \
                                    OR t.datetime= '"+ str(dateLoggerDiaAnterior) +"') \
                                    AND t.veribox_sn= " + veribox_sn + " \
                                    AND t.batt_vc<>0 \
                        GROUP BY  t.datetime,t.vol_nc) AS subquery \
                        WHERE ucv.ucv_data.datetime=subquery.datetime AND \
                            subquery.cierre >= 0 AND \
                            ucv.ucv_data.veribox_sn=" + veribox_sn
            #qry_postgres(sintax)
            
            #otra opcion
            ##calculo el volumen anterior  corregido
            volumenAcumuladoHoy=get_postgres('vol_c', 'ucv.ucv_data', "datetime='"+str(dateFormatHoraLogger)+" '"+" AND veribox_sn= " + veribox_sn + " LIMIT 1")
            volumenAcumuladoAyer=get_postgres('vol_c', 'ucv.ucv_data', "datetime='"+str(dateLoggerDiaAnterior)+" '"+" AND veribox_sn= " + veribox_sn + " LIMIT 1")
           
            if volumenAcumuladoHoy:
                if volumenAcumuladoAyer:
                    print(volumenAcumuladoHoy[0][0])
                    print(volumenAcumuladoAyer[0][0]) 
                    if volumenAcumuladoHoy[0][0] >= volumenAcumuladoAyer[0][0]:
                        volumenDiaAyer = volumenAcumuladoHoy[0][0]-volumenAcumuladoAyer[0][0]
                    else:
                        volumenDiaAyer=volumenAcumuladoHoy[0][0]-volumenAcumuladoAyer[0][0]+100000000
                   
                   
                    sintax="UPDATE ucv.ucv_data \
                        SET vol_c_an = " + str(volumenDiaAyer) + " \
                        WHERE \
                        ucv.ucv_data.datetime= '" + str(dateFormatHoraLogger) + "' \
                        AND ucv.ucv_data.veribox_sn=" + veribox_sn
                    
                    qry_postgres(sintax)
                    
                    print(volumenDiaAyer)
                else:
                    print('vacio volumenAcumuladoAyer')
            else:
                print ('vacio volumenAcumuladoHoy')
            
           ##calculo el volumen anterior  corregido
            volumenAcumuladoHoy=get_postgres('vol_nc', 'ucv.ucv_data', "datetime='"+str(dateFormatHoraLogger)+" '"+" AND veribox_sn= " + veribox_sn + " LIMIT 1")
            volumenAcumuladoAyer=get_postgres('vol_nc', 'ucv.ucv_data', "datetime='"+str(dateLoggerDiaAnterior)+" '"+" AND veribox_sn= " + veribox_sn + " LIMIT 1")

            if volumenAcumuladoHoy:
                if volumenAcumuladoAyer:
                    print(volumenAcumuladoHoy[0][0])
                    print(volumenAcumuladoAyer[0][0]) 
                   
                    if volumenAcumuladoHoy[0][0] >= volumenAcumuladoAyer[0][0]:
                        volumenDiaAyer = volumenAcumuladoHoy[0][0]-volumenAcumuladoAyer[0][0]
                    else:
                        volumenDiaAyer=volumenAcumuladoHoy[0][0]-volumenAcumuladoAyer[0][0]+100000000
                   
                    sintax="UPDATE ucv.ucv_data \
                    SET vol_nc_an = " + str(volumenDiaAyer) + " \
                    WHERE \
                        ucv.ucv_data.datetime= '" + str(dateFormatHoraLogger) + "' \
                        AND ucv.ucv_data.veribox_sn=" + veribox_sn
                    
                    qry_postgres(sintax)    
                    
                    print(volumenDiaAyer)
                else:
                    print('vacio volumenAcumuladoAyer')
            else:
                print ('vacio volumenAcumuladoHoy')
            
            
                
def mover_archivo(file_to_move,succes= True):
    #print(file_to_move)
    #print(file_to_move[:7]+'output\\'+file_to_move[8:]) 
    if succes :  
        shutil.move(file_to_move, file_to_move[:-33]+'output\\'+file_to_move[-33:]) 
        print("file moved to output",file_to_move)
        logging.info("File moved to output %s",file_to_move)
        
    else :
        shutil.move(file_to_move, file_to_move[:-33]+'noproc\\'+file_to_move[-33:])
        print("file moved to noproc",file_to_move)
        logging.warning("File moved to noproc %s",file_to_move) 
       

def getCsvDataFromPdm(pdm):  
    '''leo los datos csv'''
    csvconfig=config('database.ini','csv')
    inputpath= csvconfig['input']
    regex= csvconfig['regex']
    #busco los archivos en carpeta
    resultado= list()
    listaDeArchivosEnCarpeta = buscar_archivo( regex,inputpath)
    time.sleep(10)
    #for row in buscar_archivo( r'VERIBOX-[0-9]{5}-[0-9]{8}-[0-9]{6}.csv',inputpath):
    for row in listaDeArchivosEnCarpeta:

        #print (basic_backend_csv.leer_datos_csv(row))
        filelen=len(inputpath)+len('\VERIBOX-')
        print(row)
        print(row[filelen:21])
        if pdm.es_activo(int(row[filelen:filelen+6])):
            #abro el archivo para buscar resultados
            resultado = resultado + leerDatosCsv(row,pdm) 
            mover_archivo(row)

    return resultado
def qry_postgres(sintax):
    try:        
        #get db params
        params = config()

        # Establish a connection to the database.
        conn = psycopg2.connect(**params)
    
        # Create a cursor. The cursor allows you to execute database queries. 
        cur = conn.cursor() 
    
        # Set up a query and execute it 
        cur.execute(sintax)
        
        # Fetch the data 
        #rows=cur.fetchall()
           
        conn.commit()
        count=cur.rowcount 
        print (count,"Record updated successfully")      
    except (Exception, psycopg2.Error) as error :
        print("Failed to fetch record data", error)   
        logging.error("Failed to fetch record data %s", error)
    finally:
        if (conn):
            cur.close()
            conn.close()
           # print("Connection Closed")
            
def get_postgres(sSelect, sFrom, sWhere="TRUE"):
    try:        
        #get db params
        params = config()

        # Establish a connection to the database.
        conn = psycopg2.connect(**params)
        # Create a cursor. The cursor allows you to execute database queries.
        cur = conn.cursor() 
       
        sintax="select "+sSelect+" from "+sFrom+" where "+sWhere+" "" "
        
        # Set up a query and execute it 
        cur.execute(sintax)
        
        # Fetch the data 
        rows=cur.fetchall()
           
        conn.commit()
        conn.close() 
             
        return rows          
    except (Exception, psycopg2.Error) as error :
        print("Failed to fetch record data", error)   
        logging.error("Failed to fetch record data %s", error)
        
def insertar_datos_query (records,sql_insert_query):
    '''requiere un strqry y una lista de datos'''
    try:
        #get db params
        params = config()
        # Establish a connection to the database.
        conn = psycopg2.connect(**params)
        # Create a cursor. The cursor allows you to execute database queries. 
        cur = conn.cursor()
        
        #sql_insert_query = """ INSERT INTO ucv_data (id, DateTime, vol_c, vol_nc) VALUES (%s,%s,%s,%s)"""

        # executemany() to insert multiple rows rows
        result = cur.executemany(sql_insert_query, records)
        conn.commit()
        print(cur.rowcount, "Record inserted successfully into mobile table")
        logging.info("%s Record inserted successfully into mobile table",cur.rowcount)
    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into mobile table {}".format(error))
        logging.warning("Failed inserting record into mobile table {}".format(error))        
        
    finally:
        # closing database connection.
        if (conn):
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")
            

class PuntoDeMedicion(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.rows = list()
        self.load_rows()
        pass
      
    def load_rows(self):
        sSelect="veribox_sn,linea,pca,ric,tramo,nombre,id"
        sFrom="ucv.pdm"
        sWhere="activo=1"
        self.rows = get_postgres(sSelect,sFrom,sWhere)
        
    def get_rows(self):
        return self.rows
        
    def get_nombre(self,veribox_sn):
        for a in self.rows:
            if veribox_sn in a :
                return (a[5])
        return ' ' 
    
    def get_pca(self,veribox_sn):
        for a in self.rows:
            if veribox_sn in a :
                return (a[2])
        return ' ' 
        
    def get_pdm_id(self,veribox_sn):
        for a in self.rows:
            if veribox_sn in a :
                return (a[6])
        return ' ' 
        
    def get_tramo(self,veribox_sn):
        for a in self.rows:
            if veribox_sn in a :
                return (a[4])
        return ' ' 
        
    def es_activo(self,veribox_sn):
        for a in self.rows:
            if veribox_sn in a :
                return True
        return False

class Controller1(object):
    
    def __init__(self):
        
        self.pdm = PuntoDeMedicion() 
        #self.view = View()
        self.pdm.load_rows()
        
    def getCsvData(self):
        #leo datos en csv
       
        items = getCsvDataFromPdm(self.pdm)
        #self.view.display_csv(items)
        return items
    def getRows(self):
        self.pdm.load_rows()
        return self.pdm.get_rows()
    
    def insertar_datos_query(self,items):
        sql_insert_query = """ INSERT INTO datalogger.veribox_data (veribox_sn, DateTime, presion, Switch, GSMQ,batt_vb,stationname,pdm_id,pca) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        items = insertar_datos_query(items,sql_insert_query)
        #self.view.display_item_stored(items)

if __name__ == '__main__':
    logging.basicConfig(filename='app.log',  format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',level=logging.DEBUG)

    logging.warning('App started')
    '''leo los datos csv'''
    csvconfig=config('database.ini','csv')
    inputpath= csvconfig['input']
    sleepTime= int(csvconfig['sleeptime'])
    regex= csvconfig['regex']
    print("********************************************************")
    print("* dataloggerVeribox: CSV to PSQL for Veribox Pressure dl*")
    print("* 2021@NV                                              *")
    print("* Input path:",inputpath)
    print("********************************************************")
    
    c = Controller1()
    #c.insertar_datos_query(records_to_insert,sql_insert_query)
    #c.mostrar_tabla()
    while(True):
        try:
            print(time.strftime("%d %H:%M:%S"))          
            #obtengo datos csv y le agrego     datos pdm
            datos= c.getCsvData()         
            if len(datos) != 0:
                #si  hay inserto los datos en la BD
                c.insertar_datos_query(datos)
                #elimino updateVolumenAnterior(datos)
                pass         
        except Exception as e:
            logging.error("Exception occurred", exc_info=True)
        
        time.sleep(sleepTime)
        
    pass
    sys.exit()
    

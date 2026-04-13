import pandas as pd 
import time

from ingestion.lectura_csv import leer_csv

def run_orchestator():
    almacen_datos={}

    print("---lectura de csv---")
    almacen_datos['Titanic']=leer_csv()
    
    return almacen_datos

import pandas as pd 
import time

from ingestion.lectura_csv import leer_csv

def run_orchestator():
    almacen_datos={}

    print("---lectura de csv---")
    almacen_datos['Titanic']=leer_csv()
    
    return almacen_datos


from ingestion.batch_datos import leer_datos_batch
def run_orchestator_batch():
    almacen_datos={}

    print("---lectura de batch---")
    almacen_datos['libros']=leer_datos_batch("science_fiction")
    
    return almacen_datos

if __name__ == "__main__":
    run_orchestator()
    run_orchestator_batch()
   


from ingestion.fuente_realtime import leer_clima_tiempo_real
def run_orchestator_realtime():

    
   
    
if __name__ == "__main__":
    run_orchestator()
    run_orchestator_batch()
    run_orchestator_realtime()
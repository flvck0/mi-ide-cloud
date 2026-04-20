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

from ingestion.fuente_realtime import leer_clima_tiempo_real
def run_orchestator_realtime():

    almacen_datos={}

    print("---lectura de clima en tiempo real---")
    almacen_datos['clima']=leer_clima_tiempo_real()
    
    return almacen_datos

if __name__ == "__main__":
    datos = run_orchestator()
    df = datos['Titanic']
    
    # Análisis de sobrevivientes usando group by 
    sobrevivientes = df.groupby('2urvived').size()
    print("Número de pasajeros que sobrevivieron y no sobrevivieron:")
    print(sobrevivientes)
    
    # Actualizar el diccionario datos con los datos batch
    datos.update(run_orchestator_batch())
    df_batch = datos['libros']
    
    # Extraer la llave única de la columna 'key' y crear 'UniqueKey'
    df_batch['UniqueKey'] = df_batch['key'].str.split('/').str[-1]
    print(df_batch[['title', 'key', 'UniqueKey']].head())
    
    print(f"\nClaves en el diccionario datos: {list(datos.keys())}")    


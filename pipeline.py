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

    print("--- Lectura del clima en tiempo real")
    total_lecturas=[]
    
    # Tomamos 5 instantaneas para simular tiempo real
    for i in range(5):
        print(f"  > instantanea {i+1}...")
        df_snap = leer_clima_tiempo_real()
        if not df_snap.empty:
            total_lecturas.append(df_snap)
        time.sleep(1) # Short delay
    
    if total_lecturas:
        almacen_datos['clima'] = pd.concat(total_lecturas, ignore_index=True)
    else:
        almacen_datos['clima'] = pd.DataFrame()

    print("--- Resumen de datos sin transformar")

    for elemento, df in almacen_datos.items():
        print(f"\n📍 FUENTE: {elemento}")
        if not df.empty:
            print(f"Rows: {len(df)} | Columns: {list(df.columns)}")
            print(df.head(2))
        else:
            print("Empty Table (Check connection)")

    return almacen_datos

if __name__ == "__main__":
    datos = run_orchestator()
    df = datos['Titanic']
    
    # PASO 1: Borrar filas de pasajeros menores de 10 años y actualizar la entrada
    df_filtrado = df[df['Age'] >= 10].copy()
    datos['Titanic'] = df_filtrado
    print("---PASO 1: Filtrado de Titanic---")
    print(f"Filas originales: {len(df)}, Filas después de filtrado (>= 10 años): {len(df_filtrado)}")
    
    # Análisis de sobrevivientes usando group by 
    sobrevivientes = df_filtrado.groupby('2urvived').size()
    print("Número de pasajeros que sobrevivieron y no sobrevivieron:")
    print(sobrevivientes)
    
    # Actualizar el diccionario datos con los datos batch
    datos.update(run_orchestator_batch())
    df_batch = datos['libros']
    
    # Extraer la llave única de la columna 'key' y crear 'UniqueKey'
    df_batch['UniqueKey'] = df_batch['key'].str.split('/').str[-1]
    print(df_batch[['title', 'key', 'UniqueKey']].head())
    
    # Obtener datos de clima en tiempo real y crear tabla resumen
    datos.update(run_orchestator_realtime())
    df_clima = datos['clima']
    
    # Crear tabla resumen con el promedio de temperatura (solo si hay datos)
    if not df_clima.empty and 'temperature' in df_clima.columns:
        resumen_clima = pd.DataFrame({
            'promedio_temperatura': [df_clima['temperature'].mean()]
        })
        datos['resumen_clima'] = resumen_clima
        print("\nTabla resumen - Promedio de temperatura (Clima):")
        print(datos['resumen_clima'])
    else:
        print("\n⚠️ No se pudo obtener datos de clima. DataFrame vacío o sin columna 'temperature'.")
    for clave, valor in datos.items():
        if isinstance(valor, pd.DataFrame):
            print(f"\n{clave}: DataFrame con {len(valor)} filas y {len(valor.columns)} columnas")
        else:
            print(f"\n{clave}: {type(valor).__name__}")
    
    print(f"\nClaves en el diccionario datos: {list(datos.keys())}")    


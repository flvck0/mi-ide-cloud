# mi-ide-cloud

Este proyecto procesa tres fuentes de datos: Titanic, Librería y Clima.

Transformaciones aplicadas:

- En 'Titanic': se eliminan pasajeros menores de 10 años y se crea un resumen del conteo de sobrevivientes y no sobrevivientes.
- En 'Librería': se agrega una columna 'UniqueKey' que extrae la llave única de cada libro a partir de la columna 'key'.
- En 'Clima': se toman varias lecturas en tiempo real, se concatenan y se crea un resumen con el promedio de temperatura.

Los resultados se almacenan en un diccionario 'almacen_datos' y se imprime un resumen de cada entrada en terminal.

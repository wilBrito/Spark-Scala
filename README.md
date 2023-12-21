# Spark-Scala


Ejercicio 1

Cree tres DataFrames a partir de los datos proporcionados y verifique que todos los nombres de columnas de los tres DataFrames cumplen el siguiente formato:

 Todas la letras en minúscula.
Las palabras deben estar separadas por el carácter “_”.
Los nombres de columna no deben tener espacios en blanco al principio, final o en medio.
Los nombres de columnas no deben contener caracteres como puntos, paréntesis, o guiones medios.
Un ejemplo de como debe quedar el nombre de las columnas es el siguiente: average_price_of_1gb_usd_at_the_start_of_2021.



Ejercicio 2

Determine los cinco países con mayor número de usuarios de Internet en la región de América. La salida debe contener el nombre del país, la región, la subregión y la cantidad de usuarios de Internet.



Ejercicio 3

Obtenga el top tres de las regiones con más usuarios de internet


Ejercicio 4

Obtenga el país con más usuarios de Internet por región y subregión. Por ejemplo, el resultado para la región de las Américas y la subregión Norte América debería ser Estados Unidos. La salida debe contener el nombre del país con más usuarios de Internet, la región, la subregión y la cantidad de usuarios de Internet. Además, la salida debe estar ordenada de mayor a menor atendiendo a la cantidad de usuarios de Internet de cada país.



Ejercicio 5

Escriba el DataFrame obtenido en el ejercicio anterior teniendo en cuenta las siguientes cuestiones:

El DataFrame debe tener tres particiones
 La escritura del DataFrame debe quedar particionada por la región
El modo de escritura empleado para la escritura debe ser overwrite
El formato de escritura debe ser AVRO
El DataFrame debe guardarse en la ruta /FileStore/ProyectoFinal/salida



Ejercicio 6

Determine los 10 países con la mayor velocidad promedio de internet según el test de Ookla. Es de interés conocer la región, subregión, población y usuarios de internet de cada país por lo tanto los países a los que no se les pueda recuperar estos datos deben ser excluidos de la salida resultante.


Ejercicio 7

Determine el promedio del costo de 1GB en usd a principios del año 2021 por región. Aquellas ubicaciones a las que no pueda obtenerle la región no deben ser consideradas en el cálculo. La salida debe tener tres columnas, region, costo_prom_1_gb y grupo_region y además mostrar las regiones ordenadas de menor a mayor por su costo promedio de un 1GB en usd a principios del año 2021. La columna grupo_region debe ser etiquetada de acuerdo a la siguiente regla:

Si la región comienza con la letra A la etiqueta debe ser region_a
Si la región comienza con la letra E la etiqueta debe ser region_e
 En los demás casos la etiqueta debe ser region_por_defecto


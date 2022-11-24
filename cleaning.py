import petl as etl
import psycopg2

tabla1 = etl.fromcsv('./datos.csv', delimiter=',')
tabla1 = etl.rename(tabla1, 'salary', 'salary_pesos')
"""
print(tabla1.head(3)) # Muestra los primeros 3 registros
print(tabla1.tail(3)) # Muestra los últimos 3 registros
print(tabla1.head(1))
"""

#tabla2 = etl.skip(tabla1,1)
# print(tabla2.head(0))

tabla2 = etl.convert(tabla1, 'salary_pesos', float)
tabla3 = etl.sort(tabla2, 'salary_pesos')
print(tabla3)

tabla4 = etl.convert(tabla3, 'firstname', 'lower')
print(tabla4)

tabla5 = etl.convert(tabla4, 'lastname', 'upper')
print(tabla5)

tabla6 = etl.convert(tabla5, 'firstname', lambda item:
                     item[0:1].upper() + item[1:].lower() + str(len(item)))
print(tabla6)

etl.tocsv(tabla6, 'nuevosdatos.csv')

tabla7 = etl.addfield(tabla6, 'salary_dolares', lambda elemento:
                      round(elemento['salary_pesos'] / 21.4, 2))
print(tabla7)

conexion = psycopg2.connect(database="inventariodb",
                            user="postgres", password="12345",
                            host="localhost", port="5432")
etl.todb(tabla7, conexion, 'tabla', create=False)
etl.tohtml(tabla7, 'tabla.html', caption='Tabla de datos')
etl.tojson(tabla7, 'tabla.json')

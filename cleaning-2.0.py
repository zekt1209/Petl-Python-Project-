import petl as etl
import psycopg2

tabla1 = etl.fromcsv('datos.csv', delimiter=',')
#tabla1 = etl.rename(tabla1,'salary', 'salary_pesos')

print(tabla1.head(3))  # Muestra los primeros 3 registros

tabla2 = etl.convert(tabla1, 'salary', float)
print(tabla2.head(1))

tabla3 = etl.sort(tabla2, 'salary')
print(tabla3.head(3))

"""
CALCULAR EL PROMEDIO DE AMIGOS POR EDAD
"""

from pyspark import SparkConf,SparkContext, rdd

conf = SparkConf().setMaster('local').setAppName('PromedioAmigosPorEdad')
sc=SparkContext(conf=conf)

def getLinesBD(line_BD):
    campos=line_BD.split(',')
    edad=int(campos[2])
    numAmigos=int(campos[3])
    return(edad,numAmigos)

lineas_BD=sc.textFile("C:/Users/52951\Desktop/analisis de datos/BDamigos/amigos.csv")
rdd=lineas_BD.map(getLinesBD)
total_Edad=rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
promedio_Edad=total_Edad.mapValues(lambda x:x[0]/x[1])
resultados= promedio_Edad.collect()
for resultado in resultados:
    print(resultado)
    
    
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster('local').setAppName('ratingMovies')
sc=SparkContext(conf=conf)

lines= sc.textFile("C:/Users/52951\Desktop/analisis de datos/BDMovies/ml-100k/u.data")
ratings=lines.map(lambda x: x.split()[2])
result=ratings.countByValue()
print('Los resultados son:')
print(result)
print('Fin:\n')

orderedResults=collections.OrderedDict(sorted(result.items()))
print('Los resultados Ordenados son:')
print(orderedResults)
print('Fin:\n')

for ratingStar,countStar in orderedResults.items():
    print("%s %i " %(ratingStar,countStar))


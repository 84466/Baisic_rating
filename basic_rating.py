from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType


spark = SparkSession.builder.appName('app_name').getOrCreate()

def read_csv(path:str):
        return spark.read.csv(path, sep='\t',
                          header=True)


def joins(basic,rating):
        return basic.join(rating,basic.tconst ==  rating.tconst,"inner")


def morethanhundred(list):
    return list.filter(list.numVotes > 100)

def movie(morethanhundred):
    return morethanhundred.filter(morethanhundred.titleType == 'movie')


def converting_in_int(movie):
    return movie.withColumn("numVotes", movie["numVotes"].cast(IntegerType()))

def maxvalue(max):
    return max.orderBy(['numVotes'], ascending = [False])

def coloum(max):
    return  max.select("primaryTitle")


def main():
    basic = read_csv('C:\\Users\\Rev07\\Downloads\\ratings\\data.tsv')
    rating =read_csv('C:\\Users\\Rev07\\Downloads\\basics\\data.tsv')
    inner_joins = joins(basic,rating)
    fetch_hundred_movie = morethanhundred(inner_joins)
    moviecoloum = movie(fetch_hundred_movie)
    converting_into_int = converting_in_int(moviecoloum)
    max_ascending = maxvalue(converting_into_int)
    coloum_select = coloum(max_ascending)
    coloum_select.show(15)


if _name_ == '_main_':
    main()
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *

#Comments-
#1-Add more tests if possible
#2-Rename the Makefile (filename)
#3-write command of black and flake8 in Makefile
#4-Add all answers in the comments
#5-Use logger if possible
#6-Create new branch for diffrent changes
# This is spark session
spark = SparkSession.builder.appName("app_name").getOrCreate()

#  This function read the path file and seperate the coloumns
def read_csv(path: str) -> DataFrame:
    """this functon is read spark dataframe"""
    return spark.read.csv(path, header=True)


def days_of_flights(flights: DataFrame) -> DataFrame:
    """this function shows the total days"""
    total_days = flights.select(countDistinct("day"))
    return total_days


def joints_flights_planes(flights: DataFrame, planes: DataFrame) -> DataFrame:
    """ join flights dataframe and planes daraframe on tailnum and year coloumns"""
    flight_planes= flights.join(planes, how="inner", on=["tailnum"])
    delay = (
        flight_planes.groupBy("manufacturer")
        .agg(sum("dep_delay").alias("dep_delay"))
        .sort(desc("dep_delay")))
    return delay


def joints_flights_airports(flights: DataFrame, airports: DataFrame) -> DataFrame:
    """this function join flights dataframe and airports daraframe on origin and IATA_CODE coloumns"""
    flight_airport = flights.join(airports, flights.origin == airports.IATA_CODE, "inner")
    return flight_airport.select(countDistinct("CITY"))

# This is main function
def main():
    current_path = "C:/Users/Rev07/Downloads/data/"
    paths = os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        current_path,
    )
    flights = "flights.csv"
    planes = "planes.csv"
    airport = "airports.csv"

    flights = read_csv(f"{paths}{flights}")
    planes = read_csv(f"{paths}{planes}")
    airport = read_csv(f"{paths}{airport}")

    #31 days
    days = days_of_flights(flights)
    days.show()

    # the relationship between flights and planes tables is tailnum
    df_merge = joints_flights_planes(flights, planes)
    df_merge.show(1)
    #the most delays airplane manufacturer in the analysis period is EMBRAER
    # analysis_period = delay_agg(df_merge)
    # analysis_period.show(1)
    # Newark and New York are the cities
    df_join = joints_flights_airports(flights, airport)
    df_join.show()


if __name__ == "__main__":
    main()

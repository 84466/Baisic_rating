import os
from pyspark.sql.functions import current_date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("app_name").getOrCreate()


def read_csv(path: str) -> DataFrame:
    return spark.read.csv(path, header=True)


def days_of_flights(flights: DataFrame) -> DataFrame:
    return flights.agg({"day": "sum"})


def city(flights: DataFrame) -> float:
    return flights.groupBy("origin").count()


def joints_df(flights: DataFrame, planes: DataFrame) -> DataFrame:
    return flights.join(planes, how="inner", on=["tailnum", "year"])


def delay_agg(short) -> float:
    delay = (
        short.groupBy("manufacturer")
        .agg(sum("dep_delay").alias("dep_delay"))
        .sort(desc("dep_delay"))
    )
    return delay


def main():
    file = os.path.abspath(os.path.dirname(__file__))
    current_path = "C:\\Users\\Rev07\\Downloads\\data\\"
    paths = os.path.join(
        file,
        current_path,
    )
    flights = read_csv(f"{paths}flights.csv")
    planes = read_csv(f"{paths}planes.csv")

    # the flights table cover 5291016 days
    days = days_of_flights(flights)
    days.show()

    # departure cities the flight database covers LGA , EWR , JFK
    departure_cities = city(flights)
    departure_cities.show()

    # the relationship between flights and planes tables is years and tailnum

    df_merge = joints_df(flights, planes)
    # df_merge.show()

    # the most delays airplane manufacturer in the analysis period is BOEING
    analysis_period = delay_agg(df_merge)
    analysis_period.show(1)


if __name__ == "__main__":
    main()

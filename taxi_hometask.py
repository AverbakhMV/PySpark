"""
Берем данные по Нью-Йоркскому такси
из S3 хранилища на яндекс облаке и
рассчитываем среднюю цену поездки и
среднюю стоимость за км по дате и
типу оплаты
Записываем в csv файл
"""
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

dim_columns = ['id', 'name']

payment_rows = [
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip'),
]

trips_schema = StructType([
    StructField('vendor_id', StringType(), True),
    StructField('tpep_pickup_datetime', TimestampType(), True),
    StructField('tpep_dropoff_datetime', TimestampType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('ratecode_id', IntegerType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('pulocation_id', IntegerType(), True),
    StructField('dolocation_id', IntegerType(), True),
    StructField('payment_type', IntegerType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType()),
])

def create_dict(spark, data, header):
    df=spark.createDataFrame(data=data, schema=header)
    return df

def agg_calc(spark):
    data_path = 's3a://karpovlab/2020'

    trip_fact=spark.read \
        .option('header', 'true') \
        .schema(trips_schema) \
        .csv(data_path)

    trip_fact.show()

    datamart=trip_fact \
        .where ((f.month(trip_fact['tpep_pickup_datetime'])=='1') & \
                (f.year(trip_fact['tpep_pickup_datetime'])=='2020')) \
        .groupby(trip_fact['payment_type'],
                 f.to_date(trip_fact['tpep_pickup_datetime']).alias('date')) \
        .agg(f.avg(trip_fact['total_amount']).alias('average_trip_cost'),
             (f.sum(trip_fact['total_amount'])/f.sum(trip_fact['trip_distance'])).alias('avg_trip_km_cost')) \
        .select(f.col('payment_type'),
                f.col('date'),
                f.col('average_trip_cost'),
                f.col('avg_trip_km_cost'))

    datamart.show()

    return datamart


def main (spark):
    payment_dim=create_dict(spark, payment_rows, dim_columns)

    datamart=agg_calc(spark).cache()

    joined_datamart=datamart \
        .join(other=payment_dim, on=payment_dim['id']==f.col('payment_type'), how='inner') \
        .select(payment_dim['name'], f.col('date'), f.round(f.col('average_trip_cost'),2).alias('average_trip_cost'),
                f.round(f.col('avg_trip_km_cost'),2).alias('avg_trip_km_cost')) \
        .orderBy(f.col('date').desc(), f.col('payment_type'))

    joined_datamart.show(truncate=False, n=1000)
    joined_datamart.coalesce(1).write.option('header','true').mode('overwrite').csv('output')


spark=SparkSession \
    .builder \
    .appName ('Spark hometask') \
    .getOrCreate()


if __name__ == '__main__':
    main(spark)
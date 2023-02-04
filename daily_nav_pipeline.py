import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, to_date, avg, max, min
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Initilize Spark Session
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Create schema for the DailyNAV data
daily_nav_schema = StructType(fields=[StructField("Scheme Code", IntegerType(), False),
                                    StructField("Scheme Name", StringType(), True),
                                    StructField("ISIN Div Payout/ISIN Growth", StringType(), True),
                                    StructField("ISIN Div Reinvestment", StringType(), True),
                                    StructField("Net Asset Value", DoubleType(), True),
                                    StructField("Repurchase Price", DoubleType(), True),
                                    StructField("Sale Price", DoubleType(), True),
                                    StructField("Date", DateType(), True)])


# Read data from csv
df_raw = spark.read.format('csv').option('header', 'true').schema(daily_nav_schema).load('/Users/prashrawat/Desktop/Kroger/InternalTrainings/DailyNAV/*')

# Filter out rows which is having zero or invalid NAV, Repurchase and/or Sale Price values
df_clean_data = df_raw.filter((col('Net Asset Value') > 0) & (col('Net Asset Value').isNotNull())) \
                    .filter((col('Repurchase Price') > 0) & (col('Repurchase Price').isNotNull())) \
                    .filter((col('Sale Price') > 0) & (col('Sale Price').isNotNull()))

# Validate and transform date field to YYYY-MM-DD format
# Calculate Exit Load (1% of NAV) and persist it as a new column
df_pre_processed = df_clean_data.withColumn('Date', to_date(col('Date'), 'YYYY-MM-DD')) \
                           .withColumn('Exit Load', col('Net Asset Value')*0.01)

df_pre_processed.persist()

# Write bad data to the error_data directory
df_bad_data = df_raw.join(df_clean_data, [df_raw['Scheme Code'] == df_clean_data['Scheme Code'], df_raw['Net Asset Value'] == df_clean_data['Net Asset Value'], df_raw['Repurchase Price'] == df_clean_data['Repurchase Price'], df_raw['Sale Price'] == df_clean_data['Sale Price']], "anti")
df_bad_data.write.format('csv').option('header','true').mode('overwrite').save('/Users/prashrawat/Desktop/Kroger/InternalTrainings/error_data')

# Calculate Monthly average   NAV, Repurchase & Sale Price for each scheme
df_average = df_pre_processed.groupBy(['Scheme Name', month('Date').alias('Month'), year('Date').alias('Year')]).agg(avg('Net Asset Value').alias('Average_NAV'), avg('Repurchase Price').alias('Average_Repurchase_Price'), avg('Sale Price').alias('Average_Sale_Price'))

# Find out scheme(code) with non-zero exit load
df_non_zero_exit_load = df_pre_processed.filter(col('Exit Load') > 0).select(col('Scheme Code')).distinct()

# Find out each scheme’s Max and Min NAV value and Date it occurred
df_pre_processed.createOrReplaceTempView("pre_processed_table")
df_max_min_nav = spark.sql("select final_table.`Scheme Name`, min_nav, (select min(p1.Date) from pre_processed_table p1 where p1.`Scheme Name` = final_table.`Scheme Name` and p1.`Net Asset Value` = final_table.min_nav) as min_date, max_nav, (select max(p2.Date) from pre_processed_table p2 where p2.`Scheme Name` = final_table.`Scheme Name` and p2.`Net Asset Value` = final_table.max_nav) as max_date from (select `Scheme Name`, min(`Net Asset Value`) min_nav, max(`Net Asset Value`) max_nav from pre_processed_table group by `Scheme Name`) final_table")

# Filter the resultant NavHistory Dataset to extract rows corresponding to the month of December 2018
df_processed_date = df_pre_processed.filter((year(col('Date')) == 2017) & (month(col('Date')) == 12))

df_pre_processed.unpersist()

# Partition the data based on schemeId(code)
# Store partitioned data to the processed_data directory in parquet format
df_processed_date.write.format('parquet').partitionBy('Scheme Code').mode('overwrite').save('/Users/prashrawat/Desktop/Kroger/InternalTrainings/processed_data')

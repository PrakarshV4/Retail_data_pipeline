# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, mean, stddev
# from pyspark.sql.types import FloatType, DoubleType, IntegerType


# spark = SparkSession.builder.appName("Data_Cleaning").getOrCreate()

# # 2️List of CSV files
# files = {
#     "dim_category": "/Users/prakarshverma/Documents/Hackzilla/Retail_Data_Hackzilla/scripts/data/raw/dim_category.csv",
#     "dim_customer": "/Users/prakarshverma/Documents/Hackzilla/Retail_Data_Hackzilla/scripts/data/raw/dim_customer.csv",
#     "dim_date": "/Users/prakarshverma/Documents/Hackzilla/Retail_Data_Hackzilla/scripts/data/raw/dim_date.csv",
#     "dim_location": "/Users/prakarshverma/Documents/Hackzilla/Retail_Data_Hackzilla/scripts/data/raw/dim_location.csv",
#     "dim_product": "/Users/prakarshverma/Documents/Hackzilla/Retail_Data_Hackzilla/scripts/data/raw/dim_product.csv",
#     "fact_reviews": "/Users/prakarshverma/Documents/Hackzilla/Retail_Data_Hackzilla/scripts/data/raw/fact_reviews.csv",
#     "fact_transaction": "/Users/prakarshverma/Documents/Hackzilla/Retail_Data_Hackzilla/scripts/data/raw/fact_transactions.csv"
# }

# #  Load CSV Files
# dataframes = {name: spark.read.csv(path, header=True, inferSchema=True) for name, path in files.items()}

# # Data Cleaning Functions
# def clean_dataframe(df):
#     # Remove rows with nulls in any column
#     df = df.dropna(how='any')
    
#     # Remove duplicates
#     df = df.dropDuplicates()
    
#     # Remove outliers (Z-score method for numeric columns)
#     numeric_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, (FloatType, DoubleType, IntegerType))]
#     for col_name in numeric_cols:
#         mean_val = df.select(mean(col(col_name))).collect()[0][0]
#         stddev_val = df.select(stddev(col(col_name))).collect()[0][0]
        
#         if mean_val is not None and stddev_val is not None and stddev_val != 0:
#             df = df.filter((col(col_name) - mean_val) / stddev_val < 3)
    
#     return df

# # Apply Cleaning Function
# cleaned_dataframes = {name: clean_dataframe(df) for name, df in dataframes.items()}

# # Save Cleaned Data (optional)
# for name, df in cleaned_dataframes.items():
#     df.write.csv(f"data/refined/cleaned_{name}.csv", header=True)

# # Show Cleaned Data (optional)
# for name, df in cleaned_dataframes.items():
#     print(f"Cleaned Data for {name}:")
#     df.show(5)
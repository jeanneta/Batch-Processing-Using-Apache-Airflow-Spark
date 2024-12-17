import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from airflow.hooks.base_hook import BaseHook
import psycopg2
import os
from sqlalchemy import create_engine
import pandas as pd
import logging
from datetime import datetime

# Initialize Spark Session (only once globally for efficiency)
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
    .master("local") \
    .appName("PySpark_Postgres") \
    .getOrCreate()

# Function to get database connection details
def get_db_connection(conn_id, db_type="postgres"):
    conn = BaseHook.get_connection(conn_id)
    if db_type == "postgres":
        return {
            "url": f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}",
            "driver": "org.postgresql.Driver",
            "user": conn.login,
            "password": conn.password
        }
    elif db_type == "mysql":
        return {
            "url": f"mysql+mysqlconnector://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}",
            "user": conn.login,
            "password": conn.password
        }
    else:
        raise ValueError("Unsupported database type")

# Function to read data from Postgres using Spark
def read_from_postgres(table_name, conn_details):
    return spark.read.format("jdbc") \
        .option("url", conn_details["url"]) \
        .option("driver", conn_details["driver"]) \
        .option("dbtable", table_name) \
        .option("user", conn_details["user"]) \
        .option("password", conn_details["password"]).load()

# Function to save DataFrame to Parquet
def save_to_parquet(df, output_path):
    df.write.mode('overwrite') \
        .option('compression', 'snappy') \
        .option('partitionOverwriteMode', 'dynamic') \
        .save(output_path)

# Function to load data from Parquet to MySQL
def load_to_mysql(parquet_path, table_name, conn_details, index=False):
    df = pd.read_parquet(parquet_path)
    engine = create_engine(conn_details["url"], echo=False)
    df.to_sql(name=table_name, con=engine, if_exists='append', index=index)

# Extract and Transform: Top Countries
def extract_transform_top_countries():
    try:
        conn_details = get_db_connection('postgres_conn')

        # Read necessary tables
        city_df = read_from_postgres("city", conn_details)
        country_df = read_from_postgres("country", conn_details)
        customer_df = read_from_postgres("customer", conn_details)
        address_df = read_from_postgres("address", conn_details)

        # Create temporary views
        city_df.createOrReplaceTempView("city")
        country_df.createOrReplaceTempView("country")
        customer_df.createOrReplaceTempView("customer")
        address_df.createOrReplaceTempView("address")

        # Perform transformation
        df_result = spark.sql('''
            SELECT
                country,
                COUNT(country) as total,
                current_date() as date,
                'jeanneta_airflow' as data_owner
            FROM customer
            JOIN address ON customer.address_id = address.address_id
            JOIN city ON address.city_id = city.city_id
            JOIN country ON city.country_id = country.country_id
            GROUP BY country
            ORDER BY total DESC
        ''')

        # Save to Parquet
        save_to_parquet(df_result, 'output/data_result_1')

        logging.info("Extract and transform for Top Countries completed successfully.")
    except Exception as e:
        logging.error(f"Error in extract_transform_top_countries: {e}")

# Extract and Transform: Total Films by Category
def extract_transform_total_film():
    try:
        conn_details = get_db_connection('postgres_conn')

        # Read necessary tables
        category_df = read_from_postgres("category", conn_details)
        film_category_df = read_from_postgres("film_category", conn_details)

        # Create temporary views
        category_df.createOrReplaceTempView("category")
        film_category_df.createOrReplaceTempView("film_category")

        # Perform transformation
        df_result = spark.sql('''
            SELECT 
                c.name AS category_name,
                COUNT(fc.film_id) AS total_film,
                'jeanneta_airflow' as data_owner,
                current_date() as date
            FROM 
                category c
            JOIN 
                film_category fc ON c.category_id = fc.category_id
            GROUP BY 
                c.name
            ORDER BY 
                total_film DESC;
            ''')

        # Save to Parquet
        save_to_parquet(df_result, 'output/data_result_2')

        logging.info("Extract and transform for Total Films by Category completed successfully.")
    except Exception as e:
        logging.error(f"Error in extract_transform_total_film: {e}")

# Load functions for both datasets
def load_top_countries():
    try:
        conn_details = get_db_connection('mysql_conn', db_type="mysql")
        load_to_mysql('output/data_result_1', 'top_country', conn_details, index=True)
        logging.info("Load of Top Countries to MySQL completed successfully.")
    except Exception as e:
        logging.error(f"Error in load_top_countries: {e}")

def load_total_film():
    try:
        conn_details = get_db_connection('mysql_conn', db_type="mysql")
        load_to_mysql('output/data_result_2', 'total_film_by_category', conn_details, index=True)
        logging.info("Load of Total Films by Category to MySQL completed successfully.")
    except Exception as e:
        logging.error(f"Error in load_total_film: {e}")

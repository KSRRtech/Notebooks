# Databricks notebook source

# MAGIC %md
# MAGIC # Add Rows to Department Table

# Create sample department data
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F

# Define schema for department table
dept_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("managerId", IntegerType(), True),
    StructField("employeeCount", IntegerType(), True)
])

# Create initial department data
initial_dept_data = [
    (1, "Engineering", "Bangalore", 101, 25),
    (2, "Sales", "Mumbai", 102, 15),
    (3, "Human Resources", "Delhi", 103, 8)
]

# Create DataFrame
df_dept = spark.createDataFrame(initial_dept_data, schema=dept_schema)

# Display existing departments
display(df_dept)

# COMMAND ----------

# Add 2 new departments
new_dept_data = [
    (4, "Finance", "Pune", 104, 12),
    (5, "Marketing", "Hyderabad", 105, 10)
]

df_new_dept = spark.createDataFrame(new_dept_data, schema=dept_schema)

# Union with existing data
df_dept_updated = df_dept.union(df_new_dept)

# Display updated departments
display(df_dept_updated)

# COMMAND ----------

# Save to table (optional)
df_dept_updated.write.mode("overwrite").saveAsTable("departments")

# Verify table was created
spark.sql("SELECT * FROM departments").show()

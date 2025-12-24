# Create Employees Table
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Define schema for employee table
emp_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("jobTitle", StringType(), True),
    StructField("departmentId", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("status", StringType(), True)
])

# Create employee data
emp_data = [
    (1001, "Romin", "Irani", "Developer", 101, "romin.irani@company.com", 75000.00, "Active"),
    (1002, "Alice", "Carter", "Senior Developer", 101, "alice.carter@company.com", 85000.00, "Active"),
    (1003, "Daniel", "Kim", "HR Manager", 103, "daniel.kim@company.com", 70000.00, "On Leave"),
    (1004, "Priya", "Patel", "Sales Manager", 102, "priya.patel@company.com", 80000.00, "Active"),
    (1005, "Raj", "Kumar", "QA Engineer", 101, "raj.kumar@company.com", 65000.00, "Active"),
    (1006, "Sarah", "Johnson", "Finance Analyst", 104, "sarah.johnson@company.com", 72000.00, "Active")
]

# Create DataFrame
df_employees = spark.createDataFrame(emp_data, schema=emp_schema)

# Display employees table
display(df_employees)

print(f"Total Employees: {df_employees.count()}")

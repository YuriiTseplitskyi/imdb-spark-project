from pyspark.sql.functions import col
from pyspark.sql.types import(
    StringType, 
    IntegerType, 
    FloatType, 
    BooleanType
)

from prettytable import PrettyTable


def print_df_info(df, df_name):

    print("\n" + "=" * 40)
    print(f"\nDataframe: {df_name}") 
    print("\nColumns:")
    df.printSchema()

    num_rows = df.count()
    num_columns = len(df.columns)
    
    # categorizing columns into numerical and categorical
    numerical_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, (IntegerType, FloatType))]
    categorical_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, (StringType, BooleanType))]
    
    # general statistics
    print("\nGeneral statistics for df:")
    table = PrettyTable()
    table.field_names = ["Metric", "Value"]
    table.add_row(["Number of rows", num_rows])
    table.add_row(["Number of columns", num_columns])
    table.add_row(["Number of numerical features", len(numerical_columns)])
    table.add_row(["Number of categorical features", len(categorical_columns)])
    table.align = "r"
    print(table)
    
    # statistics for numerical features
    if numerical_columns:
        print("\nStatistics for numerical features:")
        df.select(numerical_columns).describe().show()
    else:
        print("\nNo numerical columns found in this dataframe.")
        
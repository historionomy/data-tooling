import os
import yaml
import pandas as pd
from supabase import create_client, Client
from sqlalchemy import create_engine, text

env_dict = {}

# Function to map Pandas data types to SQL data types
def pandas_type_to_sql(pandas_type, col_name):

    if "step_" in col_name:
        return 'TEXT'
    if pandas_type == 'int64':
        return 'INTEGER'
    elif pandas_type == 'float64':
        return 'FLOAT'
    elif pandas_type == 'bool':
        return 'BOOLEAN'
    else:  # Default to text for other types, could add more mappings as needed
        return 'TEXT'

def populate_supabase_table(df,dataset_name, engine):

    df.to_sql(dataset_name, con=engine, if_exists='append', index=False, method='multi')

def create_supabase_table(df, table_name, engine):
    # Create table creation SQL
    drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
    drop_statement = text(drop_table_sql)

    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    create_table_sql += ", ".join([f"{col} {pandas_type_to_sql(str(dtype), col)}" for col, dtype in df.dtypes.items()])
    create_table_sql += ");"

    print(create_table_sql)

    statement = text(create_table_sql)

    with engine.connect() as connection:
        print(f"Start connection attempt")
        connection.execute(drop_statement)
        connection.execute(statement)
        connection.commit()

with open('.env') as file:
    for line in file:
        # Strip whitespace and check if the line starts with 'export'
        line = line.strip()
        if line.startswith('export '):
            # Remove 'export ' and then split by the first '='
            key_value_pair = line.replace('export ', '', 1).split('=', 1)
            
            # Check if the line was correctly formatted with an '='
            if len(key_value_pair) == 2:
                key, value = key_value_pair
                env_dict[key] = value

data_structure = {}

data_structure_file = "historionomy-data.yml"

with open(data_structure_file, "r") as file:
    data_structure = yaml.load(file, Loader=yaml.FullLoader)

# Supabase Client
url: str = env_dict.get("SUPABASE_PROJECT_URL")
key: str = env_dict.get("SUPABASE_PROJECT_API_KEY")
print(f"Creating Supabase client with url {url} and API key {key}")
supabase: Client = create_client(url, key)

# SQLAlchemy engine for Supabase (PostgreSQL)
user = "postgres." + env_dict['SUPABASE_PROJECT_ID']
password = env_dict['SUPABASE_PROJECT_PASSWORD']
host = env_dict['SUPABASE_PUBLIC_HOST']
database = "postgres"
port = "5432"
connection_url = f"postgresql+psycopg2://{user}:{password}@{host}/{database}"
# print(connection_url)
engine = create_engine(connection_url)

# mode = "download"
# mode = "initialize"
mode = "upload"

for sheet_name, sheet_content in data_structure.items():
    for dataset_name, dataset_content in sheet_content.items():
        if 'columns' in dataset_content.keys():

            if mode == "download":
                dataset_url = f"{env_dict['HISTORIONOMY_GOOGLE_SOURCE']}/gviz/tq?tqx=out:csv&sheet={sheet_name}&range={dataset_content['columns']}"
                print(f"Loading dataset from google at URL {dataset_url}")
                df = pd.read_csv(dataset_url)
                if dataset_content.get("custom_header", False):
                    df.columns = dataset_content.get("header", [])
                df.to_csv(dataset_name + ".csv")

            if mode == "initialize":
                df = pd.read_csv(dataset_name + ".csv", header=0, index_col=0)
                # remove whitespaces in column names and cast to lowercase
                df.columns = df.columns.str.replace(' ', '_')
                df.columns = df.columns.str.lower()
                # Add "step_" to column names that are numerical values
                df.columns = [f'step_{col}' if col.isdigit() else col for col in df.columns]

                create_supabase_table(df, dataset_name, engine)

            if mode == "upload":
                df = pd.read_csv(dataset_name + ".csv", header=0, index_col=0)
                # remove whitespaces in column names and cast to lowercase
                df.columns = df.columns.str.replace(' ', '_')
                df.columns = df.columns.str.lower()
                # Add "step_" to column names that are numerical values
                df.columns = [f'step_{col}' if col.isdigit() else col for col in df.columns]
                populate_supabase_table(df, dataset_name, engine)

owid_datasets = {
    "literacy": "cross-country-literacy-rates",
    "gdp" : "gdp-per-capita-penn-world-table",
    "urbanization" : "long-term-urban-population-region",
    "gov" : "historical-gov-spending-gdp"
}

# mode = "download"
# mode = "initialize"
# mode = "upload"

# for dataset_id, dataset_name in owid_datasets.items():
#     if mode == "initialize":
#         df = pd.read_csv(dataset_name + ".csv", sep=',', header=0)
#         print(df.columns)
#         for i in range(len(df.columns)):
#             df.columns.values[i] = df.columns.values[i].lower()
#         df.columns.values[3] = dataset_id + "_data"
#         print(df.head())
#         # remove whitespaces in column names and cast to lowercase
#         # df.columns = df.columns.str.replace(' ', '_')
#         # df.columns = df.columns.str.replace('-', '_')
#         # df.columns = df.columns.str.replace('(', '')
#         # df.columns = df.columns.str.replace(')', '')
#         # df.columns = df.columns.str.replace(',', '')
#         # df.columns = df.columns.str.replace('.', '')
#         # df.columns = df.columns.str.lower()
#         create_supabase_table(df, dataset_id, engine)

#     if mode == "upload":
#         df = pd.read_csv(dataset_name + ".csv", sep=',', header=0)
#         for i in range(len(df.columns)):
#             df.columns.values[i] = df.columns.values[i].lower()
#         df.columns.values[3] = dataset_id + "_data"
#         print(df.head())
#         # remove whitespaces in column names and cast to lowercase
#         # df.columns = df.columns.str.replace(' ', '_')
#         # df.columns = df.columns.str.replace('-', '_')
#         # df.columns = df.columns.str.replace('(', '')
#         # df.columns = df.columns.str.replace(')', '')
#         # df.columns = df.columns.str.replace(',', '')
#         # df.columns = df.columns.str.replace('.', '')
#         # df.columns = df.columns.str.lower()
#         populate_supabase_table(df, dataset_id, engine)

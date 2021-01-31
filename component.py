# Currently searches for all parts containing input component based on customer and extracts dimensions
# TODO attach pricing #

# Libraries
import pandas.io.sql
import pandas as pd
import pyodbc
import numpy as np
import re

# Display more columns in terminal
desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns', 10)

# For testing only
cus_no = input('Enter customer code:\n')
cus_no = cus_no.upper()
search_part = input('Enter raw material:\n')
search_part = search_part.upper()

# dims = re.compile(r'(\d*\.?\d*?\s?x\s?){,3}')
dims = re.compile(r'(\d+\.*?\d*?(mm)*?\s*?x*?\d+\.*?\d*?(mm)*?\s+?x+?.+mm+)', flags=re.I)

# Connection info
conx_string = 'DRIVER={SQL Server};SERVER=NMC-JAV; DATABASE=Javelin19r1_Live; '
'TRUSTED CONNECTION=yes'

# SQL query
query = "SELECT Components.Parent_Part_No, Components.Component_Part_No, " \
        " Customer_Part_Link.Customer_No, Customer_Part_Link.Customers_Part_No, Part.Sales_Desc FROM" \
        " ((production.Components " \
        "INNER JOIN sales.Customer_Part_Link ON Components.Parent_Part_No = Customer_Part_Link.Part_No)" \
        "INNER JOIN production.Part ON Components.Parent_Part_No = Part.Part_No) " \
        "WHERE Customer_No = '{}' AND Component_Part_No = '{}';".format(cus_no, search_part)



with pyodbc.connect(conx_string) as conx:
    # Establish dataframes with pandas
    df = pandas.io.sql.read_sql(query, conx)


# Find all components of customer parts
# print(df.tail(50))

# clean sales_desc to extract dims
df['dims'] = df['Sales_Desc'].str.findall(dims)
df = df.drop(columns='Sales_Desc')
print(df.tail(50))

import pandas.io.sql
import pandas as pd
import numpy as np
import pyodbc
import re

# Required formatting for Pandas
desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns',10)

conx_string = 'DRIVER={SQL Server};SERVER=NMC-JAV; DATABASE=Javelin19r1_Live; '
'TRUSTED CONNECTION=yes'

query = "SELECT XX.Part_No, SUBSTRING(WO.WO_No, 0, 6) AS WO_No, SUBSTRING(User_ID, 0, 6) AS Setup_By FROM " \
        "(SELECT Part_No, SUM(WO_Complete) AS no_complete, COUNT(WO_No) AS no_wos " \
        "FROM wip.WO GROUP BY Part_No) AS XX " \
        "LEFT JOIN wip.WO ON WO.Part_No = XX.Part_No " \
        "LEFT JOIN production.Issue_History_File ON XX.Part_No = production.Issue_History_File.Part_No " \
        "WHERE no_complete = 0 AND no_wos = 1;"


with pyodbc.connect(conx_string) as conx:
    df = pandas.io.sql.read_sql(query, conx)

print(df.tail(50))
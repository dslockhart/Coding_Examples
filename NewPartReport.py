import pandas.io.sql
import pandas as pd
import numpy as np
import pyodbc
import os
import datetime

my_date = datetime.date.today()
year, week_num, day_of_wk = my_date.isocalendar()
wk_name = ("Week_" + str(week_num) + "_")
# Required formatting for Pandas
desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns',10)

conx_string = 'DRIVER={SQL Server};SERVER=NMC-JAV; DATABASE=Javelin19r1_Live; '
'TRUSTED CONNECTION=yes'

query = "SELECT XX.Part_No, SUBSTRING(WO.WO_No, 0, 6) AS WO_No, SUBSTRING(User_ID, 0, 6) AS Setup_By, " \
        "Part.Part_Issue FROM " \
        "(SELECT Part_No, SUM(WO_Complete) AS no_complete, COUNT(WO_No) AS no_wos " \
        "FROM wip.WO GROUP BY Part_No) AS XX " \
        "LEFT JOIN wip.WO ON WO.Part_No = XX.Part_No " \
        "LEFT JOIN production.Issue_History_File ON XX.Part_No = production.Issue_History_File.Part_No " \
        "LEFT JOIN production.Part ON XX.Part_No = Part.Part_No " \
        "WHERE no_complete = 0 AND no_wos = 1;"


with pyodbc.connect(conx_string) as conx:
    df = pandas.io.sql.read_sql(query, conx)

path1 = "N:\\Production\\"
prod_folder = []
nm_fold_list = []
for part in df.Part_No:
        part_path = path1+str(part)
        prod_folder.append(os.path.exists(part_path))
        try:
                nm_folder = os.listdir(part_path)
                nm_folder = nm_folder[-1][0:3]
                if nm_folder[0:2] != "NM":
                        nm_folder = "Null"
                nm_fold_list.append(nm_folder)
        except:
                nm_fold_list.append("Null")
drawing = []
dxf = []
for part in df.Part_No:
        part_path = path1+str(part)
        try:
                nm_folder = os.listdir(part_path)
                nm_folder = nm_folder[-1][0:3]
                if nm_folder[0:2] != "NM":
                        drawing.append('Null')
                        dxf.append('Null')
                fold = (part_path+"\\"+nm_folder)
                try:
                        files = os.listdir(fold)
                        for file in files:
                                if file[-4:] == ".pdf":
                                        drawing.append(file)
                                        break
                        for file in files:
                                if file[-4:] == ".dxf" or file[-4:] == 'stp':
                                        dxf.append(file)
                                        break


                except:
                        continue

        except:
                drawing.append('Null')
                dxf.append('Null')


df['Prod_Folder'] = nm_fold_list
df['Drawing'] = drawing
df['DXF/STEP'] = dxf


print(df.tail(50))
filename = wk_name + "NewPartReport.xlsx"
df.to_excel(filename, index=False)


"""
Project: HIV prevention
Version: 1.0
Date: 03-Jun-2022

save_result.py library include functions to processed data in order to create 
statistics and save the data in CSV and Excel format.
"""
# import modules
import csv
from datetime import datetime
from pathlib import Path 
import os
from pyspark.sql.types import StructType,StringType
import xlsxwriter

# getSchema return the header of the CSV file to add to the DataFrame version
def getSchema():
    schema =StructType().add("Key",StringType(),True)\
                .add("Name Surname",StringType(),True)\
                .add("Country",StringType(),True)\
                .add("Age",StringType(),True)\
                .add("Birth sex",StringType(),True)\
                .add("Gender Identity",StringType(),True)\
                .add("Race/Etnicity",StringType(),True)\
                .add("Sexual Orientation",StringType(),True)\
                .add("Sexual Behaviour",StringType(),True)\
                .add("HIV Knowledge",StringType(),True)\
                .add("Living situation",StringType(),True)\
                .add("Drop-in center key",StringType(),True)\
                .add("Relationship status",StringType(),True)\
                .add("Drug Experience",StringType(),True)\
                .add("Intervention",StringType(),True)
    return schema

# createPieExcelFile has as input parameter the dataset as a list and the current drop-in center's name
# and the columns to performs the Statistic, then it return an Excel file with Statistic represented by a Pie Chart 
def createPieExcelFile(data , p_drop_center,main_column,filt_columns,name_chart):
    # Get the name
    file_name = createNameFile('chart_pie',p_drop_center,'xlsx')
    # Create the Excel file 
    workbook = xlsxwriter.Workbook(file_name)
    worksheet = workbook.add_worksheet()
    # Add a bold format to use to highlight cells.
    bold = workbook.add_format({'bold': True})
    # Convert RDD to DataFrame
    schema = getSchema()  
    data_frame = data.toDF(schema = schema)
    # Obtain the list of distinct values from the main column and the list of lists for each filtering columns
    main__col_list = data_frame.select(main_column).distinct().collect()
    filt_cols_lists = []
    for column in filt_columns:
        filt_cols_lists.append(data_frame.select(column).distinct().collect())
    filt_cols_lists_values = []
    for i in range(len(filt_cols_lists[0])):
        filt_cols_lists_values.append(filt_cols_lists[0][i][0])
    #create dictionary
    country_know_dict = {} 
    #list data 1st loop
    for i in range(len(main__col_list)): 
        for j in range(len(filt_cols_lists)):
            for k in range(len(filt_cols_lists[j])):
                country_know_dict[main__col_list[i][0] + filt_cols_lists[j][k][0]] = data_frame.filter((data_frame[main_column] == main__col_list[i][0]) & (data_frame[filt_columns[j]] == filt_cols_lists[j][k][0])).count()
    # Write data headers :
    columns = getColumns()
    # Write the Main Column
    for i in filt_columns:
        worksheet.write('A1',i, bold)
    worksheet.write('A2',main_column, bold)
    # Write the Filetring Columns
    col = 1
    for i in range(len(filt_cols_lists)):
        for j in range(len(filt_cols_lists[i])):
            worksheet.write(columns[col]+str(1),filt_cols_lists[i][j][0], bold)
            col += 1
    # Write the rows
    for i in range(len(main__col_list)): 
      for j in range(len(filt_cols_lists)):
          for k in range(len(filt_cols_lists[j])):
            column_index = i + 3
            worksheet.write('A' + str(column_index)  , main__col_list[i][0])
            worksheet.write(columns[k+1] + str(column_index), country_know_dict[main__col_list[i][0] + filt_cols_lists[j][k][0]])
    # Sum Values from 2 Columns
    worksheet.write(columns[col]+str(1),"Poor+None", bold)
    for i in range(3,len(main__col_list)+3):
        first_column_inx = int(filt_cols_lists_values.index("Poor"))+int(1)
        second_column_inx = int(filt_cols_lists_values.index("None"))+int(1)
        first_column = columns[first_column_inx]
        second_column = columns[second_column_inx]
        worksheet.write(columns[col] + str(i), '=SUM('+str(first_column)+str(i)+':'+str(second_column)+str(i)+')')
    # Create a Pie Chart
    chart1 = workbook.add_chart({'type': 'pie'})
    # Add a data series to a chart
    chart1.add_series({
        'name':       str(name_chart),
        'categories': ['Sheet1', 2, 0, len(main__col_list)+1, 0],
        'values':     ['Sheet1', 2, col, len(main__col_list)+1, col],
        })
    # Add a chart title 
    chart1.set_title({'name': str(name_chart)})
    # Set an Excel chart style. Colors with white outline and shadow.
    chart1.set_style(10)
    # Insert the chart into the worksheet(with an offset).
    last_column = col+2
    worksheet.insert_chart(str(columns[last_column])+str(2), chart1, {'width':60,'height':65,'x_offset': 55, 'y_offset': 55})
    # Close Excel file  
    workbook.close() 

# createExcelNameFile takes as input the name of the new file and the current drop-in center, 
# it returns the file name by concatenating the name-name of the drop-in center-current time.xlsx
def createNameFile(name,p_drop_center,format):
    current_path = Path().absolute()
    t = datetime.now() 
    year = str(t.year)
    month = str(t.month)
    day = str(t.day)
    name_file =str(name) + '_' + str(p_drop_center) + '_' + str(year) + '_' + str(month) + '_' + str(day) + '.' + str(format)
    cross_path_file = os.path.join(str(current_path),'Results',str(name_file))
    return cross_path_file

# getHeaders return a list of headers to use throughout the writing of an Excel file process
def getHeaders():
    headers = ["Key","Name Surname","Country","Age","Birth sex","Gender Identity","Race/Etnicity","Sexual Orientation",
              "Sexual Behaviour","HIV Knowledge","Living situation","Drop-in center key","Relationship status","Drug Experience"
              ,"Intervention"]
    return headers

# getColumns return a list of columns to use throughout the writing of an Excel file process
def getColumns():
    columns = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]
    return columns

# getRowCol return the starting index where to write in an Excel file
def getRowCol():
    row = 1
    column = 0
    return row,column


# saveData has as input parameter the dataset as a list and the current drop-in center's name,
# then create an Excel file of the current dataset
def saveData(data , p_drop_center):
    # Get the name
    file_name = createNameFile('data',p_drop_center,'xlsx')
    # Create the Excel file
    workbook = xlsxwriter.Workbook(file_name)
    worksheet = workbook.add_worksheet()
    # Add a bold format to use to highlight cells.
    bold = workbook.add_format({'bold': True})
    # Write data headers :
    headers = getHeaders()
    columns = getColumns()
    for i in range(len(headers)):
        column = columns[i] + str(1)
        worksheet.write(column, headers[i] , bold)
    # Write the rows :
    row, column = getRowCol()
    for i in range(len(data)):
        column = 0
        for j in range(len(data[i])):
            worksheet.write(row, column, str(data[i][j]))
            column += 1
        row += 1
    # Close the Excel file
    workbook.close()

# saveCSVData takes the list of the current dataset and the name of the drop-in center,
# it return the created CSV file with the respective dataset
def saveCSVData(data,p_drop_center):
    # Get the name
    file_name = createNameFile('data',p_drop_center,'csv')
    # Write in the CSV file
    with open(file_name, 'w',newline='', encoding="utf-8") as csvfile: 
        # creating a csv writer object 
        csvwriter = csv.writer(csvfile,delimiter=';') 
        # writing the data rows 
        csvwriter.writerows(data)
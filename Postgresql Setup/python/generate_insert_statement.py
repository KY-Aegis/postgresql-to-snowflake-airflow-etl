try:
    #import libraries
    import csv
    import os

    # function to convert CSV file to list of INSERT statements
    def csv_to_inserts(csv_file, table_name):
        with open(csv_file, 'r', newline='') as file: #open the file
            reader = csv.reader(file)#read the file
            headers = next(reader)  # get the headers
            inserts = [] #array to store insert statements
            for row in reader: #lloop through each rows
                values = ', '.join(f"'{value}'" for value in row) #get the values
                insert = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({values});" #combine into a insert statement
                inserts.append(insert) #insert into array
        return inserts

    # Array of csv files and matching table names using index
    csv_files = ['customers.csv', 'orders.csv','products.csv', 'sales.csv']  # csv file path
    table_names = ['Customers_Table', 'Orders_Table', 'Products_Table', 'Sales_Data_Table']  # table names

    # loop through the csv files
    for csv_file, table_name in zip(csv_files, table_names):
        insert_statements = csv_to_inserts(csv_file, table_name)
        
        # append INSERT statements to SQL file
        with open('../sql/insert_data.sql', 'a') as sql_file:
            for statement in insert_statements:
                sql_file.write(statement)
                sql_file.write("\n")
        print(f"SQL Conversion Completed")


except Exception as e:
    print(e)
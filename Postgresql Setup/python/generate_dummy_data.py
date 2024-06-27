#try except block to generate dummy data
try:
    #import libraries
    import pandas as pd #to create dataframes
    import numpy as np #to generate random figures
    import json #to convert address to proper json format
    from datetime import datetime #import datetime

    #function to generate dummy data for customers table
    def generate_customer_data():
        dataset_size = 100000 # set the dataset size to 100k million
        customer_id_list = np.arange(1, dataset_size + 1) # generate a list of customer id
        name_list = [f'Customer_{i}' for i in customer_id_list] # generate a list of customer names
        email_list = [f'Customer_{i}@gmail.com' for i in customer_id_list] # generate a list of customer email
        address_list = [] # define address array
        created_on_list = pd.date_range(start='2020-01-01', periods=dataset_size, freq='min')
        updated_on_list = created_on_list + pd.to_timedelta(np.random.randint(0, 24, dataset_size), unit='h')
        
        # loop through the dataset
        for i in range(dataset_size):
            #create a array with a json as the element
            address = [{
                "Street": f"{np.random.randint(1, 1000)} St",
                "City": f"City{np.random.randint(1, 100)}",
                "State": f"State{np.random.randint(1, 50)}",
                "Zip": f"ZipCode{np.random.randint(10000, 99999)}"
            }]
            address_list.append(json.dumps(address)) # append the data to the address list

        # construct a dataframe based on the random data generated
        customers = pd.DataFrame({
        'customer_id': customer_id_list,
        'name': name_list,
        'email': email_list,
        'address': address_list,
        'created_on': created_on_list.strftime('%Y-%m-%d %H:%M:%S'),
        'updated_on': updated_on_list.strftime('%Y-%m-%d %H:%M:%S')
        })

        return customers #return the dataframe

    #function to generate dummy data for products table
    def generate_products_table():
        dataset_size = 1000 # set the dataset size to 10k 
        # Generate Product Data
        product_id_list = np.arange(1, dataset_size + 1) #generate a list of product id
        product_name_list = [f'Widget {i}' for i in product_id_list] # generate a list of product names
        product_description_list = [f'Description {i}' for i in product_id_list] # generate a list of product names
        product_price_list = np.round(np.random.uniform(1, 100, dataset_size), 2)
        created_on_list = pd.date_range(start='2020-01-01', periods=dataset_size, freq='min') #genenrate a list of dates from 1st of Jan 2020
        updated_on_list = created_on_list + pd.to_timedelta(np.random.randint(0, 24, dataset_size), unit='h') # add some random numbers the created on in hour unit
        
        # construct a dataframe based on the random data generated
        products = pd.DataFrame({
            'product_id': product_id_list,
            'name': product_name_list,
            'description': product_description_list,
            'price': product_price_list,
            'created_on': created_on_list.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_on': updated_on_list.strftime('%Y-%m-%d %H:%M:%S')
        })

        return products #return the dataframe
    
    #function to generate dummy data for orders table
    def generate_orders_table(customer_id,product_id):
        # Generate Orders Data
        dataset_size = 100000 # set the dataset size to 100k 
        order_id_list = np.arange(1, dataset_size + 1) #generate a list of order id
        customer_order_id_list = np.random.choice(customer_id, dataset_size) #get a random customer id
        product_order_id_list = np.random.choice(product_id, dataset_size) #get a random product id
        quantity_list = np.random.randint(1, 5, dataset_size) # generate a random qty range from 1 to 5
        created_on_list = pd.date_range(start='2020-01-01', periods=dataset_size, freq='min') # Generate a list of dates from 1st of Jan 2020
        updated_on_list = created_on_list + pd.to_timedelta(np.random.randint(0, 24, dataset_size), unit='h') # add some random numbers the created on in hour unit
        order_date_list = created_on_list.date# Get the dates of the order

        # construct a dataframe based on the random data generated
        orders = pd.DataFrame({
            'order_id': order_id_list,
            'customer_id':customer_order_id_list,
            'product_id':product_order_id_list,
            'quantity': quantity_list,
            'order_date': order_date_list,
            'created_on': created_on_list.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_on': updated_on_list.strftime('%Y-%m-%d %H:%M:%S')
        })

        return orders #return the dataframe

    #function to generate dummy data for sales table
    def generate_sales_table(product_dataframe,orders_dataframe):
        dataset_size = 100000 # set the dataset size to 100k 
        sales_id_list = np.arange(1, dataset_size + 1) #generate a list of order id
        sales_order_id_list = []
        total_amount = []
        sales_date = []
        created_on_list = []
        updated_on_list = []

        for i in range (dataset_size): #loop to generate dummy values
            order_id = np.random.choice(orders_dataframe['order_id'], size=1)[0] #get a random order id
            order_details = orders_dataframe[orders_dataframe['order_id'] == order_id] #get the order details
            product_id = order_details['product_id'].values[0]  # get the product id
            product_details = product_dataframe[product_dataframe['product_id'] == product_id] # get the product details
            sales_order_id_list.append(order_id) # add to the sales order id
            total_amount.append(order_details['quantity'].values[0] * product_details['price'].values[0])  #compute the total amount
            sales_date.append(datetime.strptime(order_details['created_on'].values[0], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d'))  # extract the date
            created_on_list.append(order_details['created_on'].values[0]) # get the timestamp
            updated_on_list.append(order_details['updated_on'].values[0] ) # add a random buffer to the timestamp

        # construct a dataframe based on the random data generated
        sales = pd.DataFrame({
            'sale_id': sales_id_list,
            'order_id':sales_order_id_list,
            'total_amount':total_amount,
            'sale_date': sales_date,
            'created_on': created_on_list,
            'updated_on': updated_on_list
        })

        return sales #return the dataframe

    #main function
    def main():
        #try to execute the code
        try:
            #create the dummy data for each table
            customer_table = generate_customer_data() 
            products_table = generate_products_table()
            order_table = generate_orders_table(customer_table['customer_id'],products_table['product_id'])
            sales_table = generate_sales_table(products_table,order_table)
            
            # Save to CSV
            customer_table.to_csv('customers.csv', index=False)
            products_table.to_csv('products.csv', index=False)
            order_table.to_csv('orders.csv', index=False)
            sales_table.to_csv('sales.csv', index=False)

            #print on success
            print("CSV files generated successfully.")

        #handle the error by printing error
        except Exception as e:
            print(e)
    
    if __name__ == "__main__":
        main()

#handle the error by printing error
except Exception as e:
    print(e)








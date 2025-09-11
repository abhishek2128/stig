import mysql.connector
import pandas as pd

def map_dtype_to_mysql(dtype):
    """Map pandas data types to MySQL data types."""
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'DECIMAL(10, 2)'  # Adjust precision if necessary
    elif pd.api.types.is_string_dtype(dtype):
        return 'VARCHAR(255)'  # Adjust length if necessary
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

def create_table(cursor, df):
    """Dynamically create the table based on the DataFrame's columns and data types."""
    create_table_query = "CREATE TABLE IF NOT EXISTS technical_manager_analytics ("
    
    columns = []
    for col, dtype in zip(df.columns, df.dtypes):
        # Convert column names to lowercase and replace spaces with underscores
        mysql_col_name = col.strip().lower().replace(' ', '_')
        mysql_dtype = map_dtype_to_mysql(dtype)
        columns.append(f"`{mysql_col_name}` {mysql_dtype}")
    
    create_table_query += ", ".join(columns) + ");"
    
    try:
        cursor.execute(create_table_query)
        print("Table created successfully.")
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")

def insert_data(cursor, df):
    """Insert data from DataFrame into MySQL table."""
    insert_query = f"""
    INSERT INTO technical_manager_analytics 
    ({', '.join([f'`{col.strip().lower().replace(" ", "_")}`' for col in df.columns])})
    VALUES ({', '.join(['%s' for _ in df.columns])})
    """
    
    for _, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))
    
    print("Data inserted successfully.")

def get_data_and_analytics():
    """Main function to process data and insert into MySQL."""
    try:
        # Establish MySQL connection
        with mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='stig'
        ) as conn:
            with conn.cursor() as cursor:
                # Query data from MySQL
                query = "SELECT * FROM STIG_detail_150722"
                df = pd.read_sql(query, conn)

                # Clean and preprocess the data
                df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')  # Clean column names
                for col in ['technical.manager', 'stig_ship_type_profile_main', 'classgroup', 'approx._enrol_fee_â£']:
                    if col in df.columns:
                        df[col] = df[col].str.strip()

                # Pivot and aggregate data
                ship_type_counts = df.groupby(['technical.manager', 'tech_manager_domicile', 'lr_region', 'stig_ship_type_profile_main']).size().unstack(fill_value=0)
              
                classgroup_counts = df.groupby(['technical.manager',  'tech_manager_domicile' , 'lr_region', 'classgroup']).size().unstack(fill_value=0)
                
                # Process the enrolment fee and calculate sums and averages
                df['approx._enrol_fee_â£'] = pd.to_numeric(df['approx._enrol_fee_â£'], errors='coerce')

                total_enroll_count = df.groupby(['technical.manager',  'tech_manager_domicile', 'lr_region'])['approx._enrol_fee_â£'].sum()
                avg_per_manager = df.groupby(['technical.manager',  'tech_manager_domicile', 'lr_region'])['approx._enrol_fee_â£'].mean()

                # Combine the pivot tables and add additional columns
                combined = ship_type_counts.join(classgroup_counts, how='outer', lsuffix='_ship', rsuffix='_class').fillna(0)
                combined['fleet_total'] = ship_type_counts.sum(axis=1)
                combined['total_class_ship_count'] = classgroup_counts.sum(axis=1)
                combined['whole_fleet_ex_sers_no_of_ships'] = combined['total_class_ship_count']
                combined['whole_fleet_ex_sers_total_enrol_fee'] = combined.index.map(total_enroll_count)
                combined['whole_fleet_ex_sers_cpe'] = combined.index.map(avg_per_manager)

                # Reset index for easier insertion
                final_df = combined.reset_index()
            

                 # save data in csv file
                final_df.to_csv('technical_manager_analytics.csv', index=False)

                # Step: Dynamically create the table in MySQL
                create_table(cursor, final_df)

                # Step: Insert data into MySQL
                insert_data(cursor, final_df)

                # Commit transaction
                conn.commit()
                print("Transaction committed successfully.")

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    except Exception as e:
        print(f"Unexpected error: {e}")

# Run the function
get_data_and_analytics()

import requests
import pandas as pd
from bs4 import BeautifulSoup
import pymysql
from datetime import datetime, timedelta

def scrape_and_process_data(target_date):
    print(f"Starting data scraping process for {target_date.strftime('%Y-%m-%d')}...")
    
    try:
        # Open the website using requests
        base_url = 'https://www.niggrid.org/'
        
        # Initialize an empty list to store the scraped data
        all_data = []
        
        try:
            # Get the page content
            response = requests.get(base_url)
            
            # Parse the page with BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract table headers
            headers = [header.text.strip() for header in soup.find_all('th')]
            
            # Extract data rows
            table_rows = soup.find_all('tr')
            for row in table_rows:
                cols = row.find_all('td')
                if cols:
                    row_data = [col.text.strip() for col in cols]
                    row_data.insert(0, target_date.strftime('%Y-%m-%d'))
                    all_data.append(row_data)
            
            print(f"Scraped data for {target_date.strftime('%Y-%m-%d')}")
        
        except Exception as e:
            print(f"An error occurred for {target_date.strftime('%Y-%m-%d')}: {e}")
        
        # Convert to DataFrame
        columns = ['Date'] + headers
        hourly_data_df = pd.DataFrame(all_data, columns=columns)
        
        print("Starting data processing...")
        
        # Replace empty strings or whitespace with NaN
        hourly_data_df['Genco'] = hourly_data_df['Genco'].replace(r'^\s*$', pd.NA, regex=True)
        
        # Drop rows where 'Genco' is NaN or empty
        filtered_df = hourly_data_df.dropna(subset=['Genco'])
        
        # Drop the 'TotalGeneration' column if it exists
        if 'TotalGeneration' in filtered_df.columns:
            filtered_df = filtered_df.drop('TotalGeneration', axis=1)
        
        # Drop the '#' column
        filtered_df = filtered_df.drop('#', axis=1)
        
        # Rename columns
        filtered_df = filtered_df.rename(columns={
            '24:00': '00:00',
            'Genco': 'Gencos'
        })
        
        # Unpivot the DataFrame
        unpivoted_df = pd.melt(
            filtered_df,
            id_vars=['Date', 'Gencos'],
            var_name='Hour',
            value_name='EnergyGeneratedMWh'
        )
        
        # Rearrange columns
        unpivoted_df = unpivoted_df[['Date', 'Hour', 'Gencos', 'EnergyGeneratedMWh']]
        
        return unpivoted_df
    
    except Exception as e:
        print(f"Scraping process failed: {e}")
        return None

def load_to_database(df):
    print("Starting database upload...")
    try:
        # Database connection details
        db_host = "148.251.246.72"
        db_port = 3306
        db_name = "jksutauf_nesidb"
        db_user = "jksutauf_martins"
        db_password = "12345678"
        
        # Create a MySQL connection
        db_connection = pymysql.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        
        # Create a cursor
        cursor = db_connection.cursor()
        
        # Prepare the insert statement
        sql = "INSERT INTO combined_data (Date, Hour, Gencos, EnergyGeneratedMWh) VALUES (%s, %s, %s, %s)"
        
        # Create a list of tuples from the dataframe records
        data_tuples = df.to_records(index=False).tolist()
        
        # Execute the SQL command in batches
        batch_size = 1000
        for i in range(0, len(data_tuples), batch_size):
            cursor.executemany(sql, data_tuples[i:i+batch_size])
        
        # Commit the changes and close the connection
        db_connection.commit()
        db_connection.close()
        
        print("Data inserted successfully into the database.")
        
    except Exception as e:
        print(f"An error occurred while uploading to the database: {e}")

def main():
    try:
        # Get yesterday's date
        yesterday = datetime.now() - timedelta(days=1)
        
        # Step 1: Scrape and process the data
        print("Starting ETL process...")
        final_df = scrape_and_process_data(yesterday)
        
        if final_df is not None:
            # Step 2: Load the data to the database
            load_to_database(final_df)
            
            print("ETL process completed successfully.")
        else:
            print("Data scraping failed. Aborting process.")
        
    except Exception as e:
        print(f"An error occurred in the main process: {e}")

if __name__ == "__main__":
    main()
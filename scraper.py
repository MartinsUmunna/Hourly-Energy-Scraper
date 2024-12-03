import pandas as pd
from playwright.sync_api import sync_playwright
from datetime import datetime, timedelta
import pymysql

def scrape_and_process_data(target_date):
    print(f"Starting data scraping process for {target_date.strftime('%Y-%m-%d')}...")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.goto('https://www.niggrid.org/', timeout=120000)

            # Click on the "Generate Hourly Data" button
            hourly_data_button = page.wait_for_selector('#sideContent_loginVWShortCuts_lnkGencoProfile2')
            hourly_data_button.click()

            # Open the calendar
            calendar_element = page.wait_for_selector('#MainContent_txtReadingDate')
            calendar_element.click()

            # Select the year
            select_year = page.query_selector('//*[@id="ui-datepicker-div"]/div/div/select[2]')
            select_year.select_option(str(target_date.year))

            # Select the month
            select_month = page.query_selector('//*[@id="ui-datepicker-div"]/div/div/select[1]')
            select_month.select_option(str(target_date.month - 1))

            # Select the day
            day_element = page.wait_for_selector(f'//*[@id="ui-datepicker-div"]/table/tbody/tr/td/a[text()="{target_date.day}"]')
            day_element.click()

            # Click the "Generate Readings" button
            generate_button = page.wait_for_selector('#MainContent_btnGetReadings')
            generate_button.click()

            # Wait for the data to load
            page.wait_for_timeout(8000)

            # Extract table headers
            headers = [header.text_content().strip() for header in page.query_selector_all('th')]

            # Extract data rows
            table_rows = page.query_selector_all('tr')
            all_data = []
            for row in table_rows:
                cols = row.query_selector_all('td')
                if cols:
                    row_data = [col.text_content().strip() for col in cols]
                    row_data.insert(0, target_date.strftime('%Y-%m-%d'))
                    all_data.append(row_data)

            print(f"Scraped data for {target_date.strftime('%Y-%m-%d')}")
            browser.close()

        # Convert to DataFrame
        columns = ['Date'] + headers
        hourly_data_df = pd.DataFrame(all_data, columns=columns)

        print("Starting data processing...")

        # Replace empty strings or whitespace with NaN
        hourly_data_df = hourly_data_df.replace(r'^\s*$', pd.NA, regex=True)

        # Drop rows where 'Genco' is NaN or empty
        hourly_data_df = hourly_data_df.dropna(subset=['Genco'])

        # Drop the 'TotalGeneration' column if it exists
        if 'TotalGeneration' in hourly_data_df.columns:
            hourly_data_df = hourly_data_df.drop('TotalGeneration', axis=1)

        # Drop the '#' column
        hourly_data_df = hourly_data_df.drop('#', axis=1)

        # Rename columns
        hourly_data_df = hourly_data_df.rename(columns={
            '24:00': '00:00',
            'Genco': 'Gencos'
        })

        # Unpivot the DataFrame
        unpivoted_df = pd.melt(
            hourly_data_df,
            id_vars=['Date', 'Gencos'],
            var_name='Hour',
            value_name='EnergyGeneratedMWh'
        )

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
        sql = "INSERT INTO combined_hourly_energy_generated_mwh (Date, Hour, Gencos, EnergyGeneratedMWh) VALUES (%s, %s, %s, %s)"

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
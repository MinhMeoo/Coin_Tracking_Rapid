

from fetch_data import fetch_all_data, save_data_to_excel
from fetch_data import generate_report, fetch_and_update_data

import schedule
import time
from datetime import datetime

if __name__ == "__main__":
    try:
        # Fetch dữ liệu cho tất cả các đồng coin
        print("Fetching data for all symbols...")
        all_data = fetch_all_data()

        # Kiểm tra nếu không có dữ liệu
        if not all_data:
            print("No data fetched. Exiting...")
        else:
            print(f"Fetched data for {len(all_data)} symbols.")

            # Lưu dữ liệu vào file Excel
            print("Saving data to Excel...")
            save_data_to_excel(all_data)
            print("Data saved to Excel successfully.")

            # Generate báo cáo và lưu vào file report.txt
            print("Generating report...")
            generate_report(all_data)
            print("Report generated and saved to report.txt.")

    except Exception as e:
        print(f"An error occurred: {e}")

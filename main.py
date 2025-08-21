

from fetch_data import fetch_all_data, save_data_to_excel
from fetch_data import generate_report, fetch_and_update_data
from datetime import datetime
from notice import send_email_report

import schedule
import time
import subprocess

import schedule
import time

def main():
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

            # Gửi email nếu report có nội dung
            send_email_report()
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Lên lịch chạy mỗi 15 phút
    schedule.every(15).minutes.do(main)

    print("⏳ Scheduler started, will run every 15 minutes...")
    while True:
        schedule.run_pending()
        time.sleep(1)

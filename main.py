from fetch_data import fetch_all_data, save_data_to_excel
from fetch_data import generate_report, fetch_and_update_data
from datetime import datetime
from notice import send_email_report
import subprocess
import schedule
import time

from datetime import datetime

import schedule
import time
from datetime import datetime, timedelta

def main():
    try:
        now = datetime.now()
        print(f"\n⏳ Running task at {now.strftime('%H:%M:%S')}")

        # Fetch dữ liệu cho tất cả các đồng coin
        print("Fetching data for all symbols...")
        all_data = fetch_all_data()

        if not all_data:
            print("No data fetched. Exiting...")
        else:
            print(f"Fetched data for {len(all_data)} symbols.")

            print("Saving data to Excel...")
            save_data_to_excel(all_data)
            print("Data saved to Excel successfully.")

            print("Generating report...")
            generate_report(all_data)
            print("Report generated and saved to report.txt.")

            send_email_report()

    except Exception as e:
        print(f"An error occurred: {e}")


def wait_until_next_quarter():
    now = datetime.now()
    # tính phút kế tiếp chia hết cho 15
    minute = ((now.minute // 15) + 1) * 15
    if minute == 60:
        next_run = now.replace(hour=now.hour+1, minute=0, second=0, microsecond=0)
    else:
        next_run = now.replace(minute=minute, second=0, microsecond=0)

    # giới hạn khung giờ 6h30–22h
    if next_run.hour < 6 or (next_run.hour == 6 and next_run.minute < 30):
        next_run = now.replace(hour=6, minute=30, second=0, microsecond=0)
    elif next_run.hour >= 22:
        tomorrow = now + timedelta(days=1)
        next_run = tomorrow.replace(hour=6, minute=30, second=0, microsecond=0)

    sleep_seconds = (next_run - now).total_seconds()
    print(f"⏸ Sleeping until {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
    time.sleep(sleep_seconds)

if __name__ == "__main__":
    while True:
        now = datetime.now()
        if (6 < now.hour < 22) or (now.hour == 6 and now.minute >= 30):
            main()
        wait_until_next_quarter()
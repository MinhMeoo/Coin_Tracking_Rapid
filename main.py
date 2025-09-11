from notice import send_email_report
from fetch_data import (
    fetch_all_data,
    save_data_to_excel,
    generate_report,
    fetch_and_update_data,
    fetch_append_latest_15m_candle,
    SYMBOLS_LIST,
)

import os
import time
import pandas as pd
from datetime import datetime, timedelta, timezone

# --- Cấu hình ---
DATA_FOLDER = "./datafiles"
os.makedirs(DATA_FOLDER, exist_ok=True)

REPORT_READY = False

# --- Hàm main (fetch full 30 nến) ---
def main():
    global REPORT_READY
    REPORT_READY = False
    try:
        now = datetime.now()
        print(f"\n⏳ Running task at {now.strftime('%H:%M:%S')}")

        print("Fetching full 30 candles for all symbols...")
        all_data = fetch_all_data()  # fetch toàn bộ 30 nến
        if not all_data:
            print("No data fetched. Exiting...")
            return

        print(f"Fetched data for {len(all_data)} symbols.")

        print("Saving data to Excel...")
        save_data_to_excel(all_data)
        print("Data saved to Excel successfully.")

        print("Generating report...")
        generate_report(all_data)
        print("Report generated and saved to report.txt.")

        REPORT_READY = True

    except Exception as e:
        print(f"[ERROR] main() exception: {e}")


# --- Hàm chờ tới quý tiếp theo ---
def wait_until_next_quarter():
    now = datetime.now()
    minute = ((now.minute // 15) + 1) * 15
    if minute == 60:
        next_run = now.replace(hour=now.hour + 1, minute=0, second=0, microsecond=0)
    else:
        next_run = now.replace(minute=minute, second=0, microsecond=0)

    # Giới hạn khung giờ 6:00–20:00
    if now.hour < 6:
        next_run = now.replace(hour=6, minute=0, second=0, microsecond=0)
    elif now.hour >= 23:
        tomorrow = now + timedelta(days=1)
        next_run = tomorrow.replace(hour=6, minute=0, second=0, microsecond=0)

    sleep_seconds = (next_run - now).total_seconds()
    print(f"⏸ Sleeping until {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
    time.sleep(sleep_seconds)



# --- Vòng lặp chính ---
if __name__ == "__main__":
    while True:
        now_local = datetime.now()
        fetch_full = False

        # Case 1: 6:00 sáng → fetch full
        if now_local.hour == 6 and now_local.minute == 0:
            fetch_full = True
            print(f"[DEBUG] Time is 6:00 → fetch_full = True")
        else:
            # Case 2: kiểm tra file dữ liệu → fetch full nếu mất hoặc cũ
            for symbol in SYMBOLS_LIST:
                excel_file = os.path.join(DATA_FOLDER, f"{symbol}_data.xlsx")

                if not os.path.exists(excel_file):
                    fetch_full = True
                    print(f"[DEBUG] {symbol}: file not exists -> fetch_full = True")
                    break

                try:
                    df = pd.read_excel(excel_file)
                    last_ts = pd.to_datetime(df["timestamp"].iloc[-1])

                    if last_ts.tzinfo is None:
                        last_ts = last_ts.tz_localize("UTC")
                        print(f"[DEBUG] {symbol}: tz_localize applied -> last_ts = {last_ts}")
                    else:
                        last_ts = last_ts.tz_convert("UTC")
                        print(f"[DEBUG] {symbol}: tz_convert applied -> last_ts = {last_ts}")

                except Exception as e:
                    fetch_full = True
                    print(f"[DEBUG] {symbol}: exception reading file -> fetch_full = True, {e}")
                    break

                now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
                gap_minutes = (now_utc - last_ts).total_seconds() / 60

                if gap_minutes > 60:
                    fetch_full = True
                    break
            else:
                fetch_full = False
                print(f"[DEBUG] All files OK and recent → fetch_full = False")

        # --- Thực hiện fetch ---
        if fetch_full:
            print(f"[INFO] Fetching full 30 candles for all symbols...")
            main()
        else:
            print(f"[INFO] Fetching only latest 1 candle for all symbols...")
            # 1. Fetch 1 nến cho tất cả symbol (hàm gốc tự loop SYMBOLS_LIST)
            try:
                all_data = fetch_and_update_data()
                print(f"[DEBUG] 1-candle fetch_and_update_data completed")
            except Exception as e:
                print(f"[DEBUG] error updating 1 candle -> {e}")

            # 2. Generate report
            try:
                print(f"[DEBUG] Generating report after 1-candle fetch...")
                generate_report(all_data)
                print(f"[DEBUG] Report generated")
                REPORT_READY = True
            except Exception as e:
                print(f"[DEBUG] Error generating report → {e}")
                REPORT_READY = False

        # --- Gửi email 1 lần ---
        if REPORT_READY:
            try:
                send_email_report()
                print(f"[DEBUG] Email sent successfully")
            except Exception as e:
                print(f"[DEBUG] Error sending email → {e}")
            finally:
                REPORT_READY = False

        wait_until_next_quarter()

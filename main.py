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
        print(f"[DEBUG] Files currently in {DATA_FOLDER}: {os.listdir(DATA_FOLDER)}")
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
    elif now.hour >= 22:
        tomorrow = now + timedelta(days=1)
        next_run = tomorrow.replace(hour=6, minute=0, second=0, microsecond=0)

    sleep_seconds = (next_run - now).total_seconds()
    print(f"⏸ Sleeping until {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
    time.sleep(sleep_seconds)



# --- Vòng lặp chính ---
if __name__ == "__main__":
    # --- Khai báo khung giờ cho phép chạy (local time) ---
    # Mỗi tuple: (start_hour, start_minute, end_hour, end_minute)
    ALLOWED_WINDOWS = [
        (6, 0, 10, 30),    # 06:00 -> 10:30
        (12, 30, 14, 30),  # 12:30 -> 14:30
        (17, 30, 19, 30),  # 17:30 -> 19:30
        (22, 0, 23, 30),   # 22:00 -> 23:30
    ]

    def _in_allowed_window(now_dt):
        """Trả True nếu now_dt rơi trong 1 trong các khung giờ cho phép."""
        for sh, sm, eh, em in ALLOWED_WINDOWS:
            start = now_dt.replace(hour=sh, minute=sm, second=0, microsecond=0)
            end = now_dt.replace(hour=eh, minute=em, second=0, microsecond=0)
            if start <= now_dt < end:
                return True
        return False

    def _next_allowed_start(now_dt):
        """
        Trả về datetime của start window tiếp theo (cùng ngày hoặc ngày sau).
        Nếu now_dt đang trong window -> trả về now_dt (không sleep).
        """
        # Nếu đang trong window, trả chính now_dt (caller sẽ tiếp tục làm việc)
        if _in_allowed_window(now_dt):
            return now_dt

        # Kiểm tra các window còn lại trong cùng ngày
        candidates = []
        for sh, sm, eh, em in ALLOWED_WINDOWS:
            start = now_dt.replace(hour=sh, minute=sm, second=0, microsecond=0)
            if start > now_dt:
                candidates.append(start)

        if candidates:
            return min(candidates)

        # Không còn window trong hôm nay -> trả về start của window đầu tiên ngày mai
        sh, sm, eh, em = ALLOWED_WINDOWS[0]
        tomorrow = now_dt + timedelta(days=1)
        return tomorrow.replace(hour=sh, minute=sm, second=0, microsecond=0)

    # --- Vòng lặp chính ---
    while True:
        now_local = datetime.now()
        print(f"[DEBUG] Loop tick at local time: {now_local.strftime('%Y-%m-%d %H:%M:%S')}")

        # Nếu hiện tại KHÔNG nằm trong khung cho phép -> sleep tới start tiếp theo rồi continue
        if not _in_allowed_window(now_local):
            next_start = _next_allowed_start(now_local)
            sleep_seconds = (next_start - now_local).total_seconds()
            # bảo đảm sleep ít nhất 1 giây nếu sai số âm
            if sleep_seconds <= 0:
                sleep_seconds = 1
            print(f"[DEBUG] Now is outside allowed windows. Sleeping until next allowed start: {next_start.strftime('%Y-%m-%d %H:%M:%S')} (sleep {int(sleep_seconds)}s)")
            time.sleep(sleep_seconds)
            continue  # quay lại kiểm tra (không gọi main hoặc fetch)

        # Nếu đến đây nghĩa là đang ở trong 1 khung giờ cho phép -> chạy logic cũ
        fetch_full = False

        # Case 1: 6:00 sáng → fetch full (giữ nguyên hành vi)
        if now_local.hour == 6 and now_local.minute == 0:
            fetch_full = True
            print(f"[DEBUG] Time is 06:00 → fetch_full = True")
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
                    else:
                        last_ts = last_ts.tz_convert("UTC")

                except Exception as e:
                    fetch_full = True
                    print(f"[DEBUG] {symbol}: exception reading file -> fetch_full = True, {e}")
                    break

                now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
                gap_minutes = (now_utc - last_ts).total_seconds() / 60

                if gap_minutes > 60:
                    fetch_full = True
                    print(f"[DEBUG] {symbol}: gap_minutes = {gap_minutes:.1f} > 60 → fetch_full = True")
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
            # 1. Fetch 1 nến cho tất cả symbol
            try:
                all_data = fetch_and_update_data()
                print(f"[DEBUG] 1-candle fetch_and_update_data completed")
            except Exception as e:
                print(f"[DEBUG] error updating 1 candle -> {e}")
                all_data = {}

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

        # Chờ tới quý 15 phút tiếp theo (hàm hiện có của bạn)
        wait_until_next_quarter()

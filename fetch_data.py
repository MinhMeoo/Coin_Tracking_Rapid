import requests
import pandas as pd
import os
from config import SYMBOLS_LIST

import time
from datetime import datetime, timedelta

INTERVAL_MS = 15 * 60 * 1000  # 15 minutes in ms
DATA_FOLDER = "/Users/minhmeoow/MiniProject_Python/Coin_tracking_rapid_test_m15/datafiles"
os.makedirs(DATA_FOLDER, exist_ok=True)

def get_binance_server_time():
    """Lấy serverTime (ms) từ Binance futures (fapi)."""
    url = "https://fapi.binance.com/fapi/v1/time"
    resp = requests.get(url, timeout=5)
    resp.raise_for_status()
    return int(resp.json()["serverTime"])
#  Ham nay fetch du data 30 nen, thuong duoc call vao luc 6h sang

def fetch_15m_closed_klines(symbol: str, limit: int = 30, use_server_time: bool = True, retries: int = 2, delay_retry: float = 1.0):
    """
    Lấy `limit` nến 15m **đã đóng** (previous closed candles) cho symbol.
    - use_server_time=True: dùng thời gian server Binance để đồng bộ (khuyến nghị).
    - Trả về DataFrame đã convert kiểu và có các cột numeric.
    """
    url = "https://fapi.binance.com/fapi/v1/klines"
    for attempt in range(1, retries + 1):
        try:
            if use_server_time:
                server_ms = get_binance_server_time()
                boundary_ms = (server_ms // INTERVAL_MS) * INTERVAL_MS
            else:
                # fallback dùng local UTC time
                now = datetime.utcnow()
                boundary = now.replace(second=0, microsecond=0) - timedelta(minutes=now.minute % 15)
                boundary_ms = int(boundary.timestamp() * 1000)

            # endTime = boundary_ms - 1 ensures we only get candles that closed strictly BEFORE boundary
            end_time_ms = boundary_ms - 1

            params = {
                "symbol": symbol.upper(),
                "interval": "15m",
                "limit": limit,
                "endTime": end_time_ms
            }

            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if not data:
                # có thể thị trường quá mới hoặc symbol không hợp lệ
                return None

            df = pd.DataFrame(data, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_asset_volume", "number_of_trades",
                "taker_buy_base", "taker_buy_quote", "ignore"
            ])

            # convert types
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
            numeric_cols = ["open", "high", "low", "close", "volume"]
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

            # tính các cột bạn cần
            df["delta_change"] = (df["close"] - df["open"]) / df["open"]
            # average_volume_20: trung bình 20 nến TRƯỚC nến hiện tại
            df["average_volume_20"] = pd.to_numeric(df["volume"], errors="coerce").rolling(20).mean().shift(1)

            # giữ các cột cần thiết
            df_out = df[["open_time", "open", "high", "low", "close", "volume", "delta_change", "average_volume_20"]].copy()
            df_out.rename(columns={"open_time": "timestamp"}, inplace=True)

            return df_out

        except Exception as e:
            print(f"⚠️ Fetch error for {symbol} (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(delay_retry)
                continue
            else:
                return None

#  ham nay call vao moi 15p
def fetch_append_latest_15m_candle(symbol: str, excel_file: str):
    """
    Lấy 1 nến 15m mới nhất, append vào cuối file Excel.
    - symbol: ví dụ "BTCUSDT"
    - excel_file: đường dẫn file Excel
    """
    try:
        # đọc dữ liệu cũ nếu có
        if os.path.exists(excel_file):
            df_old = pd.read_excel(excel_file)
        else:
            df_old = pd.DataFrame()

        # lấy thời gian server Binance
        server_ms = get_binance_server_time()
        boundary_ms = (server_ms // INTERVAL_MS) * INTERVAL_MS
        end_time_ms = boundary_ms - 1  # nến đóng ngay trước boundary

        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {
            "symbol": symbol.upper(),
            "interval": "15m",
            "limit": 1,
            "endTime": end_time_ms
        }

        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            print(f"[{datetime.now()}] ⚠️ No new candle for {symbol}")
            return

        df_new = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base", "taker_buy_quote", "ignore"
        ])

        # convert kiểu
        df_new["open_time"] = pd.to_datetime(df_new["open_time"], unit="ms")
        df_new["close_time"] = pd.to_datetime(df_new["close_time"], unit="ms")
        numeric_cols = ["open", "high", "low", "close", "volume"]
        df_new[numeric_cols] = df_new[numeric_cols].apply(pd.to_numeric, errors="coerce")

        # tính các cột cần thiết
        df_new["delta_change"] = (df_new["close"] - df_new["open"]) / df_new["open"]

        # rename open_time → timestamp trước khi append
        df_new.rename(columns={"open_time": "timestamp"}, inplace=True)

        # append dữ liệu mới vào df_old
        if not df_old.empty:
            # tránh duplicate nến
            if df_new["timestamp"].iloc[0] <= df_old["timestamp"].iloc[-1]:
                print(f"[{datetime.now()}] Candle already exists for {symbol}, skip")
                df = df_old.copy()
            else:
                df = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df = df_new.copy()

        # tính lại average_volume_20 cho toàn bộ bảng
        df["average_volume_20"] = df["volume"].rolling(20).mean().shift(1)

        # giữ các cột cần thiết
        cols_keep = ["timestamp", "open", "high", "low", "close", "volume", "delta_change", "average_volume_20"]
        df_out = df[cols_keep].copy()

        # lưu Excel
        df_out.to_excel(excel_file, index=False)
        print(f"[{datetime.now()}] ✅ Appended 1 candle for {symbol}, total rows: {len(df_out)}")

    except Exception as e:
        print(f"[{datetime.now()}] ⚠️ Error fetching/appending candle for {symbol}: {e}")



def fetch_all_data(delay=0.1, retries=3):
    """
    Lấy dữ liệu cho tất cả các đồng coin trong SYMBOLS_LIST.
    - delay: nghỉ giữa các lần gọi API (tránh bị chặn).
    - retries: số lần thử lại nếu lỗi.
    """
    all_data = {}

    for symbol in SYMBOLS_LIST:   # ✅ dùng global SYMBOLS_LIST luôn
        for attempt in range(retries):
            try:
                df = fetch_15m_closed_klines(symbol, limit=30)
                all_data[symbol] = df
                print(f"✅ {symbol}: fetched {len(df)} rows")
                break
            except Exception as e:
                print(f"⚠️ Error fetching {symbol} (attempt {attempt+1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(3)
                else:
                    print(f"❌ Skipping {symbol} sau {retries} lần thất bại")

        time.sleep(delay)

    return all_data



def save_data_to_excel(all_data):
    # Tạo thư mục datafiles nếu chưa có
    if not os.path.exists('datafiles'):
        os.makedirs('datafiles')
    
    # Lưu dữ liệu vào từng file Excel riêng biệt cho mỗi đồng coin
    for symbol, data in all_data.items():
        # Đặt tên file theo tên đồng coin, ví dụ: BTCUSDT.xlsx
        file_path = f'datafiles/{symbol}_data.xlsx'
        data.to_excel(file_path, index=False)

from datetime import datetime

def calculate_delta_change(open_price, close_price):
    """Tính toán tỷ lệ thay đổi giá từ mở đến đóng."""
    return round((close_price - open_price) / open_price, 4)  # Làm tròn đến 4 chữ số sau dấu phẩy

def calculate_average_volume(df, window=20):
    """Tính toán trung bình khối lượng của 20 nến gần nhất."""
    return df['volume'].rolling(window=window).mean()

# debug function function
def generate_report(all_data):
    """Tạo báo cáo và ghi vào file report.txt (có debug chi tiết)."""
    try:
        if not all_data:
            print("No data to generate report.")
            return
        
        with open("report.txt", "w") as file:
            for symbol, data in all_data.items():
                try:
                    last_row = data.iloc[-1]  # Lấy dòng dữ liệu mới nhất
                    
                    last_delta_change = last_row.get('delta_change', None)
                    last_volume = last_row.get('volume', None)
                    avg_volume_20 = last_row.get('average_volume_20', None)
                    last_timestamp = last_row.get('timestamp', None)

                    # Check lỗi NaN
                    if pd.isna(last_delta_change) or pd.isna(last_volume) or pd.isna(avg_volume_20):
                        print(f"[WARNING] {symbol} có dữ liệu NaN, bỏ qua.")
                        continue
                    
                    # Định dạng timestamp
                    if isinstance(last_timestamp, pd.Timestamp):
                        formatted_timestamp = last_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        formatted_timestamp = str(last_timestamp)  # fallback
                    
                    # Điều kiện log
                    if last_delta_change > 0 and last_volume >= 4.2 * avg_volume_20:
                        line = f"{symbol} - Delta Change: {last_delta_change:.4f} at {formatted_timestamp}\n"
                        file.write(line)
                        print(f"[INFO] {symbol} thỏa điều kiện -> ghi vào report.")
                
                except Exception as e:
                    print(f"❌ Error processing {symbol}: {e}")
                
        print("✅ Report generated and saved to report.txt.")
    except Exception as e:
        print(f"❌ Error generating report: {e}")


# def generate_report(all_data):
#     """Tạo báo cáo và ghi vào file report.txt."""
#     try:
#         if not all_data:
#             print("No data to generate report.")
#             return
        
#         with open("report.txt", "w") as file:
#             for symbol, data in all_data.items():
#                 try:
#                     last_row = data.iloc[-1]  # Lấy dòng dữ liệu mới nhất
#                     last_delta_change = last_row['delta_change']
#                     last_volume = last_row['volume']
#                     avg_volume_20 = last_row['average_volume_20']
#                     last_timestamp = last_row['timestamp']
                    
#                     # Định dạng lại timestamp để có dạng đầy đủ: YYYY-MM-DD HH:mm:ss
#                     formatted_timestamp = last_timestamp.strftime('%Y-%m-%d %H:%M:%S')

#                     # Điều kiện: delta_change > 0 và volume >= 3 lần trung bình 20 nến
#                     if last_delta_change > 0 and last_volume >= 3.5 * avg_volume_20:
#                         # Ghi vào file report
#                         file.write(f"{symbol} - Delta Change: {last_delta_change:.4f} at {formatted_timestamp}\n")
#                 except Exception as e:
#                     print(f"Error processing {symbol}: {e}")
                
#         print("Report generated and saved to report.txt.")
#     except Exception as e:
#         print(f"Error generating report: {e}")




def fetch_and_update_data():
    """
    Fetch 1 nến 15m mới nhất cho tất cả symbol và append vào Excel.
    Trả về all_data dict {symbol: df} để generate_report sử dụng.
    """
    all_data = {}

    for symbol in SYMBOLS_LIST:
        try:
            print(f"[DEBUG] Fetching latest 1 candle for {symbol}...")

            excel_file = os.path.join(DATA_FOLDER, f"{symbol}_data.xlsx")
            fetch_append_latest_15m_candle(symbol, excel_file)

            # đọc lại toàn bộ dữ liệu từ Excel để dùng cho report
            if os.path.exists(excel_file):
                df = pd.read_excel(excel_file)
                all_data[symbol] = df
                print(f"[DEBUG] {symbol}: {len(df)} rows loaded for report")
            else:
                print(f"[DEBUG] {symbol}: file not found after append")

        except Exception as e:
            print(f"[DEBUG] Error updating {symbol}: {e}")

    print(f"[DEBUG] Fetched and updated data for {len(all_data)} symbols.")
    return all_data
import requests
import pandas as pd
import os
from config import SYMBOLS_LIST

import time
from datetime import datetime, timedelta
from typing import Dict, Optional, List


INTERVAL_MS = 15 * 60 * 1000  # 15 minutes in ms
DATA_FOLDER = "/Users/minhmeoow/MiniProject_Python/Coin_tracking_rapid/datafiles"
os.makedirs(DATA_FOLDER, exist_ok=True)


def get_binance_server_time() -> int:
    """Lấy serverTime (ms) từ Binance futures (fapi)."""
    url = "https://fapi.binance.com/fapi/v1/time"
    resp = requests.get(url, timeout=5)
    resp.raise_for_status()
    return int(resp.json()["serverTime"])





#  Ham nay fetch du data 30 nen, thuong duoc call vao luc 6h sang
def fetch_15m_closed_klines(
    symbol: str,
    limit: int = 30,
    use_server_time: bool = True,
    server_ms: Optional[int] = None,
    retries: int = 1,
    delay_retry: float = 1.0,
) -> Optional[pd.DataFrame]:
    """
    Lấy `limit` nến 15m đã đóng cho symbol.
    
    Args:
        symbol (str): ví dụ "BTCUSDT"
        limit (int): số lượng nến cần lấy
        use_server_time (bool): nếu True thì dùng server time Binance, False dùng local time
        server_ms (Optional[int]): serverTime đã có sẵn, nếu None sẽ tự fetch
        retries (int): số lần retry khi lỗi request
        delay_retry (float): thời gian chờ (giây) giữa các lần retry
    
    Returns:
        Optional[pd.DataFrame]: bảng dữ liệu nến hoặc None nếu lỗi
    """
    url = "https://fapi.binance.com/fapi/v1/klines"

    for attempt in range(1, retries + 1):
        try:
            # xác định endTime
            if use_server_time:
                if server_ms is None:
                    server_ms = get_binance_server_time()
                boundary_ms = (server_ms // INTERVAL_MS) * INTERVAL_MS
            else:
                # fallback dùng local UTC time
                now = datetime.utcnow()
                boundary = now.replace(second=0, microsecond=0) - timedelta(minutes=now.minute % 15)
                boundary_ms = int(boundary.timestamp() * 1000)

            # endTime = boundary_ms - 1 → chỉ lấy nến đã đóng
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
                # symbol không hợp lệ hoặc thị trường quá mới
                return None

            df = pd.DataFrame(data, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_asset_volume", "number_of_trades",
                "taker_buy_base", "taker_buy_quote", "ignore"
            ])

            # convert kiểu
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
            numeric_cols = ["open", "high", "low", "close", "volume"]
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

            # tính thêm cột
            df["delta_change"] = (df["close"] - df["open"]) / df["open"]
            df["average_volume_20"] = pd.to_numeric(df["volume"], errors="coerce").rolling(20).mean().shift(1)

            # giữ các cột cần thiết
            df_out = df[["open_time", "open", "high", "low", "close", "volume", "delta_change", "average_volume_20"]].copy()
            df_out.rename(columns={"open_time": "timestamp"}, inplace=True)

            return df_out

        except Exception as e:
            print(f"[{datetime.now()}] ⚠️ Fetch error for {symbol} (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(delay_retry)
                continue
            else:
                return None




#  ham nay call vao moi 15p
def fetch_append_latest_15m_candle(
    symbol: str,
    excel_file: str,
    server_ms: Optional[int] = None,
    retries: int = 2,
    delay_retry: float = 1.0,
) -> Optional[pd.DataFrame]:
    """
    Lấy 1 nến 15m mới nhất và append vào cuối file Excel.
    Trả về DataFrame mới sau khi cập nhật (hoặc None nếu lỗi).
    """
    try:
        # đọc dữ liệu cũ nếu có
        if os.path.exists(excel_file):
            df_old = pd.read_excel(excel_file)
        else:
            df_old = pd.DataFrame()

        # tính boundary dựa vào server_ms (được truyền từ ngoài vào)
        boundary_ms = (server_ms // INTERVAL_MS) * INTERVAL_MS
        end_time_ms = boundary_ms - 1  # nến đóng ngay trước boundary

        # fetch dữ liệu
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {
            "symbol": symbol.upper(),
            "interval": "15m",
            "limit": 1,
            "endTime": end_time_ms,
        }

        resp = None
        for attempt in range(1, retries + 1):
            try:
                resp = requests.get(url, params=params, timeout=10)
                resp.raise_for_status()
                break
            except Exception as e:
                print(f"[{datetime.now()}] ⚠️ Error fetching {symbol} (attempt {attempt}/{retries}): {e}")
                if attempt < retries:
                    time.sleep(delay_retry)
                    continue
                else:
                    return None

        data = resp.json()
        if not data:
            print(f"[{datetime.now()}] ⚠️ No new candle for {symbol}")
            return None

        # build df_new
        df_new = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base", "taker_buy_quote", "ignore"
        ])
        df_new["open_time"] = pd.to_datetime(df_new["open_time"], unit="ms")
        df_new["close_time"] = pd.to_datetime(df_new["close_time"], unit="ms")
        numeric_cols = ["open", "high", "low", "close", "volume"]
        df_new[numeric_cols] = df_new[numeric_cols].apply(pd.to_numeric, errors="coerce")
        df_new["delta_change"] = (df_new["close"] - df_new["open"]) / df_new["open"]
        df_new.rename(columns={"open_time": "timestamp"}, inplace=True)

        # append dữ liệu mới
        if not df_old.empty:
            if df_new["timestamp"].iloc[0] <= df_old["timestamp"].iloc[-1]:
                print(f"[{datetime.now()}] Candle already exists for {symbol}, skip")
                df = df_old.copy()
            else:
                df = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df = df_new.copy()

        # tính lại average_volume_20
        df["average_volume_20"] = df["volume"].rolling(20).mean().shift(1)

        # giữ cột cần thiết
        cols_keep = ["timestamp", "open", "high", "low", "close", "volume", "delta_change", "average_volume_20"]
        df_out = df[cols_keep].copy()

        # lưu ra Excel
        df_out.to_excel(excel_file, index=False)

        return df_out

    except Exception as e:
        print(f"[{datetime.now()}] ⚠️ Error in fetch_append_latest_15m_candle for {symbol}: {e}")
        return None



# /* gọi hàm fetch 15m và ghi data vào file
def fetch_and_update_data(delay: float = 0.1) -> Dict[str, pd.DataFrame]:
    """
    Fetch 1 nến 15m mới nhất cho tất cả symbol và append vào Excel.
    Trả về all_data dict {symbol: df} để generate_report sử dụng.
    - delay: nghỉ giữa các request để tránh bị block
    """
    all_data = {}

    # gọi server time 1 lần + delay 2s để chắc chắn nến đã đóng
    server_ms = get_binance_server_time()
    time.sleep(2)

    for symbol in SYMBOLS_LIST:
        try:

            excel_file = os.path.join(DATA_FOLDER, f"{symbol}_data.xlsx")
            df = fetch_append_latest_15m_candle(symbol, excel_file, server_ms=server_ms)

            if df is not None and not df.empty:
                all_data[symbol] = df
                print(f"[DEBUG] {symbol}: {len(df)} rows loaded for report")
            else:
                print(f"[DEBUG] {symbol}: no data after append")

        except Exception as e:
            print(f"[DEBUG] Error updating {symbol}: {e}")

        time.sleep(delay)  # nghỉ giữa các symbol

    print(f"[DEBUG] Fetched and updated data for {len(all_data)} symbols.")
    return all_data


#  gọi all dâta
def fetch_all_data(delay=0.2, retries=1):
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
                break
            except Exception as e:
                print(f"⚠️ Error fetching {symbol} (attempt {attempt+1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(3)
                else:
                    print(f"❌ Skipping {symbol} sau {retries} lần thất bại")

        time.sleep(delay)

    return all_data


# # ghi vào excel
# def save_data_to_excel(all_data):
#     # Tạo thư mục datafiles nếu chưa có
#     if not os.path.exists('datafiles'):
#         os.makedirs('datafiles')
    
#     # Lưu dữ liệu vào từng file Excel riêng biệt cho mỗi đồng coin
#     for symbol, data in all_data.items():
#         # Đặt tên file theo tên đồng coin, ví dụ: BTCUSDT.xlsx
#         file_path = f'datafiles/{symbol}_data.xlsx'
#         data.to_excel(file_path, index=False)


#  For debug
def save_data_to_excel(all_data):
    # Tạo thư mục datafiles nếu chưa có
    if not os.path.exists('datafiles'):
        os.makedirs('datafiles')
        print("[DEBUG] Created folder: datafiles")

    # Lưu dữ liệu vào từng file Excel riêng biệt cho mỗi đồng coin
    for symbol, data in all_data.items():
        file_path = f'datafiles/{symbol}_data.xlsx'
        print(f"[DEBUG] Saving {symbol} to {os.path.abspath(file_path)} with {len(data)} rows")
        abs_path = os.path.abspath(file_path)
        try:
            data.to_excel(file_path, index=False)
            print(f"[DEBUG] Saved {symbol} successfully")
            print(f"[DEBUG] ✅ Saved {symbol} -> {abs_path} ({len(data)} rows)")
        except Exception as e:
            print(f"[ERROR] ❌ Failed to save {symbol} -> {abs_path}: {e}")


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
                        continue
                    
                    # Định dạng timestamp
                    if isinstance(last_timestamp, pd.Timestamp):
                        formatted_timestamp = last_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        formatted_timestamp = str(last_timestamp)  # fallback
                    
                    # Điều kiện log
                    if last_delta_change > 0 and last_volume >= 4.7 * avg_volume_20:
                        line = f"{symbol} - Delta Change: {last_delta_change:.4f} at {formatted_timestamp}\n"
                        file.write(line)
                        print(f"[INFO] {symbol} thỏa điều kiện -> ghi vào report.")
                
                except Exception as e:
                    print(f"❌ Error processing {symbol}: {e}")
                
        print("✅ Report generated and saved to report.txt.")
    except Exception as e:
        print(f"❌ Error generating report: {e}")


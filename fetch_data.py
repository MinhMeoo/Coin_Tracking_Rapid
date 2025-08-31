import requests
import pandas as pd
import os
from datetime import datetime
from config import SYMBOLS_LIST

import os
import pandas as pd
import requests

import pandas as pd
import requests

def fetch_15m_futures(symbol, limit=30):
    """Lấy dữ liệu nến 15 phút từ Binance."""
    url = 'https://fapi.binance.com/fapi/v1/klines'
    params = {
        'symbol': symbol,
        'interval': '15m',
        'limit': limit
    }
    
    response = requests.get(url, params=params)
    data = response.json()

    # Chuyển đổi dữ liệu thành DataFrame
    df = pd.DataFrame(data, columns=[
        "timestamp", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    
    # Chuyển đổi timestamp và các cột số liệu về dạng numeric
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['open'] = pd.to_numeric(df['open'])
    df['close'] = pd.to_numeric(df['close'])
    df['high'] = pd.to_numeric(df['high'])
    df['low'] = pd.to_numeric(df['low'])
    df['volume'] = pd.to_numeric(df['volume'])

    # Tính delta_change
    df['delta_change'] = df.apply(lambda row: calculate_delta_change(row['open'], row['close']), axis=1)

    # Tính average_volume_20
    df['average_volume_20'] = calculate_average_volume(df)

    # Giới hạn chỉ giữ lại các cột cần thiết
    df = df[['timestamp', 'open', 'close', 'volume', 'delta_change', 'average_volume_20']]

    return df



# Hàm lấy dữ liệu cho tất cả các đồng coin trong SYMBOLS_LIST
def fetch_all_data():
    all_data = {}
    for symbol in SYMBOLS_LIST:
        data = fetch_15m_futures(symbol)
        all_data[symbol] = data
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


def generate_report(all_data):
    """Tạo báo cáo và ghi vào file report.txt."""
    try:
        if not all_data:
            print("No data to generate report.")
            return
        
        with open("report.txt", "w") as file:
            for symbol, data in all_data.items():
                try:
                    last_row = data.iloc[-1]  # Lấy dòng dữ liệu mới nhất
                    last_delta_change = last_row['delta_change']
                    last_volume = last_row['volume']
                    avg_volume_20 = last_row['average_volume_20']
                    last_timestamp = last_row['timestamp']
                    
                    # Định dạng lại timestamp để có dạng đầy đủ: YYYY-MM-DD HH:mm:ss
                    formatted_timestamp = last_timestamp.strftime('%Y-%m-%d %H:%M:%S')

                    # Điều kiện: delta_change > 0 và volume >= 3 lần trung bình 20 nến
                    if last_delta_change > 0 and last_volume >= 4 * avg_volume_20:
                        # Ghi vào file report
                        file.write(f"{symbol} - Delta Change: {last_delta_change:.4f} at {formatted_timestamp}\n")
                except Exception as e:
                    print(f"Error processing {symbol}: {e}")
                
        print("Report generated and saved to report.txt.")
    except Exception as e:
        print(f"Error generating report: {e}")



def fetch_and_update_data():
    all_data = {}
    for symbol in SYMBOLS_LIST:
        print(f"Fetching data for {symbol}...")  # Log trước khi fetch
        new_data = fetch_15m_futures(symbol)
        
        if new_data is None:
            print(f"Data fetch failed for {symbol}. Skipping.")
            continue
        
        # Lưu dữ liệu vào Excel hoặc file tương ứng
        save_data_to_excel(symbol, new_data)
        all_data[symbol] = new_data
        
    print(f"Fetched and updated data for {len(all_data)} symbols.")
    return all_data



from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
from vnstock import Vnstock
import json
import pandas as pd
import numpy as np
import time
import requests
import socket
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Class giúp xử lý numpy và pandas datatype trong json
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, pd.Timestamp):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        return super(NpEncoder, self).default(obj)

def fetch_and_push_to_kafka():
    # Thiết lập Socket timeout dài hơn để cho phép DNS resolution
    socket.setdefaulttimeout(60)  # 60 giây timeout
    
    # Thiết lập session với retry
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    # Thử ping Google DNS để kiểm tra kết nối internet
    try:
        session.get("https://8.8.8.8", timeout=5)
        print("✅ Kết nối internet OK")
    except Exception as e:
        print(f"⚠️ Kiểm tra kết nối internet thất bại: {e}")
    
    # Thêm retry và timeout dài hơn
    max_retries = 3
    retry_delay = 30  # seconds
    
    # Danh sách các mã cổ phiếu phổ biến (VN30 và một số blue-chip khác)
    popular_symbols = [
        # VN30
        "VCB", "VIC", "VHM", "HPG", "BID", "VNM", "FPT", "MSN", "MWG", "TCB",
        "CTG", "VPB", "NVL", "VRE", "GAS", "SAB", "PLX", "STB", "POW", "HDB",
        "VJC", "TPB", "ACB", "PDR", "BCM", "BVH", "GVR", "SSI", "KDH", "LPB",
        # Thêm một số blue-chip khác
        "DHG", "VGC", "VSH", "REE", "HAG", "DXG", "KBC", "VGT", "HBC", "GMD",
        # Thêm các mã theo yêu cầu
        "FRT", "NKG", "NLG", "PNJ", "PVD", "PVS", "VHC"
    ]
    
    try:
        vnstock_instance = Vnstock()
        stock = vnstock_instance.stock(symbol='VN30', source='VCI')
        
        # Sử dụng các mã phổ biến đã định nghĩa thay vì lấy tất cả
        symbols = popular_symbols
        
        # Lấy giá hiện tại cho các mã phổ biến
        price_board = stock.trading.price_board(symbols_list=symbols)
        
    except Exception as e:
        print(f"❌ Lỗi khi kết nối đến API: {str(e)}")
        # Sử dụng danh sách mã phổ biến nếu không lấy được giá
        symbols = popular_symbols
        # Trả về nếu không tiếp tục xử lý được
        if not hasattr(locals(), 'price_board'):
            print("❌ Không thể lấy được bảng giá, dừng xử lý")
            return

    if isinstance(price_board.columns, pd.MultiIndex):
        price_board.columns = ['_'.join(map(str, col)).strip() for col in price_board.columns.values]

    # Tạo dictionary để lưu trữ thông tin từ price_board theo symbol
    price_board_dict = {}
    for _, row in price_board.iterrows():
        symbol = row.get('listing_symbol')
        if symbol:
            price_board_dict[symbol] = {
                'current_price': row.get('match_match_price', 0),
                'ceiling': row.get('listing_ceiling', 0),
                'floor': row.get('listing_floor', 0),
                'vol': row.get('match_match_vol', 0)
            }

    snapshot = {'time': datetime.now().isoformat()}
    for symbol in symbols:
        try:
            row = price_board[price_board['listing_symbol'] == symbol]
            snapshot[symbol] = row['match_match_price'].values[0] if not row.empty else None
        except Exception:
            snapshot[symbol] = None

    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v, cls=NpEncoder).encode('utf-8'),
        batch_size=16384,  # Tăng kích thước batch
        buffer_memory=67108864  # Tăng bộ đệm lên 64MB
    )

    # Gửi dữ liệu giá hiện tại vào Kafka
    producer.send('stock-topic', snapshot)
    print("✅ Đã đẩy dữ liệu giá hiện tại vào Kafka topic `stock-topic`")
    
    # Lấy và gửi dữ liệu lịch sử vào Kafka
    # Sử dụng các mã phổ biến đã định nghĩa
    sample_symbols = symbols
    
    # Chia thành các batch nhỏ hơn để xử lý song song
    batch_size = 3  # Xử lý 3 mã mỗi batch
    symbol_batches = [sample_symbols[i:i+batch_size] for i in range(0, len(sample_symbols), batch_size)]
    
    print(f"Sẽ xử lý {len(sample_symbols)} mã cổ phiếu trong {len(symbol_batches)} batch")
    
    for batch_idx, symbol_batch in enumerate(symbol_batches):
        print(f"Xử lý batch {batch_idx+1}/{len(symbol_batches)} với {len(symbol_batch)} mã...")
        
        for symbol in symbol_batch:
            try:
                print(f"🔄 Đang lấy dữ liệu lịch sử cho {symbol}...")
                
                # Thêm retry cho API history
                for history_attempt in range(max_retries):
                    try:
                        # Lấy dữ liệu lịch sử từ 2020 đến hiện tại
                        historical_data = stock.quote.history(
                            symbol=symbol,
                            start='2020-01-01', 
                            end=datetime.now().strftime('%Y-%m-%d'),
                            interval='1D'
                        )
                        break  # Nếu thành công, thoát khỏi vòng lặp retry
                    except Exception as e:
                        print(f"❌ Lỗi khi lấy lịch sử {symbol} (lần {history_attempt+1}/{max_retries}): {str(e)}")
                        if history_attempt < max_retries - 1:
                            print(f"⏱ Đợi {retry_delay} giây trước khi thử lại...")
                            time.sleep(retry_delay)
                        else:
                            print(f"❌ Đã hết số lần thử lại. Bỏ qua mã {symbol}.")
                            raise  # Re-raise để xử lý ở except bên ngoài
                
                # Lấy giá hiện tại từ snapshot
                current_price = snapshot.get(symbol)
                
                # Lấy thông tin trần, sàn, khối lượng từ price_board_dict
                symbol_info = price_board_dict.get(symbol, {})
                ceiling = symbol_info.get('ceiling', 0)
                floor = symbol_info.get('floor', 0)  
                vol = symbol_info.get('vol', 0)
                
                print(f"✅ Thông tin cho {symbol}: Giá hiện tại={current_price}, Trần={ceiling}, Sàn={floor}, Khối lượng={vol}")
                
                # Chuyển thành dictionary để serialize - xử lý đúng kiểu dữ liệu
                if not historical_data.empty:
                    # Convert DataFrame to dict with Python native types 
                    records = []
                    for _, row in historical_data.iterrows():
                        record = {}
                        for col, val in row.items():
                            if isinstance(val, (np.integer, np.int64, np.int32)): 
                                record[col] = int(val)
                            elif isinstance(val, (np.floating, np.float64, np.float32)):
                                record[col] = float(val)
                            elif isinstance(val, pd.Timestamp):
                                record[col] = val.strftime('%Y-%m-%d')
                            else:
                                record[col] = val
                        records.append(record)
                    
                    # Thêm thông tin mới vào historical_dict
                    historical_dict = {
                        'symbol': symbol,
                        'current_price': current_price,
                        'ceiling': ceiling,
                        'floor': floor,
                        'vol': vol,
                        'historical_data': records
                    }
                    
                    # Gửi vào Kafka
                    producer.send('stock-history-topic', historical_dict)
                    print(f"✅ Đã đẩy dữ liệu lịch sử cho {symbol} vào Kafka topic `stock-history-topic`")
                    
                    # Thêm delay để tránh giới hạn API
                    time.sleep(5)
                
            except Exception as e:
                print(f"❌ Lỗi khi lấy dữ liệu lịch sử cho {symbol}: {str(e)}")
                # Nếu gặp lỗi giới hạn API, chờ thêm để tránh bị block
                if "quá nhiều request" in str(e).lower():
                    print("⏱ Đang chờ 30 giây để tránh giới hạn API...")
                    time.sleep(30)
        
        # Sau mỗi batch, chờ một chút để đảm bảo API không bị quá tải
        if batch_idx < len(symbol_batches) - 1:
            print(f"⏱ Đã xử lý xong batch {batch_idx+1}, đang chờ 20 giây trước khi xử lý batch tiếp theo...")
            time.sleep(20)  # Tăng thời gian chờ từ 15 lên 20 giây
        
        # Nếu đã xử lý quá 100 mã, dừng lại
        if (batch_idx + 1) * batch_size >= 100:
            print(f"🛑 Đã xử lý đủ 100 mã cổ phiếu, dừng xử lý.")
            break
    
    producer.flush()
    print("✅ Hoàn thành việc đẩy dữ liệu vào Kafka")

default_args = {
    'owner': 'airflow',
    'retries': 3,  # Tăng số lần retry của task
    'retry_delay': timedelta(minutes=5),  # Tăng thời gian giữa các lần retry
    'start_date': datetime(2023, 5, 1),
    'depends_on_past': False,  # Thêm depends_on_past = False
    'execution_timeout': timedelta(minutes=30),  # Thêm timeout cho task
}

with DAG(
    dag_id='fetch_stock_to_kafka',
    default_args=default_args,
    schedule_interval='@once',  # Thay đổi thành @once để chạy một lần khi Airflow khởi động
    catchup=False,
    max_active_runs=1,  # Thêm để chỉ cho phép 1 instance của DAG chạy cùng lúc
    tags=['stock', 'vnstock', 'kafka'],
) as dag:
    task = PythonOperator(
        task_id='fetch_push_kafka',
        python_callable=fetch_and_push_to_kafka,
        execution_timeout=timedelta(minutes=25),  # Thêm timeout riêng cho task này
    )
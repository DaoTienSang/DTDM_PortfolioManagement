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
    socket.setdefaulttimeout(60)  

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
    

    try:
        session.get("https://8.8.8.8", timeout=5)
        print(" Kết nối internet OK")
    except Exception as e:
        print(f"Kiểm tra kết nối internet thất bại: {e}")
    

    max_retries = 3
    retry_delay = 30  
    

    popular_symbols = [
        "VCB", "VIC", "VHM", "HPG", "BID", "VNM", "FPT", "MSN", "MWG", "TCB",
        "CTG", "VPB", "NVL", "VRE", "GAS", "SAB", "PLX", "STB", "POW", "HDB",
        "VJC", "TPB", "ACB", "PDR", "BCM", "BVH", "GVR", "SSI", "KDH", "LPB",
        "DHG", "VGC", "VSH", "REE", "HAG", "DXG", "KBC", "VGT", "HBC", "GMD",
        "FRT", "NKG", "NLG", "PNJ", "PVD", "PVS", "VHC"
    ]
    
    try:
        vnstock_instance = Vnstock()
        stock = vnstock_instance.stock(symbol='VN30', source='VCI')
        
        symbols = popular_symbols

        price_board = stock.trading.price_board(symbols_list=symbols)
        
    except Exception as e:
        print(f"Lỗi khi kết nối đến API: {str(e)}")
        symbols = popular_symbols

        if not hasattr(locals(), 'price_board'):
            print(" Không thể lấy được bảng giá, dừng xử lý")
            return

    if isinstance(price_board.columns, pd.MultiIndex):
        price_board.columns = ['_'.join(map(str, col)).strip() for col in price_board.columns.values]

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
        batch_size=16384,  
        buffer_memory=67108864  
    )

    producer.send('stock-topic', snapshot)
    print("Đã đẩy dữ liệu giá hiện tại vào Kafka topic `stock-topic`")
    

    sample_symbols = symbols

    batch_size = 3  
    symbol_batches = [sample_symbols[i:i+batch_size] for i in range(0, len(sample_symbols), batch_size)]
    
    print(f"Sẽ xử lý {len(sample_symbols)} mã cổ phiếu trong {len(symbol_batches)} batch")
    
    for batch_idx, symbol_batch in enumerate(symbol_batches):
        print(f"Xử lý batch {batch_idx+1}/{len(symbol_batches)} với {len(symbol_batch)} mã...")
        
        for symbol in symbol_batch:
            try:
                print(f"Đang lấy dữ liệu lịch sử cho {symbol}...")
                
                for history_attempt in range(max_retries):
                    try:

                        historical_data = stock.quote.history(
                            symbol=symbol,
                            start='2020-01-01', 
                            end=datetime.now().strftime('%Y-%m-%d'),
                            interval='1D'
                        )
                        break  
                    except Exception as e:
                        print(f"Lỗi khi lấy lịch sử {symbol} (lần {history_attempt+1}/{max_retries}): {str(e)}")
                        if history_attempt < max_retries - 1:
                            print(f"⏱ Đợi {retry_delay} giây trước khi thử lại...")
                            time.sleep(retry_delay)
                        else:
                            print(f"Đã hết số lần thử lại. Bỏ qua mã {symbol}.")
                            raise 
                
       
                current_price = snapshot.get(symbol)
                
        
                symbol_info = price_board_dict.get(symbol, {})
                ceiling = symbol_info.get('ceiling', 0)
                floor = symbol_info.get('floor', 0)  
                vol = symbol_info.get('vol', 0)
                
                print(f"Thông tin cho {symbol}: Giá hiện tại={current_price}, Trần={ceiling}, Sàn={floor}, Khối lượng={vol}")
                
               
                if not historical_data.empty:
                
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
                    
                    
                    historical_dict = {
                        'symbol': symbol,
                        'current_price': current_price,
                        'ceiling': ceiling,
                        'floor': floor,
                        'vol': vol,
                        'historical_data': records
                    }
                    
                    
                    producer.send('stock-history-topic', historical_dict)
                    print(f"Đã đẩy dữ liệu lịch sử cho {symbol} vào Kafka topic `stock-history-topic`")
                    
                    
                    time.sleep(5)
                
            except Exception as e:
                print(f" Lỗi khi lấy dữ liệu lịch sử cho {symbol}: {str(e)}")
                
                if "quá nhiều request" in str(e).lower():
                    print("⏱ Đang chờ 30 giây để tránh giới hạn API...")
                    time.sleep(30)
        
  
        if batch_idx < len(symbol_batches) - 1:
            print(f"⏱ Đã xử lý xong batch {batch_idx+1}, đang chờ 20 giây trước khi xử lý batch tiếp theo...")
            time.sleep(20)  
        
        
        if (batch_idx + 1) * batch_size >= 100:
            print(f"Đã xử lý đủ 100 mã cổ phiếu, dừng xử lý.")
            break
    
    producer.flush()
    print("Hoàn thành việc đẩy dữ liệu vào Kafka")

default_args = {
    'owner': 'airflow',
    'retries': 3,  
    'retry_delay': timedelta(minutes=5),  
    'start_date': datetime(2023, 5, 1),
    'depends_on_past': False,  
    'execution_timeout': timedelta(minutes=30),  
}

with DAG(
    dag_id='fetch_stock_to_kafka',
    default_args=default_args,
    schedule_interval='@once',  
    catchup=False,
    max_active_runs=1,  
    tags=['stock', 'vnstock', 'kafka'],
) as dag:
    task = PythonOperator(
        task_id='fetch_push_kafka',
        python_callable=fetch_and_push_to_kafka,
        execution_timeout=timedelta(minutes=25),  
    )
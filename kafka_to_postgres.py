#!/usr/bin/env python3
import json
import os
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Thông tin kết nối Kafka
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = 'stock-history-topic'  # Topic chứa dữ liệu cổ phiếu

# Thông tin kết nối PostgreSQL
PG_HOST = os.environ.get('DB_HOST', 'db')
PG_PORT = os.environ.get('DB_PORT', '5432')
PG_DB = os.environ.get('DB_NAME', 'portfolio')
PG_USER = os.environ.get('DB_USER', 'postgres')
PG_PASSWORD = os.environ.get('DB_PASSWORD', 'postgres')

def create_table_if_not_exists(conn):
    """Tạo bảng stock_prices nếu chưa tồn tại"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                symbol VARCHAR(20) NOT NULL,
                price DOUBLE PRECISION,
                timestamp TIMESTAMP,
                PRIMARY KEY (symbol)
            )
            """)
        conn.commit()
        logger.info("✅ Bảng stock_prices đã được tạo hoặc đã tồn tại")
    except Exception as e:
        logger.error(f"❌ Lỗi khi tạo bảng: {e}")
        conn.rollback()

def connect_to_postgres():
    """Kết nối đến PostgreSQL và trả về connection"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
        logger.info("✅ Đã kết nối đến PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"❌ Lỗi khi kết nối đến PostgreSQL: {e}")
        raise

def connect_to_kafka():
    """Kết nối đến Kafka và trả về consumer"""
    try:
        # Thử kết nối đến Kafka với backoff
        for attempt in range(5):
            try:
                consumer = KafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=KAFKA_BROKER,
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    group_id='stock-to-postgres',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                logger.info(f"✅ Đã kết nối đến Kafka broker: {KAFKA_BROKER}")
                return consumer
            except Exception as e:
                if attempt < 4:
                    wait_time = (2 ** attempt)
                    logger.warning(f"Không thể kết nối đến Kafka. Thử lại sau {wait_time}s... ({attempt+1}/5)")
                    time.sleep(wait_time)
                else:
                    raise
    except Exception as e:
        logger.error(f"❌ Lỗi khi kết nối đến Kafka: {e}")
        raise

def upsert_stock_prices(conn, records):
    """Cập nhật hoặc thêm mới giá cổ phiếu vào PostgreSQL"""
    if not records:
        return
    
    try:
        with conn.cursor() as cur:
            # Sử dụng execute_values để thực hiện upsert nhiều records cùng lúc
            insert_query = """
            INSERT INTO stock_prices (symbol, price, timestamp)
            VALUES %s
            ON CONFLICT (symbol) 
            DO UPDATE SET
                price = EXCLUDED.price,
                timestamp = EXCLUDED.timestamp
            """
            
            # Chuẩn bị dữ liệu theo định dạng mà execute_values mong đợi
            values = [(r['symbol'], r['price'], r['timestamp']) for r in records]
            
            execute_values(cur, insert_query, values)
            
        conn.commit()
        logger.info(f"✅ Đã lưu {len(records)} giá cổ phiếu vào PostgreSQL")
    except Exception as e:
        logger.error(f"❌ Lỗi khi lưu dữ liệu vào PostgreSQL: {e}")
        conn.rollback()

def process_messages():
    """Xử lý tin nhắn từ Kafka và lưu vào PostgreSQL"""
    try:
        # Kết nối đến PostgreSQL
        pg_conn = connect_to_postgres()
        
        # Tạo bảng nếu chưa tồn tại
        create_table_if_not_exists(pg_conn)
        
        # Kết nối đến Kafka
        consumer = connect_to_kafka()
        
        # Đếm số lượng messages đã xử lý
        message_count = 0
        batch_size = 50
        batch_records = []
        last_commit_time = time.time()
        
        logger.info(f"🔄 Bắt đầu lắng nghe messages từ topic {KAFKA_TOPIC}...")
        
        # Lắng nghe và xử lý messages
        for message in consumer:
            try:
                data = message.value
                
                # Lấy thông tin cổ phiếu từ message
                symbol = data.get('symbol')
                current_price = data.get('current_price')
                
                # Bỏ qua nếu không có symbol hoặc giá
                if not symbol or current_price is None:
                    continue
                
                # Chuẩn bị record để lưu vào PostgreSQL
                record = {
                    'symbol': symbol,
                    'price': current_price,
                    'timestamp': datetime.now().isoformat()
                }
                
                batch_records.append(record)
                message_count += 1
                
                # Kiểm tra nếu đủ batch_size hoặc đã quá thời gian để commit
                current_time = time.time()
                if len(batch_records) >= batch_size or (current_time - last_commit_time) > 60:
                    upsert_stock_prices(pg_conn, batch_records)
                    batch_records = []
                    last_commit_time = current_time
                    
                # Log thống kê sau mỗi 100 messages
                if message_count % 100 == 0:
                    logger.info(f"📊 Đã xử lý {message_count} messages")
                    
            except Exception as e:
                logger.error(f"❌ Lỗi khi xử lý message: {e}")
                
    except KeyboardInterrupt:
        logger.info("🛑 Nhận tín hiệu dừng ứng dụng...")
    except Exception as e:
        logger.error(f"❌ Lỗi trong quá trình xử lý: {e}")
    finally:
        # Đảm bảo commit các records còn lại nếu có
        if pg_conn and batch_records:
            upsert_stock_prices(pg_conn, batch_records)
            
        # Đóng kết nối
        if pg_conn:
            pg_conn.close()
            logger.info("✅ Đã đóng kết nối PostgreSQL")
            
        logger.info("✅ Ứng dụng kết thúc")

if __name__ == "__main__":
    # Chờ một chút để các services khác khởi động
    logger.info("⏳ Chờ 30 giây để các services khác khởi động...")
    time.sleep(30)
    
    # Bắt đầu xử lý
    process_messages() 
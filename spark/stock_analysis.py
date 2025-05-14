from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, when, lit, expr, collect_list, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, TimestampType
import pandas as pd
import numpy as np
from datetime import datetime
import time
import psycopg2
import json
import traceback

# Khởi tạo SparkSession với timeout và tham số để tăng tính ổn định
spark = SparkSession.builder \
    .appName("Stock Analysis") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "120s") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

# Log level
spark.sparkContext.setLogLevel("INFO")

print("Spark session created successfully")

# PostgreSQL connection parameters
pg_params = {
    'host': 'db',
    'database': 'db_for_pm',
    'user': 'airflow',
    'password': 'admin123'
}

# Kiểm tra kết nối PostgreSQL khi khởi động
try:
    print(f"🔍🔍🔍 ĐANG KIỂM TRA KẾT NỐI ĐẾN POSTGRESQL 🔍🔍🔍")
    print(f"Thông số kết nối: {pg_params}")
    test_conn = psycopg2.connect(**pg_params)
    print("✅✅✅ KẾT NỐI THỬ NGHIỆM TỚI POSTGRESQL THÀNH CÔNG! ✅✅✅")
    test_cursor = test_conn.cursor()
    test_cursor.execute("SELECT version();")
    version = test_cursor.fetchone()
    print(f"PostgreSQL version: {version[0]}")
    
    # Tạo bảng nếu chưa tồn tại
    test_cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_processed_data (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(10) NOT NULL,
        current_price FLOAT,
        ceiling FLOAT,
        floor FLOAT,
        vol FLOAT,
        json_data JSONB,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    test_conn.commit()
    print("✅✅✅ ĐÃ KIỂM TRA/TẠO BẢNG stock_processed_data THÀNH CÔNG ✅✅✅")
    
    # Kiểm tra xem có dữ liệu trong bảng không
    test_cursor.execute("SELECT COUNT(*) FROM stock_processed_data")
    count = test_cursor.fetchone()[0]
    print(f"Số lượng bản ghi hiện có trong bảng: {count}")
    
    test_cursor.close()
    test_conn.close()
except Exception as e:
    print(f"❌❌❌ LỖI KẾT NỐI POSTGRESQL: {e}")
    print(f"Chi tiết lỗi: {traceback.format_exc()}")
    print("⚠️ VUI LÒNG KIỂM TRA LẠI THÔNG SỐ KẾT NỐI POSTGRESQL!")

# Utility function to save data to PostgreSQL
def save_to_postgres(symbol, data):
    """
    Lưu dữ liệu vào PostgreSQL
    - symbol: Mã cổ phiếu
    - data: JSON data đã xử lý  
    """
    conn = None
    try:
        print(f"ĐANG LƯU SYMBOL: {symbol} VÀO POSTGRESQL")
        print(f"Thông số kết nối: {pg_params}")
        conn = psycopg2.connect(**pg_params)
        cursor = conn.cursor()
        
        # Tạo bảng nếu chưa tồn tại
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_processed_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            current_price FLOAT,
            ceiling FLOAT,
            floor FLOAT,
            vol FLOAT,
            json_data JSONB,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()
        
        # Tạo lớp JSONEncoder tùy chỉnh để xử lý NaN
        class CustomJSONEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, float) and np.isnan(obj):
                    return None
                return super().default(obj)
        
        # Chuyển data thành JSON string với encoder tùy chỉnh
        json_data = json.dumps(data, cls=CustomJSONEncoder)
        print(f"Dữ liệu đã chuyển thành JSON: {json_data[:100]}...")
        
        # Kiểm tra xem bản ghi đã tồn tại chưa
        cursor.execute("SELECT id FROM stock_processed_data WHERE symbol = %s", (symbol,))
        result = cursor.fetchone()
        
        if result:
            # Cập nhật bản ghi
            cursor.execute("""
            UPDATE stock_processed_data 
            SET current_price = %s, ceiling = %s, floor = %s, vol = %s, 
                json_data = %s::jsonb, updated_at = CURRENT_TIMESTAMP
            WHERE symbol = %s
            """, (
                data.get('current_price_new', 0), 
                data.get('ceiling_new', 0), 
                data.get('floor_new', 0), 
                data.get('vol_new', 0),
                json_data,
                symbol
            ))
            print(f"Đã cập nhật dữ liệu của {symbol} trong PostgreSQL thành công")
        else:
            # Thêm bản ghi mới
            cursor.execute("""
            INSERT INTO stock_processed_data (symbol, current_price, ceiling, floor, vol, json_data)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            """, (
                symbol,
                data.get('current_price_new', 0), 
                data.get('ceiling_new', 0), 
                data.get('floor_new', 0), 
                data.get('vol_new', 0),
                json_data
            ))
            print(f"Đã thêm dữ liệu của {symbol} vào PostgreSQL thành công")
        
        conn.commit()
    except Exception as e:
        print(f"Lỗi khi lưu dữ liệu vào PostgreSQL: {str(e)}")
        print(f"\nChi tiết lỗi: {traceback.format_exc()}\n")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

# Định nghĩa schema cho dữ liệu JSON từ Kafka
schema = StructType([
    StructField("symbol", StringType()),
    StructField("current_price", DoubleType()),
    StructField("ceiling", DoubleType()),
    StructField("floor", DoubleType()),
    StructField("vol", DoubleType()),
    StructField("historical_data", ArrayType(
        StructType([
            StructField("time", StringType()),
            StructField("open", DoubleType()),
            StructField("high", DoubleType()),
            StructField("low", DoubleType()),
            StructField("close", DoubleType()),
            StructField("volume", DoubleType())
        ])
    ))
])

# Dùng địa chỉ network của container Kafka trong Docker
kafka_bootstrap_servers = "kafka:9092"

# Đọc dữ liệu từ Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "stock-history-topic") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 10000) \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON từ Kafka
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

# Explode array để xử lý từng record lịch sử
exploded_df = parsed_df \
    .select(
        col("symbol"),
        col("current_price"),
        col("ceiling"),
        col("floor"),
        col("vol"),
        expr("explode(historical_data) as history")
    ) \
    .select(
        col("symbol"),
        col("current_price"),
        col("ceiling"),
        col("floor"),
        col("vol"),
        col("history.time").alias("time"),
        col("history.open").alias("open"),
        col("history.high").alias("high"),
        col("history.low").alias("low"),
        col("history.close").alias("close"),
        col("history.volume").alias("volume")
    )

# Chuyển timestamp string thành timestamp thực và chuyển thành string date
# Để tránh lỗi datetime64 khi chuyển sang pandas
exploded_df = exploded_df \
    .withColumn("timestamp", to_timestamp(col("time"), "yyyy-MM-dd")) \
    .withColumn("date_str", expr("date_format(timestamp, 'yyyy-MM-dd')"))

# Biến toàn cục để lưu trữ SparkContext state
spark_active = True

# Xử lý dữ liệu theo batch thay vì sử dụng window functions
def process_batch(batch_df, batch_id):
    global spark_active
    
    try:
        # Kiểm tra SparkContext còn hoạt động không
        if not spark_active or spark.sparkContext._jsc.sc().isStopped():
            print(f"SparkContext đã bị shutdown, không thể xử lý batch {batch_id}")
            return
            
        # Kiểm tra batch có dữ liệu không
        try:
            if batch_df.isEmpty():
                print(f"Batch {batch_id} không có dữ liệu, bỏ qua")
                return
        except Exception as e:
            print(f"Không thể kiểm tra batch rỗng: {e}")
            # Nếu không thể kiểm tra isEmpty, giả định là có dữ liệu và tiếp tục
        
        # Xóa cột timestamp trước khi chuyển sang Pandas để tránh lỗi
        # Và sử dụng date_str thay thế
        batch_df_no_ts = batch_df.drop("timestamp")
        
        # Lấy danh sách các symbol riêng biệt trong batch này để xử lý
        symbols_in_batch = batch_df_no_ts.select("symbol").distinct().rdd.flatMap(lambda x: x).collect()
        print(f"🔍 Batch {batch_id} có {len(symbols_in_batch)} mã cổ phiếu: {symbols_in_batch}")
        
        # Giới hạn số lượng mã cổ phiếu trong mỗi batch để tránh quá tải
        if len(symbols_in_batch) > 20:
            print(f"Batch {batch_id} có quá nhiều mã ({len(symbols_in_batch)}), chỉ xử lý 20 mã đầu tiên")
            symbols_in_batch = symbols_in_batch[:20]
        
        # Nếu batch này không có dữ liệu nhiều, đợi thêm thời gian để tích lũy dữ liệu
        if len(symbols_in_batch) < 5:  # Giảm ngưỡng từ 10 xuống 5
            print(f"Batch {batch_id} chỉ có {len(symbols_in_batch)} mã, đợi thêm dữ liệu...")
            if batch_id < 3:  # Chỉ đợi thêm trong các batch đầu tiên
                time.sleep(30)  # Giảm thời gian đợi từ 60s xuống 30s
        
        # Giới hạn số lượng dữ liệu nếu quá lớn để tránh quá tải
        count = batch_df_no_ts.count()
        print(f"Batch {batch_id} có {count} records")
        
        if count > 10000:  # Giảm ngưỡng từ 20000 xuống 10000
            print(f"Batch {batch_id} quá lớn ({count} records), phân chia thành các batch nhỏ hơn")
            # Phân chia thành nhiều batch nhỏ hơn
            batches = batch_df_no_ts.randomSplit([0.25, 0.25, 0.25, 0.25])  # Giảm số lượng batches
            for i, small_batch in enumerate(batches):
                try:
                    process_small_batch(small_batch, f"{batch_id}_{i}")
                except Exception as e:
                    print(f"Lỗi khi xử lý sub-batch {batch_id}_{i}: {e}")
        else:
            # Xử lý batch nhỏ
            process_small_batch(batch_df_no_ts, batch_id)
    
    except Exception as e:
        print(f"Lỗi trong process_batch {batch_id}: {e}")
        print(traceback.format_exc())

def process_small_batch(batch_df, batch_id):
    """Xử lý một batch nhỏ dữ liệu"""
    
    if spark.sparkContext._jsc.sc().isStopped():
        print(f"SparkContext đã bị shutdown, không thể xử lý batch {batch_id}")
        return
        
    try:
        # Chuyển sang Pandas để dễ tính toán các chỉ số kỹ thuật
        batch_pd = batch_df.toPandas()
        
        if batch_pd.empty:
            print(f"Batch {batch_id} không có dữ liệu sau khi chuyển sang pandas")
            return
        
        # Chuyển date_str thành datetime nếu cần
        batch_pd['date'] = pd.to_datetime(batch_pd['date_str'])
        
        # Nhóm theo symbol và sắp xếp theo thời gian
        symbols = batch_pd['symbol'].unique()
        result_dfs = []
        
        for symbol in symbols:
            # Lọc dữ liệu cho mỗi mã cổ phiếu
            symbol_df = batch_pd[batch_pd['symbol'] == symbol].sort_values('date')
            
            # Nếu có đủ dữ liệu thì tính các chỉ số
            if len(symbol_df) > 0:
                # Lưu giữ giá trị ceiling, floor, vol
                ceiling_value = symbol_df['ceiling'].iloc[-1] if 'ceiling' in symbol_df.columns else None
                floor_value = symbol_df['floor'].iloc[-1] if 'floor' in symbol_df.columns else None
                vol_value = symbol_df['vol'].iloc[-1] if 'vol' in symbol_df.columns else None
                
                # Tính MA5, MA20
                symbol_df['ma5'] = symbol_df['close'].rolling(window=5).mean()
                symbol_df['ma20'] = symbol_df['close'].rolling(window=20).mean()
                
                # Tính RSI
                delta = symbol_df['close'].diff()
                gain = delta.where(delta > 0, 0).rolling(window=14).mean()
                loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
                
                # Xử lý tránh chia cho 0
                rs = pd.Series(np.where(loss == 0, 0, gain / loss), index=loss.index)
                symbol_df['rsi'] = 100 - (100 / (1 + rs))
                
                # Tính MACD
                ema12 = symbol_df['close'].ewm(span=12, adjust=False).mean()
                ema26 = symbol_df['close'].ewm(span=26, adjust=False).mean()
                
                symbol_df['macd_line'] = ema12 - ema26
                symbol_df['macd_signal'] = symbol_df['macd_line'].ewm(span=9, adjust=False).mean()
                symbol_df['macd_histogram'] = symbol_df['macd_line'] - symbol_df['macd_signal']
                
                # Đưa ra đề xuất dựa trên các chỉ số
                conditions = [
                    # BUY: Xu hướng tăng (MA5 > MA20), RSI chưa quá mua, MACD tích cực
                    (symbol_df['ma5'] > symbol_df['ma20']) & 
                    (symbol_df['rsi'] < 70) & 
                    (symbol_df['macd_line'] > symbol_df['macd_signal']),
                    
                    # HOLD/SELL: Xu hướng tăng nhưng RSI quá mua
                    (symbol_df['ma5'] > symbol_df['ma20']) & 
                    (symbol_df['rsi'] >= 70),
                    
                    # WATCH/BUY: Xu hướng giảm nhưng RSI quá bán và MACD tích cực
                    (symbol_df['ma5'] < symbol_df['ma20']) & 
                    (symbol_df['rsi'] <= 30) & 
                    (symbol_df['macd_line'] > symbol_df['macd_signal']),
                    
                    # SELL/AVOID: Xu hướng giảm và MACD tiêu cực
                    (symbol_df['ma5'] < symbol_df['ma20']) & 
                    (symbol_df['macd_line'] <= symbol_df['macd_signal'])
                ]
                
                choices = ['BUY', 'HOLD/SELL', 'WATCH/BUY', 'SELL/AVOID']
                symbol_df['suggestion'] = np.select(conditions, choices, default='HOLD')
                
                # Thêm lý do
                reasons = [
                    "Xu hướng tăng (MA5 > MA20). RSI chưa quá mua. MACD tích cực (MACD > Signal). Đề xuất: MUA - Xu hướng tăng, RSI chưa quá mua, MACD tích cực.",
                    "Xu hướng tăng (MA5 > MA20). Quá mua (RSI > 70). Đề xuất: CÂN NHẮC BÁN - Thị trường có dấu hiệu quá mua.",
                    "Xu hướng giảm (MA5 < MA20). Quá bán (RSI < 30). MACD tích cực (MACD > Signal). Đề xuất: THEO DÕI/MUA - Thị trường đang quá bán, có dấu hiệu đảo chiều.",
                    "Xu hướng giảm (MA5 < MA20). MACD tiêu cực (MACD < Signal). Đề xuất: BÁN/TRÁNH - Xu hướng giảm, MACD tiêu cực."
                ]
                symbol_df['reason'] = np.select(conditions, reasons, default="Xu hướng trung tính. Đề xuất: GIỮ - Chờ tín hiệu rõ ràng hơn.")
                
                # Gán giá trị ceiling, floor, vol
                symbol_df['ceiling'] = ceiling_value
                symbol_df['floor'] = floor_value
                symbol_df['vol'] = vol_value
                
                # Thêm vào kết quả
                result_dfs.append(symbol_df)
        
        # Kết hợp tất cả kết quả
        if result_dfs:
            result_df = pd.concat(result_dfs)
            
            # Kiểm tra lại SparkContext trước khi tiếp tục
            if spark.sparkContext._jsc.sc().isStopped():
                print(f"SparkContext đã bị shutdown, không thể tiếp tục xử lý batch {batch_id}")
                global spark_active
                spark_active = False
                return
            
            # Chuyển Pandas DataFrame trở lại Spark DataFrame
            processed_df = spark.createDataFrame(result_df)
            
            # Thay đổi cách xử lý: Gom nhóm dữ liệu theo symbol
            # Tạo một dataframe riêng cho thông tin hiện tại của mỗi mã cổ phiếu
            current_info_df = processed_df.select(
                col("symbol"),
                col("current_price").alias("current_price_new"),
                col("ceiling").alias("ceiling_new"),
                col("floor").alias("floor_new"),
                col("vol").alias("vol_new")
            ).dropDuplicates(["symbol"])
            
            # Nhóm dữ liệu lịch sử theo mã cổ phiếu
            symbols_to_process = processed_df.select("symbol").distinct().collect()
            
            for symbol_row in symbols_to_process:
                symbol = symbol_row[0]
                # Lọc dữ liệu cho symbol hiện tại
                symbol_data = processed_df.filter(col("symbol") == symbol)
                current_info = current_info_df.filter(col("symbol") == symbol).first()
                
                # Chuyển đổi dữ liệu lịch sử thành danh sách các record
                historical_records = symbol_data.select(
                    col("time"),
                    col("open"),
                    col("high"),
                    col("low"),
                    col("close"),
                    col("volume"),
                    col("ma5"),
                    col("ma20"),
                    col("rsi"),
                    col("macd_line"),
                    col("macd_signal"),
                    col("macd_histogram"),
                    col("suggestion"),
                    col("reason")
                ).collect()
                
                # Tạo JSON cho tất cả dữ liệu lịch sử
                historical_data_list = []
                for record in historical_records:
                    # Tạo dictionary và xử lý NaN
                    record_dict = {
                        "time": record["time"],
                        "open": float(record["open"]),
                        "high": float(record["high"]),
                        "low": float(record["low"]),
                        "close": float(record["close"]),
                        "volume": float(record["volume"]),
                        "suggestion": record["suggestion"],
                        "reason": record["reason"]
                    }
                    
                    # Xử lý các trường có thể có giá trị NaN
                    for field in ["ma5", "ma20", "rsi", "macd_line", "macd_signal", "macd_histogram"]:
                        if record[field] is not None and not np.isnan(record[field]):
                            record_dict[field] = float(record[field])
                        else:
                            record_dict[field] = None
                    
                    historical_data_list.append(record_dict)
                
                # Tạo message JSON hoàn chỉnh
                import json
                message = {
                    "symbol": symbol,
                    "current_price_new": float(current_info["current_price_new"]) if current_info["current_price_new"] is not None else 0.0,
                    "ceiling_new": float(current_info["ceiling_new"]) if current_info["ceiling_new"] is not None else 0.0,
                    "floor_new": float(current_info["floor_new"]) if current_info["floor_new"] is not None else 0.0,
                    "vol_new": float(current_info["vol_new"]) if current_info["vol_new"] is not None else 0.0,
                    "historical_processed": historical_data_list
                }
                
                # Hiển thị một mẫu JSON đẹp hơn trong log (giới hạn 2 bản ghi đầu tiên)
                message_sample = message.copy()
                if len(message_sample["historical_processed"]) > 2:
                    message_sample["historical_processed"] = message_sample["historical_processed"][:2]
                    message_sample["historical_processed"].append({"note": "... còn nữa ..."})
                
                print(f"Ví dụ JSON cho {symbol}:")
                print(json.dumps(message_sample, indent=2, ensure_ascii=False))
                
                # Lưu dữ liệu vào PostgreSQL
                try:
                    save_to_postgres(symbol, message)
                    print(f"Đã lưu dữ liệu của {symbol} vào PostgreSQL thành công")
                except Exception as e:
                    print(f"Lỗi khi lưu dữ liệu {symbol} vào PostgreSQL: {e}")
                
                # Tạo DataFrame với một hàng chứa message này
                message_json = json.dumps(message, ensure_ascii=False)
                message_df = spark.createDataFrame([(symbol, message_json)], ["key", "value"])
                
                # Ghi message vào Kafka
                try:
                    message_df.write \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                        .option("topic", "stock-processed-topic") \
                        .save()
                    
                    print(f"Đã xử lý và ghi thành công dữ liệu cho mã {symbol} với {len(historical_data_list)} bản ghi lịch sử")
                except Exception as e:
                    print(f"Lỗi khi ghi dữ liệu vào Kafka cho mã {symbol}: {e}")
            
            # Hiển thị thông tin để debug
            print(f"Đã xử lý tổng cộng {len(symbols_to_process)} mã cổ phiếu từ batch {batch_id}")
                
    except Exception as e:
        print(f"Lỗi trong process_small_batch {batch_id}: {e}")
        print(traceback.format_exc())

# Thiết lập trigger để xử lý theo từng batch với khoảng thời gian
query = exploded_df \
    .writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="1 minute") \
    .outputMode("update") \
    .start()

print(f"Bắt đầu ghi dữ liệu đã xử lý vào Kafka topic: stock-processed-topic qua {kafka_bootstrap_servers}")
print("Đang chờ dữ liệu...")

# Thêm xử lý để đóng Spark gracefully khi cần
import signal

def handle_sigterm(sig, frame):
    """Xử lý tín hiệu SIGTERM để đóng Spark gracefully"""
    print("Nhận tín hiệu để dừng ứng dụng, đóng Spark gracefully...")
    global spark_active
    spark_active = False
    if query is not None and query.isActive:
        query.stop()
    if not spark.sparkContext._jsc.sc().isStopped():
        spark.stop()
    print("Đã dừng Spark thành công")
    
# Đăng ký xử lý tín hiệu
signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

# Chờ query kết thúc
try:
    query.awaitTermination()
except Exception as e:
    print(f"Lỗi trong quá trình streaming: {e}")
    # Thử restart query nếu có lỗi
    if not spark.sparkContext._jsc.sc().isStopped():
        print("Thử restart query...")
        spark_active = True
        query = exploded_df \
            .writeStream \
            .foreachBatch(process_batch) \
            .trigger(processingTime="1 minute") \
            .outputMode("update") \
            .start()
        query.awaitTermination()
finally:
    # Đảm bảo dừng gracefully khi kết thúc
    if not spark.sparkContext._jsc.sc().isStopped():
        spark.stop()
    print("Ứng dụng kết thúc.") 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, TimestampType
import pandas as pd
import numpy as np
import psycopg2
import json

spark = SparkSession.builder \
    .appName("Stock Analysis") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "120s") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()
spark.sparkContext.setLogLevel("INFO")


pg_params = {
    'host': 'db',
    'database': 'db_for_pm',
    'user': 'airflow',
    'password': 'admin123'
}


def init_postgres():
    try:
        conn = psycopg2.connect(**pg_params)
        cursor = conn.cursor()
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
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"PostgreSQL connection error: {e}")
        raise

init_postgres()


def save_to_postgres(symbol, data):
    conn = None
    try:
        conn = psycopg2.connect(**pg_params)
        cursor = conn.cursor()
        json_data = json.dumps(data, default=lambda x: None if isinstance(x, float) and np.isnan(x) else x)
        cursor.execute("SELECT id FROM stock_processed_data WHERE symbol = %s", (symbol,))
        if cursor.fetchone():
            cursor.execute("""
                UPDATE stock_processed_data 
                SET current_price = %s, ceiling = %s, floor = %s, vol = %s, 
                    json_data = %s::jsonb, updated_at = CURRENT_TIMESTAMP
                WHERE symbol = %s
            """, (data.get('current_price_new', 0), data.get('ceiling_new', 0), 
                  data.get('floor_new', 0), data.get('vol_new', 0), json_data, symbol))
        else:
            cursor.execute("""
                INSERT INTO stock_processed_data (symbol, current_price, ceiling, floor, vol, json_data)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            """, (symbol, data.get('current_price_new', 0), data.get('ceiling_new', 0), 
                  data.get('floor_new', 0), data.get('vol_new', 0), json_data))
        conn.commit()
    except Exception as e:
        print(f"Error saving {symbol} to PostgreSQL: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


schema = StructType([
    StructField("symbol", StringType()),
    StructField("current_price", DoubleType()),
    StructField("ceiling", DoubleType()),
    StructField("floor", DoubleType()),
    StructField("vol", DoubleType()),
    StructField("historical_data", ArrayType(StructType([
        StructField("time", StringType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("volume", DoubleType())
    ])))
])


kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock-history-topic") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 10000) \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

print(kafka_df.printSchema())

exploded_df = parsed_df.select(
    col("symbol"), col("current_price"), col("ceiling"), col("floor"), col("vol"),
    expr("explode(historical_data) as history")
).select(
    col("symbol"), col("current_price"), col("ceiling"), col("floor"), col("vol"),
    col("history.time").alias("time"), col("history.open").alias("open"),
    col("history.high").alias("high"), col("history.low").alias("low"),
    col("history.close").alias("close"), col("history.volume").alias("volume")
).withColumn("timestamp", to_timestamp(col("time"), "yyyy-MM-dd")) \
 .withColumn("date_str", expr("date_format(timestamp, 'yyyy-MM-dd')"))

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty, skipping")
        return

    batch_pd = batch_df.drop("timestamp").toPandas()
    batch_pd['date'] = pd.to_datetime(batch_pd['date_str'])
    symbols = batch_pd['symbol'].unique()

    for symbol in symbols:
        symbol_df = batch_pd[batch_pd['symbol'] == symbol].sort_values('date')
        if len(symbol_df) == 0:
            continue


        symbol_df['ma5'] = symbol_df['close'].rolling(window=5).mean()
        symbol_df['ma20'] = symbol_df['close'].rolling(window=20).mean()
        delta = symbol_df['close'].diff()
        gain = delta.where(delta > 0, 0).rolling(window=14).mean()
        loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
        rs = np.where(loss == 0, 0, gain / loss)
        symbol_df['rsi'] = 100 - (100 / (1 + rs))
        
        ema12 = symbol_df['close'].ewm(span=12, adjust=False).mean()
        ema26 = symbol_df['close'].ewm(span=26, adjust=False).mean()
        symbol_df['macd_line'] = ema12 - ema26
        symbol_df['macd_signal'] = symbol_df['macd_line'].ewm(span=9, adjust=False).mean()
        symbol_df['macd_histogram'] = symbol_df['macd_line'] - symbol_df['macd_signal']

        conditions = [
            (symbol_df['ma5'] > symbol_df['ma20']) & (symbol_df['rsi'] < 70) & (symbol_df['macd_line'] > symbol_df['macd_signal']),
            (symbol_df['ma5'] > symbol_df['ma20']) & (symbol_df['rsi'] >= 70),
            (symbol_df['ma5'] < symbol_df['ma20']) & (symbol_df['rsi'] <= 30) & (symbol_df['macd_line'] > symbol_df['macd_signal']),
            (symbol_df['ma5'] < symbol_df['ma20']) & (symbol_df['macd_line'] <= symbol_df['macd_signal'])
        ]
        choices = ['BUY', 'HOLD/SELL', 'WATCH/BUY', 'SELL/AVOID']
        reasons = [
            "MA5 > MA20, RSI < 70, MACD positive: BUY",
            "MA5 > MA20, RSI >= 70: HOLD/SELL",
            "MA5 < MA20, RSI <= 30, MACD positive: WATCH/BUY",
            "MA5 < MA20, MACD negative: SELL/AVOID"
        ]
        symbol_df['suggestion'] = np.select(conditions, choices, default='HOLD')
        symbol_df['reason'] = np.select(conditions, reasons, default="Neutral: HOLD")


        current_info = symbol_df.iloc[-1] 
        
        original_kafka_info = batch_df.filter(col("symbol") == symbol) \
                                 .select("current_price", "ceiling", "floor", "vol") \
                                 .distinct() \
                                 .first()

        historical_data = [
            {
                "time": row['time'], "open": float(row['open']), "high": float(row['high']),
                "low": float(row['low']), "close": float(row['close']), "volume": float(row['volume']),
                "ma5": float(row['ma5']) if not np.isnan(row['ma5']) else None,
                "ma20": float(row['ma20']) if not np.isnan(row['ma20']) else None,
                "rsi": float(row['rsi']) if not np.isnan(row['rsi']) else None,
                "macd_line": float(row['macd_line']) if not np.isnan(row['macd_line']) else None,
                "macd_signal": float(row['macd_signal']) if not np.isnan(row['macd_signal']) else None,
                "macd_histogram": float(row['macd_histogram']) if not np.isnan(row['macd_histogram']) else None,
                "suggestion": row['suggestion'], "reason": row['reason']
            } for row in symbol_df.to_dict('records')
        ]

        message = {
            "symbol": symbol,
            "current_price_new": float(original_kafka_info['current_price']) if original_kafka_info and original_kafka_info['current_price'] is not None and not np.isnan(original_kafka_info['current_price']) else 0.0,
            "ceiling_new": float(original_kafka_info['ceiling']) if original_kafka_info and original_kafka_info['ceiling'] is not None and not np.isnan(original_kafka_info['ceiling']) else 0.0,
            "floor_new": float(original_kafka_info['floor']) if original_kafka_info and original_kafka_info['floor'] is not None and not np.isnan(original_kafka_info['floor']) else 0.0,
            "vol_new": float(original_kafka_info['vol']) if original_kafka_info and original_kafka_info['vol'] is not None and not np.isnan(original_kafka_info['vol']) else 0.0,
            "historical_processed": historical_data
        }

        save_to_postgres(symbol, message)

        message_df = spark.createDataFrame([(symbol, json.dumps(message))], ["key", "value"])
        message_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "stock-processed-topic") \
            .save()


query = exploded_df.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="1 minute") \
    .outputMode("update") \
    .start()

# Xử lý dừng an toàn
import signal
def handle_sigterm(sig, frame):
    print("Stopping Spark gracefully...")
    if query: # Kiểm tra query tồn tại trước khi stop
        query.stop()
    spark.stop() # Dừng spark context
    print("Spark stopped successfully")

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

try:
    query.awaitTermination()
except Exception as e: 
    print(f"Error during streaming query: {e}")
finally:
    if not spark.sparkContext._jsc.sc().isStopped(): 
        print("Ensuring Spark is stopped in final block...")
        spark.stop()
    print("Application stopped") 
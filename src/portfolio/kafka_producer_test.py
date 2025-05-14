import json
import time
from kafka import KafkaProducer

def send_test_data():
    """
    Gửi dữ liệu test đến Kafka
    """
    print("Bắt đầu gửi dữ liệu test đến Kafka...")
    
    try:
        # Khởi tạo Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Dữ liệu test theo định dạng của topic stock-processed-topic
        test_data = {
            "symbol": "TEST",
            "current_price_new": 100000,
            "ceiling_new": 110000,
            "floor_new": 90000,
            "vol_new": 10000,
            "historical_processed": [
                {
                    "time": "2023-01-01",
                    "open": 100000,
                    "high": 105000,
                    "low": 95000,
                    "close": 102000,
                    "volume": 500000,
                    "suggestion": "BUY",
                    "reason": "Test message",
                    "ma5": 101000,
                    "ma20": 100500,
                    "rsi": 65,
                    "macd_line": 0.5,
                    "macd_signal": 0.3,
                    "macd_histogram": 0.2
                }
            ]
        }
        
        # Thêm một vài mã cổ phiếu VN phổ biến
        additional_symbols = [
            {
                "symbol": "VCB", 
                "current_price_new": 89000,
                "ceiling_new": 95000,
                "floor_new": 85000,
                "vol_new": 25000,
                "historical_processed": [
                    {
                        "time": "2023-01-01",
                        "open": 88000,
                        "high": 90000,
                        "low": 87000,
                        "close": 89000,
                        "volume": 800000,
                        "suggestion": "HOLD",
                        "reason": "Xu hướng trung tính. Đề xuất: GIỮ - Chờ tín hiệu rõ ràng hơn.",
                        "ma5": 88500,
                        "ma20": 87500,
                        "rsi": 55,
                        "macd_line": 0.2,
                        "macd_signal": 0.1,
                        "macd_histogram": 0.1
                    }
                ]
            },
            {
                "symbol": "FPT", 
                "current_price_new": 115000,
                "ceiling_new": 120000,
                "floor_new": 110000,
                "vol_new": 15000,
                "historical_processed": [
                    {
                        "time": "2023-01-01",
                        "open": 112000,
                        "high": 116000,
                        "low": 111000,
                        "close": 115000,
                        "volume": 450000,
                        "suggestion": "BUY",
                        "reason": "Tín hiệu MUA: Giá vượt trên đường MA20, MACD dương",
                        "ma5": 113000,
                        "ma20": 110000,
                        "rsi": 68,
                        "macd_line": 0.8,
                        "macd_signal": 0.4,
                        "macd_histogram": 0.4
                    }
                ]
            },
            {
                "symbol": "VNM", 
                "current_price_new": 72000,
                "ceiling_new": 76000,
                "floor_new": 68000,
                "vol_new": 18000,
                "historical_processed": [
                    {
                        "time": "2023-01-01",
                        "open": 73000,
                        "high": 74000,
                        "low": 71000,
                        "close": 72000,
                        "volume": 350000,
                        "suggestion": "SELL",
                        "reason": "Tín hiệu BÁN: RSI trên 70 (quá mua), MACD cắt xuống Signal",
                        "ma5": 73000,
                        "ma20": 74000,
                        "rsi": 72,
                        "macd_line": -0.3,
                        "macd_signal": -0.1,
                        "macd_histogram": -0.2
                    }
                ]
            }
        ]
        
        # Gửi dữ liệu test đầu tiên
        future = producer.send('stock-processed-topic', test_data)
        future.get(timeout=5)
        print(f"Đã gửi dữ liệu cho {test_data['symbol']}")
        
        # Gửi các dữ liệu bổ sung
        for data in additional_symbols:
            future = producer.send('stock-processed-topic', data)
            future.get(timeout=5)
            print(f"Đã gửi dữ liệu cho {data['symbol']}")
            time.sleep(0.5)  # Đợi 0.5 giây giữa các lần gửi
        
        # Đảm bảo tất cả dữ liệu đã được gửi
        producer.flush()
        producer.close()
        
        print("Hoàn thành gửi dữ liệu test đến Kafka!")
        return True
        
    except Exception as e:
        print(f"Lỗi khi gửi dữ liệu test đến Kafka: {str(e)}")
        return False

if __name__ == "__main__":
    send_test_data() 
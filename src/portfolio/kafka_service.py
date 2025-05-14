import json
import logging
from datetime import datetime
from django.conf import settings
import time

logger = logging.getLogger(__name__)

# Global cache để lưu trữ dữ liệu Kafka gần nhất
_kafka_data_cache = {}
_last_cache_update = 0
_CACHE_EXPIRY = 30  # Thời gian hết hạn cache (giây)

def get_price_board_data():
    """
    Lấy dữ liệu bảng giá từ Kafka
    
    Returns:
        list: Danh sách các dữ liệu cổ phiếu theo định dạng
            [
                {
                    "CK": "AAA",
                    "Trần": 12000,
                    "Sàn": 10000,
                    "TC": 11000,
                    "Giá": 11500,
                    "Khối_lượng": 100000
                },
                ...
            ]
    """
    logger.debug("===== BẮT ĐẦU XỬ LÝ DỮ LIỆU KAFKA =====")
    
    global _kafka_data_cache, _last_cache_update
    
    # Kiểm tra xem cache có còn hiệu lực không
    current_time = time.time()
    if _kafka_data_cache and (current_time - _last_cache_update) < _CACHE_EXPIRY:
        logger.info(f"Sử dụng dữ liệu cache (còn {int(_CACHE_EXPIRY - (current_time - _last_cache_update))}s)")
        return list(_kafka_data_cache.values())
    
    try:
        # Thử kết nối đến Kafka để lấy dữ liệu
        from kafka import KafkaConsumer
        import json
        
        logger.info("Bắt đầu kết nối đến Kafka broker (kafka:9092)")
        
        # Kiểm tra các topics có sẵn
        try:
            # Tạo consumer tạm thời để kiểm tra topics
            temp_consumer = KafkaConsumer(bootstrap_servers=['kafka:9092'])
            topics = temp_consumer.topics()
            logger.info(f"Kafka topics có sẵn: {list(topics)}")
            temp_consumer.close()
            
            if 'stock-processed-topic' not in topics:
                # Gọi hàm để tạo dữ liệu test nếu topic không tồn tại
                logger.warning("Topic 'stock-processed-topic' không tồn tại, đang tạo dữ liệu test...")
                send_test_data_to_kafka()
                
        except Exception as e:
            logger.error(f"Lỗi khi kiểm tra topics Kafka: {str(e)}")
        
        # Khởi tạo consumer với timeout ngắn hơn (1 giây)
        logger.debug("Khởi tạo KafkaConsumer với topic 'stock-processed-topic'")
        consumer = KafkaConsumer(
            'stock-processed-topic',
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset='earliest',  # Đọc từ đầu topic
            enable_auto_commit=True,
            group_id='stock-processed-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=1000  # 1 giây timeout
        )
        
        logger.info("Đã khởi tạo KafkaConsumer, đang chờ messages...")
        
        # Đọc tất cả message đã có trong topic
        messages = []
        symbol_data = {}
        
        logger.debug("Bắt đầu vòng lặp để đọc messages từ Kafka...")
        for message in consumer:
            try:
                # Xử lý message
                data = message.value
                messages.append(data)
                
                # Lưu vào cache dựa trên symbol
                if isinstance(data, dict) and 'symbol' in data:
                    symbol = data['symbol']
                    logger.info(f"Nhận được message từ Kafka: {data}")
                    symbol_data[symbol] = data
                    logger.debug(f"Đã thêm message vào danh sách, số lượng hiện tại: {len(messages)}")
            except Exception as e:
                logger.error(f"Lỗi khi xử lý message: {str(e)}")
                
        # Đóng consumer sau khi đọc xong
        consumer.close()
        logger.info("Đã đóng KafkaConsumer")
        
        # Chỉ gửi dữ liệu test nếu không có dữ liệu và đây là request thực tế (không phải DEBUG)
        if not messages:
            logger.info("Không có dữ liệu mới từ Kafka")
            if not _kafka_data_cache:  # Nếu cache rỗng
                # Gửi dữ liệu test đến Kafka
                result = send_test_data_to_kafka()
                logger.info(f"Đã gửi dữ liệu test đến Kafka: {result}")
            else:
                # Sử dụng cache cũ nếu có
                logger.info("Sử dụng dữ liệu cache cũ do không có dữ liệu mới")
                return list(_kafka_data_cache.values())
        else:
            logger.info(f"Đã nhận được {len(messages)} tin nhắn từ Kafka")
            
            # Chuyển đổi dữ liệu thành định dạng frontend
            frontend_data = []
            
            # Ưu tiên xử lý dữ liệu mới nhất cho mỗi symbol
            for symbol, data in symbol_data.items():
                logger.info(f"Xử lý tin nhắn cho {symbol}, kiểu dữ liệu: {type(data)}")
                
                try:
                    # Chuyển đổi từ định dạng Kafka sang định dạng frontend
                    if isinstance(data, dict):
                        logger.debug(f"Nội dung tin nhắn: {data}")
                        
                        if 'symbol' in data:
                            logger.info(f"Tin nhắn ở định dạng dict với trường 'symbol': {data['symbol']}")
                            
                            # Hiển thị các trường dữ liệu có sẵn để debug
                            logger.debug(f"Các trường dữ liệu có sẵn: {list(data.keys())}")
                            
                            # Lấy giá trị trực tiếp từ dữ liệu Kafka
                            current_price = float(data.get('current_price_new', 0))
                            ceiling_price = float(data.get('ceiling_new', 0))
                            floor_price = float(data.get('floor_new', 0))
                            
                            # Xử lý đặc biệt cho vol_new
                            vol_value = data.get('vol_new')
                            logger.info(f"Giá trị vol_new ban đầu: {vol_value}, kiểu: {type(vol_value)}")
                            
                            # Đảm bảo giá trị hợp lệ
                            if vol_value is None:
                                volume = 0
                            else:
                                try:
                                    volume = float(vol_value)
                                    logger.info(f"Đã chuyển đổi vol_new thành float: {volume}")
                                    if volume < 1:  # Nếu khối lượng quá nhỏ, nhân lên để dễ nhìn
                                        volume = volume * 1000
                                except (ValueError, TypeError) as e:
                                    logger.error(f"Lỗi khi chuyển đổi vol_new: {e}")
                                    volume = 0
                            
                            # Tính giá tham chiếu
                            reference_price = (ceiling_price + floor_price) / 2
                            
                            # Chuyển đổi từ định dạng Kafka thành định dạng frontend
                            stock_data = {
                                'CK': data.get('symbol', ''),
                                'Trần': ceiling_price,
                                'Sàn': floor_price,
                                'TC': reference_price,
                                'Giá': current_price,
                                'Khối_lượng': int(volume)
                            }
                            
                            logger.info(f"Stock data sau khi chuyển đổi: Khối lượng = {stock_data['Khối_lượng']}")
                            
                            # Lưu dữ liệu lịch sử vào json_data
                            if 'historical_processed' in data:
                                historical = data['historical_processed']
                                stock_data['json_data'] = {'historical_processed': historical}
                                
                                logger.info(f"Tin nhắn có dữ liệu lịch sử, số lượng điểm: {len(historical)}")
                                
                                # Lấy đề xuất và lý do từ điểm dữ liệu lịch sử mới nhất
                                if historical and isinstance(historical, list) and len(historical) > 0:
                                    latest_point = historical[0]
                                    if 'suggestion' in latest_point:
                                        stock_data['suggestion'] = latest_point.get('suggestion')
                                        stock_data['reason'] = latest_point.get('reason', '')
                                        logger.debug(f"Thêm đề xuất: {stock_data['suggestion']}, lý do: {stock_data['reason']}")
                            
                            frontend_data.append(stock_data)
                except Exception as e:
                    logger.error(f"Lỗi khi chuyển đổi dữ liệu: {str(e)}")
            
            # Cập nhật cache
            for item in frontend_data:
                if 'CK' in item:
                    _kafka_data_cache[item['CK']] = item
            
            _last_cache_update = current_time
            logger.info("Đã chuyển đổi dữ liệu thành định dạng frontend")
            logger.debug(f"Dữ liệu trả về: {frontend_data}")
            
            return frontend_data
    
    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu từ Kafka: {str(e)}", exc_info=True)
        
        # Nếu có dữ liệu cache, sử dụng cache
        if _kafka_data_cache:
            logger.warning("Đang sử dụng dữ liệu cache do lỗi kết nối Kafka")
            return list(_kafka_data_cache.values())
        
        # Nếu không có cache, trả về dữ liệu mẫu
        logger.warning("Đang sử dụng dữ liệu mẫu do lỗi kết nối Kafka")
        
        # Tạo dữ liệu mẫu
        sample_data = [
            {
                'CK': 'FPT', 
                'Trần': 120000, 
                'Sàn': 100000,
                'TC': 110000,
                'Giá': 115000,
                'Khối_lượng': 100000,
                'suggestion': 'BUY',
                'reason': 'Tín hiệu MUA: Giá vượt trên đường MA20, MACD dương'
            },
            {
                'CK': 'VNM', 
                'Trần': 80000, 
                'Sàn': 70000,
                'TC': 75000,
                'Giá': 72000,
                'Khối_lượng': 50000,
                'suggestion': 'HOLD',
                'reason': 'Xu hướng trung tính. Đề xuất: GIỮ - Chờ tín hiệu rõ ràng hơn.'
            },
            {
                'CK': 'VIC', 
                'Trần': 60000, 
                'Sàn': 50000,
                'TC': 55000,
                'Giá': 57000,
                'Khối_lượng': 75000,
                'suggestion': 'SELL',
                'reason': 'Tín hiệu BÁN: RSI trên 70 (quá mua), MACD cắt xuống Signal'
            }
        ]
        logger.debug(f"Dữ liệu mẫu: {sample_data}")
        return sample_data

def send_test_data_to_kafka():
    """
    Gửi dữ liệu test đến Kafka để đảm bảo có ít nhất một số dữ liệu để hiển thị
    """
    try:
        from kafka import KafkaProducer
        import json
        
        # Khởi tạo producer
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Dữ liệu test mẫu cập nhật theo định dạng thực tế
        test_data = {
            "symbol": "VCB",
            "current_price_new": 56800.0,
            "ceiling_new": 60500.0,
            "floor_new": 52700.0,
            "vol_new": 500.0,
            "historical_processed": [
                {
                    "time": "2020-01-02", 
                    "open": 39.23, 
                    "high": 39.75, 
                    "low": 39.01, 
                    "close": 39.49, 
                    "volume": 386290.0, 
                    "suggestion": "HOLD", 
                    "reason": "Xu hướng trung tính. Đề xuất: GIỮ - Chờ tín hiệu rõ ràng hơn.", 
                    "ma5": None, 
                    "ma20": None, 
                    "rsi": None, 
                    "macd_line": 0.0, 
                    "macd_signal": 0.0, 
                    "macd_histogram": 0.0
                },
                {
                    "time": "2020-01-06", 
                    "open": 38.8, 
                    "high": 38.93, 
                    "low": 38.06, 
                    "close": 38.06, 
                    "volume": 880110.0, 
                    "suggestion": "HOLD", 
                    "reason": "Xu hướng trung tính. Đề xuất: GIỮ - Chờ tín hiệu rõ ràng hơn.", 
                    "ma5": None, 
                    "ma20": None, 
                    "rsi": None, 
                    "macd_line": -0.11407407407407533, 
                    "macd_signal": -0.022814814814815065, 
                    "macd_histogram": -0.09125925925926026
                }
            ]
        }
        
        # Thêm dữ liệu test bổ sung với khối lượng lớn
        additional_data = [
            {
                "symbol": "FPT",
                "current_price_new": 115000.0,
                "ceiling_new": 120000.0,
                "floor_new": 110000.0,
                "vol_new": 100000.0,
                "historical_processed": []
            },
            {
                "symbol": "VNM",
                "current_price_new": 75000.0,
                "ceiling_new": 80000.0,
                "floor_new": 70000.0,
                "vol_new": 50000.0,
                "historical_processed": []
            },
            {
                "symbol": "HPG",
                "current_price_new": 26500.0,
                "ceiling_new": 28000.0,
                "floor_new": 25000.0,
                "vol_new": 200000.0,
                "historical_processed": []
            }
        ]
        
        # Gửi dữ liệu đến topic
        future = producer.send('stock-processed-topic', test_data)
        result = future.get(timeout=5)  # Giảm timeout xuống 5 giây
        
        # Gửi các dữ liệu bổ sung
        for item in additional_data:
            producer.send('stock-processed-topic', item)
            logger.info(f"Đã gửi dữ liệu test cho mã {item['symbol']} với khối lượng {item['vol_new']}")
        
        # Đảm bảo dữ liệu đã được gửi
        producer.flush()
        producer.close()
        
        return True
    except Exception as e:
        logger.error(f"Lỗi khi gửi dữ liệu test đến Kafka: {str(e)}")
        return False
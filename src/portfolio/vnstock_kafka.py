import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import random
import time
import requests
import logging
import pandas as pd
import numpy as np

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kiểm tra nếu kafka-python không khả dụng
KAFKA_AVAILABLE = False
try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    logger.warning("kafka-python không được cài đặt, sẽ sử dụng dữ liệu mẫu")

# Kiểm tra nếu vnstock không khả dụng
VNSTOCK_AVAILABLE = False
try:
    from vnstock import Vnstock
    VNSTOCK_AVAILABLE = True
    # Khởi tạo vnstock instance toàn cục
    vnstock_instance = Vnstock()
except ImportError:
    logger.warning("vnstock không được cài đặt, không thể lấy dữ liệu trực tiếp")

def get_price_board(symbols_list=None):
    """
    Lấy bảng giá thị trường
    
    Args:
        symbols_list: Danh sách các mã cổ phiếu cần lấy dữ liệu
    """
    if not VNSTOCK_AVAILABLE:
        logger.warning("vnstock không khả dụng, không thể lấy bảng giá")
        return pd.DataFrame()
        
    try:
        # Khởi tạo VNStock và lấy danh sách mã
        stock = vnstock_instance.stock(symbol='VN30', source='VCI')
        
        # Nếu không có symbols_list, chỉ lấy VN30 để nhanh hơn
        if not symbols_list:
            # Lấy danh sách VN30
            try:
                vn30_symbols = [
                    "VCB", "VIC", "VHM", "HPG", "BID", "VNM", "FPT", "MSN", "MWG", "TCB",
                    "CTG", "VPB", "NVL", "VRE", "GAS", "SAB", "PLX", "STB", "POW", "HDB",
                    "VJC", "TPB", "ACB", "PDR", "BCM", "BVH", "GVR", "SSI", "KDH", "LPB"
                ]
                symbols_list = vn30_symbols
            except Exception as e:
                logger.error(f"Lỗi khi lấy danh sách VN30: {str(e)}")
                symbols_list = []
        
        if not symbols_list:
            logger.warning("Không có danh sách mã cổ phiếu để lấy bảng giá")
            return pd.DataFrame()
            
        logger.info(f"Fetching price board for {len(symbols_list)} symbols...")
        
        # Lấy dữ liệu bảng giá cho tất cả cổ phiếu
        try:
            # Lấy theo batch để tránh quá tải API
            batch_size = 10
            all_price_boards = []
            
            for i in range(0, len(symbols_list), batch_size):
                batch = symbols_list[i:i+batch_size]
                logger.info(f"Lấy dữ liệu cho batch {i//batch_size + 1}/{(len(symbols_list) + batch_size - 1)//batch_size}")
                try:
                    batch_board = stock.trading.price_board(symbols_list=batch)
                    all_price_boards.append(batch_board)
                except Exception as e:
                    logger.error(f"Lỗi lấy dữ liệu cho batch {i//batch_size + 1}: {str(e)}")
            
            if not all_price_boards:
                logger.warning("Không lấy được dữ liệu bảng giá nào")
                return pd.DataFrame()
                
            price_board_df = pd.concat(all_price_boards, ignore_index=True)
            
            # Kiểm tra và định dạng lại tên cột
            if isinstance(price_board_df.columns, pd.MultiIndex):
                price_board_df.columns = ['_'.join(map(str, col)).strip() for col in price_board_df.columns.values]
            
            # Tìm tên cột tương ứng với giá khớp lệnh
            match_price_column = None
            for col in ['match_match_price', 'match_price']:
                if col in price_board_df.columns:
                    match_price_column = col
                    break
            
            if match_price_column:
                price_board_df['match_price'] = price_board_df[match_price_column]
            
            # Đảm bảo các cột cần thiết tồn tại
            required_cols = {
                'listing_symbol': 'CK',
                'listing_ceiling': 'Trần',
                'listing_floor': 'Sàn',
                'listing_ref_price': 'TC',
                'match_price': 'Giá',
                'match_match_vol': 'Khối lượng'
            }
            
            # Tạo DataFrame mới với các cột cần thiết và đổi tên
            formatted_cols = []
            for col, new_name in required_cols.items():
                if col in price_board_df.columns:
                    formatted_cols.append(col)
                else:
                    logger.warning(f"WARNING: Missing column {col} in price board")
            
            # Nếu có đủ cột thì mới tiếp tục
            if len(formatted_cols) > 0:
                formatted_df = price_board_df[formatted_cols].rename(
                    columns={col: new_name for col, new_name in required_cols.items() if col in formatted_cols}
                )
                
                # Chuyển đổi kiểu dữ liệu cho các cột số
                numeric_cols = ['Trần', 'Sàn', 'TC', 'Giá']
                for col in numeric_cols:
                    if col in formatted_df.columns:
                        formatted_df[col] = pd.to_numeric(formatted_df[col], errors='coerce')
                
                # Chuyển đổi kiểu dữ liệu cho Khối lượng
                if 'Khối lượng' in formatted_df.columns:
                    # Đảm bảo Khối lượng là số trước khi định dạng
                    formatted_df['Khối lượng'] = pd.to_numeric(formatted_df['Khối lượng'], errors='coerce')
                    formatted_df['Khối lượng'] = formatted_df['Khối lượng'].apply(
                        lambda x: "{:,}".format(int(x)) if pd.notnull(x) and not isinstance(x, str) else "0"
                    )
                
                logger.info(f"Successfully fetched data for {len(formatted_df)} stocks")
                return formatted_df
                
        except Exception as e:
            logger.error(f"Error fetching price board: {str(e)}")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Error in get_price_board: {str(e)}")
        return pd.DataFrame()

class StockKafkaService:
    """
    Service để kết nối và lấy dữ liệu từ Kafka
    """
    def __init__(self):
        self.topic_name = "stock-history-topic"
        # Lưu cache dữ liệu
        self._data_cache = {}
        self._symbols_cache = None
        self._latest_prices = None
        self._kafka_data = None
        self._price_board = None
        
        # Kết nối kafka theo nhu cầu khi lấy dữ liệu (lazy loading)
        self._kafka_consumer = None
        self._initialize_cache()
    
    def _initialize_cache(self):
        """Khởi tạo cơ bản cho cache dữ liệu"""
        try:
            logger.info("Khởi tạo service Kafka")
            # Thử lấy dữ liệu có sẵn ban đầu - chỉ lấy số lượng nhỏ để nhanh
            initial_data = self._get_kafka_data(max_messages=50, timeout=3)
            
            # Nếu lấy được dữ liệu, cập nhật cache
            if initial_data:
                logger.info(f"Đã lấy được dữ liệu ban đầu: {len(initial_data)} messages")
                self._parse_kafka_data(initial_data)
            else:
                logger.warning("Chưa có dữ liệu Kafka ban đầu, sẽ tải khi cần")
                self._data_cache = {}
                self._symbols_cache = []
                self._latest_prices = {}
            
            # Chuẩn bị sẵn các symbol VN30 để đảm bảo luôn có dữ liệu
            vn30_symbols = [
                "VCB", "VIC", "VHM", "HPG", "BID", "VNM", "FPT", "MSN", "MWG", "TCB",
                "CTG", "VPB", "NVL", "VRE", "GAS", "SAB", "PLX", "STB", "POW", "HDB",
                "VJC", "TPB", "ACB", "PDR", "BCM", "BVH", "GVR", "SSI", "KDH", "LPB"
            ]
            
            # Đảm bảo các symbol VN30 có trong cache
            for symbol in vn30_symbols:
                if symbol not in self._data_cache:
                    logger.info(f"Tiền tải dữ liệu cho {symbol}")
                    self.refresh_symbol_data(symbol)
                
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Kafka cache: {e}")
            self._data_cache = {}
            self._symbols_cache = []
            self._latest_prices = {}

    def _create_kafka_consumer(self):
        """Tạo Kafka consumer khi cần"""
        if not KAFKA_AVAILABLE:
            logger.warning("Kafka không khả dụng, không thể tạo consumer")
            return None
            
        # Thử nhiều địa chỉ bootstrap servers khác nhau
        bootstrap_servers = ['kafka:9092', 'localhost:9092', '127.0.0.1:9092', '172.28.0.5:9092']
        
        try:
            # Tạo KafkaConsumer với nhiều bootstrap_servers
            consumer = KafkaConsumer(
                self.topic_name,  # Đọc từ topic đã xử lý
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='webapp-consumer-' + str(int(time.time())),  # Thêm timestamp để tạo group mới
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                fetch_max_bytes=5242880,  # 5MB fetch size
                session_timeout_ms=3000,  # Giảm timeout
                request_timeout_ms=5000,  # Giảm timeout
                consumer_timeout_ms=2000   # Giảm timeout
            )
            
            # Kiểm tra kết nối - giới hạn thời gian chờ
            start_time = time.time()
            while time.time() - start_time < 2:  # Chỉ chờ tối đa 2 giây
                try:
                    topics = consumer.topics()
                    if self.topic_name in topics:
                        return consumer
                    break
                except Exception as e:
                    # Nếu có lỗi khi kiểm tra topics, đợi một chút và thử lại
                    time.sleep(0.1)
            
            logger.warning(f"Topic {self.topic_name} không tồn tại hoặc không thể kết nối!")
            consumer.close()
            return None
            
        except Exception as e:
            logger.error(f"Lỗi tạo Kafka consumer: {str(e)}")
            return None

    def _get_kafka_data(self, max_messages=1000, timeout=5, symbol_filter=None):
        """
        Kết nối Kafka và lấy dữ liệu từ topic stock-processed-topic
        Có thể lọc theo mã cổ phiếu cụ thể
        """
        # Nếu không có Kafka, trả về None
        if not KAFKA_AVAILABLE:
            logger.warning("Kafka không khả dụng, không thể lấy dữ liệu")
            return None
            
        try:
            # Tạo consumer mới nếu chưa có hoặc tái sử dụng consumer cũ
            consumer = self._create_kafka_consumer()
            if not consumer:
                return None
            
            messages = []
            start_time = time.time()
            found_symbols = set()
            
            logger.info(f"Đang tải dữ liệu từ Kafka{' cho ' + symbol_filter if symbol_filter else ''}...")
            
            # Poll cho đến khi tìm thấy dữ liệu hoặc hết thời gian chờ
            while time.time() - start_time < timeout and len(messages) < max_messages:
                msg_pack = consumer.poll(timeout_ms=1000, max_records=100)
                
                if not msg_pack:
                    # Nếu không có messages mới, kiểm tra điều kiện thoát
                    if len(messages) > 0:
                        # Nếu đã tìm thấy symbol cần lọc hoặc đã có đủ messages, thoát
                        if symbol_filter and symbol_filter in found_symbols:
                            logger.info(f"Đã tìm thấy dữ liệu cho {symbol_filter}, dừng tìm kiếm")
                            break
                    
                    # Đợi ngắn hơn
                    time.sleep(0.1)
                    continue
                
                # Xử lý các messages mới
                for _, msgs in msg_pack.items():
                    for msg in msgs:
                        symbol = msg.value.get('symbol')
                        if not symbol:
                            continue
                            
                        # Nếu đang lọc theo symbol và không phải symbol cần tìm, bỏ qua
                        if symbol_filter and symbol != symbol_filter:
                            continue
                            
                        messages.append(msg.value)
                        found_symbols.add(symbol)
                        
                        # Nếu đã tìm thấy symbol cần lọc, có thể thoát sớm
                        if symbol_filter and symbol == symbol_filter and len(messages) > 0:
                            logger.info(f"Đã tìm thấy dữ liệu cho {symbol_filter}, dừng tìm kiếm")
                            break
                
                # Nếu mất quá 2 giây mà không có thêm dữ liệu mới, thoát sớm
                if len(messages) > 0 and time.time() - start_time > 2:
                    logger.info(f"Đã có dữ liệu ban đầu và mất quá 2 giây, thoát sớm")
                    break
            
            consumer.close()
            
            if not messages:
                logger.warning(f"Không tìm thấy dữ liệu Kafka{' cho ' + symbol_filter if symbol_filter else ''}")
                return None
                
            return messages
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu từ Kafka: {str(e)}")
            return None

    def refresh_symbol_data(self, symbol):
        """
        Làm mới dữ liệu cho một mã cổ phiếu cụ thể từ Kafka
        """
        logger.info(f"Đang làm mới dữ liệu cho mã {symbol}...")
        
        try:
            # Lấy dữ liệu mới từ Kafka cho mã cụ thể
            messages = self._get_kafka_data(max_messages=100, timeout=10, symbol_filter=symbol)
            
            if not messages:
                logger.warning(f"Không tìm thấy dữ liệu mới cho mã {symbol}")
                return False
                
            # Xử lý dữ liệu mới
            symbol_group = []
            for msg in messages:
                if msg.get('symbol') == symbol:
                    symbol_group.append(msg)
            
            if not symbol_group:
                logger.warning(f"Không thể tổ chức dữ liệu cho mã {symbol}")
                return False
                
            # Sắp xếp theo thời gian
            sorted_records = sorted(symbol_group, key=lambda x: x.get('time', ''))
            
            if sorted_records:
                # Lấy bản ghi mới nhất
                latest_record = sorted_records[-1]
                
                # Cập nhật cache
                self._data_cache[symbol] = latest_record
                self._latest_prices[symbol] = latest_record.get('close', 0)
                
                # Thêm dữ liệu lịch sử
                latest_record['historical_data'] = sorted_records
                
                # Cập nhật danh sách symbols nếu chưa có
                if self._symbols_cache is None:
                    self._symbols_cache = []
                if symbol not in self._symbols_cache:
                    self._symbols_cache.append(symbol)
                    
                logger.info(f"Đã cập nhật dữ liệu cho mã {symbol}: {len(sorted_records)} bản ghi")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Lỗi khi làm mới dữ liệu cho mã {symbol}: {str(e)}")
            return False

    def get_available_data_symbols(self):
        """
        Lấy danh sách các mã cổ phiếu đã có dữ liệu sẵn sàng
        """
        # Trả về keys của data_cache - chỉ những mã đã có dữ liệu
        return list(self._data_cache.keys())
        
    def get_available_symbols(self):
        """
        Lấy danh sách tất cả các mã cổ phiếu có thể lấy dữ liệu
        """
        if not self._symbols_cache:
            # Thử lấy danh sách symbols từ Kafka
            try:
                messages = self._get_kafka_data(max_messages=1000, timeout=10)
                if messages:
                    all_symbols = set([msg.get('symbol') for msg in messages if msg.get('symbol')])
                    self._symbols_cache = list(all_symbols)
                    logger.info(f"Đã cập nhật danh sách symbols: {len(self._symbols_cache)} mã")
            except Exception as e:
                logger.error(f"Lỗi khi lấy danh sách symbols: {str(e)}")
                self._symbols_cache = []
                
        return self._symbols_cache or []

    def _parse_kafka_data(self, messages):
        """
        Xử lý dữ liệu từ Kafka thành dạng phù hợp, gom nhóm theo mã cổ phiếu
        """
        if not messages:
            return
            
        # Xử lý dữ liệu từ Kafka thành dạng dict với key là symbol
        data_by_symbol = {}
        symbols = set()
        latest_prices = {}
        
        # Kiểm tra tất cả symbols trong messages
        all_symbols = [msg.get('symbol') for msg in messages if msg.get('symbol')]
        unique_symbols = set(all_symbols)
        symbol_counts = {symbol: all_symbols.count(symbol) for symbol in unique_symbols}
        
        logger.info(f"Symbols trong Kafka data: {unique_symbols}")
        for symbol, count in symbol_counts.items():
            logger.info(f"  - Mã {symbol}: {count} messages")
        
        # Bước 1: Gom nhóm dữ liệu theo mã cổ phiếu
        symbol_groups = {}
        for msg in messages:
            symbol = msg.get('symbol')
            if not symbol:
                continue
                
            symbols.add(symbol)
            
            # Thêm message vào nhóm của mã cổ phiếu tương ứng
            if symbol not in symbol_groups:
                symbol_groups[symbol] = []
            symbol_groups[symbol].append(msg)
            
        # In thông tin về số lượng dữ liệu cho mỗi mã
        logger.info(f"Số lượng mã cổ phiếu gom nhóm được: {len(symbol_groups)}")
        for symbol, records in symbol_groups.items():
            logger.info(f"  - Mã {symbol}: {len(records)} messages")
            
        # Bước 2: Xử lý từng nhóm, lấy bản ghi mới nhất
        for symbol, records in symbol_groups.items():
            # Sắp xếp bản ghi theo thời gian (mới nhất sau cùng)
            sorted_records = sorted(records, key=lambda x: x.get('time', ''))
            
            if sorted_records:
                # Lấy bản ghi mới nhất
                latest_record = sorted_records[-1]
                data_by_symbol[symbol] = latest_record
                
                # Lưu giá mới nhất
                latest_prices[symbol] = latest_record.get('close', 0)
                
                # Thêm trường historical_data chứa tất cả dữ liệu lịch sử
                data_by_symbol[symbol]['historical_data'] = sorted_records
        
        # Cập nhật cache
        self._data_cache = data_by_symbol
        self._symbols_cache = list(symbols)
        self._latest_prices = latest_prices
        
        logger.info(f"Đã xử lý dữ liệu cho {len(symbols)} mã cổ phiếu")

    def get_stock_data(self, symbol):
        """
        Lấy dữ liệu cho một mã cổ phiếu cụ thể
        """
        data = self._data_cache.get(symbol, None)
        
        # Nếu không có dữ liệu trong cache, thử làm mới dữ liệu từ Kafka
        if data is None:
            logger.info(f"Không tìm thấy dữ liệu cho {symbol} trong cache, thử làm mới...")
            success = self.refresh_symbol_data(symbol)
            if success:
                data = self._data_cache.get(symbol, None)
            else:
                logger.warning(f"Không thể làm mới dữ liệu cho {symbol}")
                return None
        
        # Đảm bảo các giá trị số đều được chuyển đổi đúng kiểu
        if data:
            numeric_fields = ['open', 'high', 'low', 'close', 'volume', 'current_price', 
                            'ma5', 'ma20', 'rsi', 'macd_line', 'macd_signal', 'macd_histogram',
                            'ceiling', 'floor', 'vol']
            
            for field in numeric_fields:
                if field in data:
                    try:
                        if isinstance(data[field], str):
                            data[field] = float(data[field]) if data[field] and data[field].strip() else 0.0
                    except (ValueError, TypeError):
                        data[field] = 0.0
            
            # Đảm bảo historical_data cũng được chuyển đổi kiểu
            if 'historical_data' in data and isinstance(data['historical_data'], list):
                for item in data['historical_data']:
                    for field in numeric_fields:
                        if field in item:
                            try:
                                if isinstance(item[field], str):
                                    item[field] = float(item[field]) if item[field] and item[field].strip() else 0.0
                            except (ValueError, TypeError):
                                item[field] = 0.0
                    
        return data
        
    def get_all_stock_data(self):
        """
        Lấy tất cả dữ liệu
        """
        return list(self._data_cache.values()) if self._data_cache else []
        
    def get_latest_prices(self):
        """
        Lấy giá mới nhất cho tất cả các mã
        """
        return self._latest_prices or {}
    
    def format_for_chart(self, stock_data):
        """
        Định dạng dữ liệu cổ phiếu để hiển thị trên biểu đồ kỹ thuật với đầy đủ chỉ báo
        """
        if not stock_data:
            logger.warning("Không thể định dạng dữ liệu cổ phiếu trống")
            return {}
        
        try:    
            # Lấy dữ liệu lịch sử nếu có
            historical_data = stock_data.get('historical_data', [])
            if not historical_data and 'time' in stock_data:
                # Nếu không có historical_data nhưng có dữ liệu cơ bản, tạo mảng đơn phần tử
                historical_data = [stock_data]
                
            # Kiểm tra nếu historical_data trống
            if not historical_data:
                logger.warning(f"Không có dữ liệu lịch sử cho mã {stock_data.get('symbol', 'unknown')}")
                return {
                    'symbol': stock_data.get('symbol', 'unknown'),
                    'candles': [],
                    'volumes': [],
                    'ma5': [],
                    'ma20': [],
                    'rsi': [],
                    'macd': [],
                    'signal': [],
                    'histogram': [],
                    'suggestion': 'UNKNOWN',
                    'reason': 'Không có dữ liệu lịch sử',
                    'latest': {},
                    'ceiling': stock_data.get('ceiling', 0),
                    'floor': stock_data.get('floor', 0),
                    'vol': stock_data.get('vol', 0)
                }
                
            # Sắp xếp dữ liệu theo thời gian
            historical_data = sorted(historical_data, key=lambda x: x.get('time', ''))
            
            # Log một số dữ liệu mẫu để debug
            sample_data = historical_data[:2] if len(historical_data) >= 2 else historical_data
            logger.info(f"Mẫu dữ liệu lịch sử cho biểu đồ: {sample_data}")
            
            # Chuẩn bị dữ liệu cho biểu đồ
            candles = []  # Dữ liệu nến
            volumes = []  # Dữ liệu khối lượng
            ma5_data = []  # MA5
            ma20_data = []  # MA20
            rsi_data = []  # RSI
            macd_data = []  # MACD
            signal_data = []  # MACD Signal
            histogram_data = []  # MACD Histogram
            
            for item in historical_data:
                time_str = item.get('time', '')
                if not time_str:  # Bỏ qua các bản ghi không có thời gian
                    continue
                
                # Chuyển đổi dữ liệu số sang đúng kiểu
                open_val = self._ensure_float(item.get('open', 0))
                high_val = self._ensure_float(item.get('high', 0))
                low_val = self._ensure_float(item.get('low', 0))
                close_val = self._ensure_float(item.get('close', 0))
                volume_val = self._ensure_float(item.get('volume', 0))
                
                # Dữ liệu nến
                candle = {
                    'time': time_str,
                    'open': open_val,
                    'high': high_val,
                    'low': low_val,
                    'close': close_val
                }
                candles.append(candle)
                
                # Dữ liệu khối lượng
                volumes.append({
                    'time': time_str,
                    'value': volume_val
                })
                
                # Dữ liệu MA5
                ma5_value = item.get('ma5')
                if ma5_value not in (None, "NaN"):
                    ma5_data.append({
                        'time': time_str,
                        'value': self._ensure_float(ma5_value)
                    })
                    
                # Dữ liệu MA20
                ma20_value = item.get('ma20')
                if ma20_value not in (None, "NaN"):
                    ma20_data.append({
                        'time': time_str,
                        'value': self._ensure_float(ma20_value)
                    })
                    
                # Dữ liệu RSI
                rsi_value = item.get('rsi')
                if rsi_value not in (None, "NaN"):
                    rsi_data.append({
                        'time': time_str,
                        'value': self._ensure_float(rsi_value)
                    })
                    
                # Dữ liệu MACD
                macd_value = item.get('macd_line')
                signal_value = item.get('macd_signal')
                hist_value = item.get('macd_histogram')
                
                if macd_value not in (None, "NaN"):
                    macd_data.append({
                        'time': time_str,
                        'value': self._ensure_float(macd_value)
                    })
                    
                if signal_value not in (None, "NaN"):
                    signal_data.append({
                        'time': time_str,
                        'value': self._ensure_float(signal_value)
                    })
                    
                if hist_value not in (None, "NaN"):
                    histogram_data.append({
                        'time': time_str,
                        'value': self._ensure_float(hist_value)
                    })
            
            # Kiểm tra nếu không có candles
            if not candles:
                logger.warning(f"Không có dữ liệu nến cho mã {stock_data.get('symbol', 'unknown')}")
                return {
                    'symbol': stock_data.get('symbol', 'unknown'),
                    'candles': [],
                    'volumes': [],
                    'ma5': [],
                    'ma20': [],
                    'rsi': [],
                    'macd': [],
                    'signal': [],
                    'histogram': [],
                    'suggestion': 'UNKNOWN',
                    'reason': 'Không có dữ liệu nến',
                    'latest': {},
                    'ceiling': stock_data.get('ceiling', 0),
                    'floor': stock_data.get('floor', 0),
                    'vol': stock_data.get('vol', 0)
                }
            
            # Thông tin chung và đề xuất
            symbol = stock_data.get('symbol', '')
            suggestion = stock_data.get('suggestion', 'HOLD')
            reason = stock_data.get('reason', 'Không có đề xuất')
            
            # Định dạng kết quả cuối cùng
            result = {
                'symbol': symbol,
                'candles': candles,
                'volumes': volumes,
                'ma5': ma5_data,
                'ma20': ma20_data,
                'rsi': rsi_data,
                'macd': macd_data,
                'signal': signal_data,
                'histogram': histogram_data,
                'suggestion': suggestion,
                'reason': reason,
                'latest': candles[-1] if candles else {},
                'ceiling': stock_data.get('ceiling', 0),
                'floor': stock_data.get('floor', 0),
                'vol': stock_data.get('vol', 0)
            }
            
            logger.info(f"Đã định dạng dữ liệu cho mã {symbol}: {len(candles)} nến")
            return result
            
        except Exception as e:
            import traceback
            logger.error(f"Lỗi khi định dạng dữ liệu cho biểu đồ: {str(e)}")
            logger.error(traceback.format_exc())
            # Trả về đối tượng rỗng với các trường cần thiết
            return {
                'symbol': stock_data.get('symbol', 'unknown'),
                'error': str(e),
                'candles': [],
                'volumes': [],
                'ma5': [],
                'ma20': [],
                'rsi': [],
                'macd': [],
                'signal': [],
                'histogram': [],
                'suggestion': 'ERROR',
                'reason': f'Lỗi: {str(e)}',
                'latest': {},
                'ceiling': stock_data.get('ceiling', 0),
                'floor': stock_data.get('floor', 0),
                'vol': stock_data.get('vol', 0)
            }

    def get_price_board_data(self):
        """
        Lấy dữ liệu bảng giá cho trang thị trường
        """
        # Nếu đã có dữ liệu bảng giá, trả về
        if self._price_board is not None and not self._price_board.empty:
            return self._price_board
            
        # Nếu chưa có, cố gắng lấy mới
        if VNSTOCK_AVAILABLE:
            logger.info("Lấy bảng giá mới từ VNStock API")
            self._price_board = get_price_board()
            if not self._price_board.empty:
                logger.info(f"Đã lấy bảng giá mới cho {len(self._price_board)} mã cổ phiếu")
                return self._price_board
                
        # Nếu không lấy được bảng giá mới, tạo bảng giá từ dữ liệu cache
        if self._data_cache:
            logger.info("Tạo bảng giá từ dữ liệu cache")
            data = []
            for symbol, stock_data in self._data_cache.items():
                if not stock_data:
                    continue
                    
                row = {
                    'CK': symbol,
                    'Giá': stock_data.get('close', 0),
                    'Khối lượng': "{:,}".format(int(stock_data.get('volume', 0)))
                }
                data.append(row)
                
            if data:
                return pd.DataFrame(data)
                
        # Nếu không có dữ liệu, trả về DataFrame rỗng
        return pd.DataFrame()

    def _ensure_float(self, value):
        """
        Đảm bảo giá trị là kiểu float
        """
        if value is None:
            return 0.0
        
        # Handle NaN values for proper JSON serialization
        if isinstance(value, float) and (pd.isna(value) or np.isnan(value)):
            return 0.0
        
        if isinstance(value, (int, float)):
            return float(value)
            
        try:
            if isinstance(value, str) and value.strip():
                return float(value)
        except (ValueError, TypeError):
            pass
            
        return 0.0

# Tạo một instance singletons
_stock_kafka_service = None

def get_stock_kafka_service():
    """
    Trả về singleton instance của StockKafkaService
    """
    global _stock_kafka_service
    if _stock_kafka_service is None:
        _stock_kafka_service = StockKafkaService()
    return _stock_kafka_service 
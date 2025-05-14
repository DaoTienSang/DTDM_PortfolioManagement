import json
import logging
import psycopg2
from django.conf import settings
from psycopg2.extras import RealDictCursor
import pandas as pd
import numpy as np
import traceback

logger = logging.getLogger(__name__)

class PostgresChartService:
    """
    Service để lấy dữ liệu từ PostgreSQL cho các biểu đồ
    """
    
    def __init__(self):
        """Khởi tạo kết nối đến PostgreSQL"""
        # Sử dụng cấu hình từ settings nếu có
        self.db_config = {
            'host': 'db',
            'dbname': 'db_for_pm',
            'user': 'airflow',
            'password': 'admin123',
            'port': '5432',
        }
        
        # Lưu trữ cache tạm thời
        self._symbol_cache = {}
        self._available_symbols = None
        
    def _get_connection(self):
        """Lấy kết nối đến PostgreSQL"""
        try:
            logger.info(f"Kết nối đến PostgreSQL với thông số: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}")
            conn = psycopg2.connect(
                host=self.db_config['host'],
                dbname=self.db_config['dbname'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                port=self.db_config['port']
            )
            return conn
        except Exception as e:
            logger.error(f"Lỗi kết nối PostgreSQL: {str(e)}")
            # Thử kết nối trực tiếp đến container
            try:
                logger.info("Thử kết nối với các thông số khác")
                conn = psycopg2.connect(
                    host='db',
                    dbname='db_for_pm',
                    user='airflow',
                    password='admin123',
                    port=5432
                )
                return conn
            except Exception as e2:
                logger.error(f"Lỗi kết nối PostgreSQL (lần 2): {str(e2)}")
                return None
    
    def get_available_symbols(self):
        """Lấy danh sách các mã cổ phiếu có trong database"""
        if self._available_symbols is not None:
            return self._available_symbols
            
        try:
            conn = self._get_connection()
            if not conn:
                logger.error("Không thể kết nối đến PostgreSQL")
                return []
                
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT symbol FROM stock_processed_data ORDER BY symbol")
                symbols = [row[0] for row in cur.fetchall()]
                logger.info(f"Đã lấy {len(symbols)} mã cổ phiếu từ PostgreSQL")
                self._available_symbols = symbols
                return symbols
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách mã cổ phiếu: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()
    
    def get_stock_data(self, symbol):
        """Lấy dữ liệu cổ phiếu từ PostgreSQL theo mã"""
        if symbol in self._symbol_cache:
            logger.info(f"Trả về dữ liệu từ cache cho mã {symbol}")
            return self._symbol_cache[symbol]
            
        try:
            conn = self._get_connection()
            if not conn:
                logger.error("Không thể kết nối đến PostgreSQL")
                return None
                
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Lấy dữ liệu cơ bản và json_data
                cur.execute("""
                    SELECT symbol, current_price, ceiling, floor, vol, json_data, updated_at 
                    FROM stock_processed_data 
                    WHERE symbol = %s
                """, (symbol,))
                
                result = cur.fetchone()
                if not result:
                    logger.warning(f"Không tìm thấy dữ liệu cho mã {symbol}")
                    return None
                
                # Chuyển đổi từ RealDictRow sang dict
                stock_data = dict(result)
                
                # Chuyển đổi json_data từ chuỗi sang dict nếu cần
                if isinstance(stock_data['json_data'], str):
                    stock_data['json_data'] = json.loads(stock_data['json_data'])
                
                # Lưu vào cache
                self._symbol_cache[symbol] = stock_data
                logger.info(f"Đã lấy dữ liệu cho mã {symbol} từ PostgreSQL")
                return stock_data
                
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu cho mã {symbol}: {str(e)}")
            return None
        finally:
            if conn:
                conn.close()
    
    def format_for_chart(self, stock_data):
        """Định dạng dữ liệu để vẽ biểu đồ"""
        if not stock_data or not isinstance(stock_data, dict):
            logger.warning("Dữ liệu không hợp lệ để vẽ biểu đồ")
            return None
        
        try:
            # Lấy dữ liệu lịch sử từ json_data
            historical_data = stock_data.get('json_data', {}).get('historical_processed', [])
            if not historical_data:
                logger.warning("Không có dữ liệu lịch sử để vẽ biểu đồ")
                return None
            
            # Chuyển đổi thành định dạng cho biểu đồ nến
            candles = []
            ma5_data = []
            ma20_data = []
            rsi_data = []
            macd_line_data = []
            macd_signal_data = []
            macd_histogram_data = []
            
            # Lấy suggestion và reason từ bản ghi cuối cùng (mới nhất)
            # Sắp xếp dữ liệu lịch sử theo thời gian tăng dần
            historical_data = sorted(historical_data, key=lambda x: x.get('time', ''))
            
            # Lấy suggestion và reason từ record mới nhất nếu có
            latest_record = historical_data[-1] if historical_data else {}
            suggestion = latest_record.get('suggestion', '')
            reason = latest_record.get('reason', '')
            
            for item in historical_data:
                # Dữ liệu nến
                if all(k in item for k in ['time', 'open', 'high', 'low', 'close']):
                    candle = {
                        'time': item.get('time', ''),
                        'open': float(item.get('open', 0)) if item.get('open') is not None else 0,
                        'high': float(item.get('high', 0)) if item.get('high') is not None else 0,
                        'low': float(item.get('low', 0)) if item.get('low') is not None else 0,
                        'close': float(item.get('close', 0)) if item.get('close') is not None else 0,
                        'volume': float(item.get('volume', 0)) if item.get('volume') is not None else 0,
                    }
                    candles.append(candle)
                
                # Dữ liệu đường MA
                if 'ma5' in item and item['ma5'] is not None and not pd.isna(item['ma5']) and not np.isnan(float(item['ma5'])):
                    ma5_data.append({
                        'time': item.get('time', ''),
                        'value': float(item.get('ma5', 0))
                    })
                    
                if 'ma20' in item and item['ma20'] is not None and not pd.isna(item['ma20']) and not np.isnan(float(item['ma20'])):
                    ma20_data.append({
                        'time': item.get('time', ''),
                        'value': float(item.get('ma20', 0))
                    })
                
                # Dữ liệu RSI
                if 'rsi' in item and item['rsi'] is not None and not pd.isna(item['rsi']) and not np.isnan(float(item['rsi'])):
                    rsi_data.append({
                        'time': item.get('time', ''),
                        'value': float(item.get('rsi', 0))
                    })
                
                # Dữ liệu MACD
                if 'macd_line' in item and item['macd_line'] is not None and not pd.isna(item['macd_line']) and not np.isnan(float(item['macd_line'])):
                    macd_line_data.append({
                        'time': item.get('time', ''),
                        'value': float(item.get('macd_line', 0))
                    })
                
                if 'macd_signal' in item and item['macd_signal'] is not None and not pd.isna(item['macd_signal']) and not np.isnan(float(item['macd_signal'])):
                    macd_signal_data.append({
                        'time': item.get('time', ''),
                        'value': float(item.get('macd_signal', 0))
                    })
                
                if 'macd_histogram' in item and item['macd_histogram'] is not None and not pd.isna(item['macd_histogram']) and not np.isnan(float(item['macd_histogram'])):
                    macd_histogram_data.append({
                        'time': item.get('time', ''),
                        'value': float(item.get('macd_histogram', 0))
                    })
            
            # Chuẩn bị kết quả
            chart_data = {
                'symbol': stock_data.get('symbol', ''),
                'current_price': float(stock_data.get('current_price', 0)) if stock_data.get('current_price') is not None else 0,
                'ceiling': float(stock_data.get('ceiling', 0)) if stock_data.get('ceiling') is not None else 0,
                'floor': float(stock_data.get('floor', 0)) if stock_data.get('floor') is not None else 0,
                'vol': float(stock_data.get('vol', 0)) if stock_data.get('vol') is not None else 0,
                'updated_at': str(stock_data.get('updated_at', '')),
                'suggestion': suggestion,
                'reason': reason,
                'candles': candles,
                'ma5': ma5_data,
                'ma20': ma20_data,
                'rsi': rsi_data,
                'macd_line': macd_line_data,
                'macd_signal': macd_signal_data,
                'macd_histogram': macd_histogram_data
            }
            
            logger.info(f"Đã định dạng dữ liệu cho biểu đồ: {len(candles)} nến")
            return chart_data
            
        except Exception as e:
            logger.error(f"Lỗi khi định dạng dữ liệu cho biểu đồ: {str(e)}")
            logger.error(f"Chi tiết lỗi: {traceback.format_exc()}")
            return None
    
    def get_price_board_data(self):
        """Lấy dữ liệu bảng giá từ PostgreSQL"""
        try:
            conn = self._get_connection()
            if not conn:
                logger.error("Không thể kết nối đến PostgreSQL")
                return pd.DataFrame()
                
            query = """
                SELECT 
                    symbol as "CK", 
                    ceiling as "Trần", 
                    floor as "Sàn", 
                    (ceiling + floor) / 2 as "TC", 
                    current_price as "Giá", 
                    vol as "Khối_lượng",
                    COALESCE(json_data->>'suggestion', 'HOLD') as suggestion,
                    COALESCE(json_data->>'reason', 'Không có lý do') as reason
                FROM stock_processed_data
                ORDER BY symbol
            """
            
            # Đọc dữ liệu vào DataFrame
            df = pd.read_sql(query, conn)
            
            # Đóng kết nối
            conn.close()
            
            # Xử lý các giá trị null
            df.fillna({'suggestion': 'HOLD', 'reason': 'Không có lý do'}, inplace=True)
            
            # Chuyển đổi các cột số sang dạng số
            numeric_cols = ['Trần', 'Sàn', 'TC', 'Giá', 'Khối_lượng']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Log mẫu dữ liệu
            if not df.empty:
                logger.info(f"Sample data - first row: {df.iloc[0].to_dict()}")
            
            logger.info(f"Đã lấy dữ liệu bảng giá: {df.shape[0]} dòng, {df.shape[1]} cột")
            return df
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu bảng giá: {str(e)}")
            return pd.DataFrame()

# Tạo instance để sử dụng trong các views
_postgres_chart_service = None

def get_postgres_chart_service():
    """Singleton pattern để lấy service"""
    global _postgres_chart_service
    if _postgres_chart_service is None:
        _postgres_chart_service = PostgresChartService()
    return _postgres_chart_service 
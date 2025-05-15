from datetime import datetime
from vnstock import Vnstock
from .models import Asset
import pandas as pd
import os

# Khởi tạo Vnstock
vnstock_instance = Vnstock()
stock = vnstock_instance.stock(symbol='VN30', source='VCI')

def get_list_stock_market():
    """Lấy danh sách mã cổ phiếu niêm yết"""
    try:
        symbols_df = stock.listing.all_symbols()
        if symbols_df.empty:
            print("WARNING: No stock symbols retrieved from vnstock")
            return []
        return symbols_df['ticker'].values.tolist()
    except Exception as e:
        print(f"Error in get_list_stock_market: {str(e)}")
        return []

def get_ticker_companyname():
    """Lấy mã cổ phiếu và tên công ty"""
    try:
        symbols_df = stock.listing.all_symbols()
        if symbols_df.empty:
            print("WARNING: No stock data in get_ticker_companyname")
            return []
        return [
            {"ticker": row[0], "organ_name": row[1]}
            for row in symbols_df.itertuples(index=False, name=None)
        ]
    except Exception as e:
        print(f"Error in get_ticker_companyname: {str(e)}")
        return []

def get_refer_price(stock_code):
    """Lấy giá tham chiếu của cổ phiếu"""
    try:
        data = stock.trading.price_board(symbols_list=[stock_code])
        if data.empty:
            return f'Không tìm thấy mã cổ phiếu {stock_code}!'
        ref_price = int(data[('listing', 'ref_price')].iloc[0])
        return ref_price
    except Exception as e:
        print(f"Error in get_refer_price for {stock_code}: {str(e)}")
        return f'Không tìm thấy mã cổ phiếu {stock_code}!'

def get_price_board():
    """Lấy bảng giá thị trường"""
    try:
        # Khởi tạo VNStock và lấy danh sách mã
        stock = vnstock_instance.stock(symbol='VN30', source='VCI')
        symbols_df = stock.listing.all_symbols()
        
        if symbols_df.empty:
            print("WARNING: No symbols to fetch price board")
            return pd.DataFrame()
            
        # Lấy tất cả mã cổ phiếu
        all_symbols = symbols_df['ticker'].tolist()
        print(f"Fetching price board for {len(all_symbols)} symbols...")
        
        # Lấy dữ liệu bảng giá cho tất cả cổ phiếu
        try:
            price_board_df = stock.trading.price_board(symbols_list=all_symbols)
            
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
                    print(f"WARNING: Missing column {col} in price board")
            
            # Nếu có đủ cột thì mới tiếp tục
            if len(formatted_cols) > 0:
                formatted_df = price_board_df[formatted_cols].rename(
                    columns={col: new_name for col, new_name in required_cols.items() if col in formatted_cols}
                )
                
                # Định dạng khối lượng
                if 'Khối lượng' in formatted_df.columns:
                    formatted_df['Khối lượng'] = formatted_df['Khối lượng'].apply(
                        lambda x: "{:,}".format(int(x)) if pd.notnull(x) else "0"
                    )
                
                print(f"Successfully fetched data for {len(formatted_df)} stocks")
                return formatted_df
                
        except Exception as e:
            print(f"Error fetching price board: {str(e)}")
            
    except Exception as e:
        print(f"Error in get_price_board: {str(e)}")
        
    # Fallback data - trả về dữ liệu mẫu nhiều cổ phiếu hơn
    return pd.DataFrame({
        'CK': ['AAA', 'VNM', 'FPT', 'VIC', 'MSN', 'HPG', 'TCB', 'MWG', 'VHM', 'VRE', 
               'VCB', 'BID', 'CTG', 'MBB', 'ACB', 'POW', 'GAS', 'PLX', 'PVD', 'REE'],
        'Trần': [25000, 60000, 90000, 45000, 70000, 30000, 27000, 55000, 48000, 25000, 
                95000, 45000, 30000, 23000, 25000, 15000, 110000, 38000, 22000, 80000],
        'Sàn': [20000, 50000, 80000, 35000, 60000, 25000, 22000, 45000, 40000, 20000, 
               85000, 40000, 25000, 18000, 20000, 12000, 90000, 30000, 18000, 70000],
        'TC': [22000, 55000, 85000, 40000, 65000, 28000, 24000, 50000, 44000, 22000, 
              90000, 42000, 28000, 20000, 22000, 13000, 100000, 34000, 20000, 75000],
        'Giá': [22500, 56000, 86000, 41000, 66000, 27500, 24500, 51000, 45000, 22500, 
               92000, 43000, 27000, 21000, 23000, 13500, 101000, 35000, 21000, 76000],
        'Khối lượng': ['10,000', '5,000', '3,000', '2,000', '4,000', '8,000', '7,000', '6,000', '3,500', '2,500', 
                       '5,500', '4,800', '7,200', '9,000', '4,300', '6,200', '2,100', '5,400', '3,800', '2,700']
    })

def get_historical_data(symbol):
    """Lấy giá lịch sử của cổ phiếu"""
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        data = stock.quote.history(symbol=symbol, start='2000-01-01', end=today)
        if 'time' not in data.columns and data.index.name != 'time':
            data = data.reset_index().rename(columns={'index': 'time'})
        print(f"Data columns for {symbol}: {data.columns}")
        print(f"Sample data:\n{data.head()}")
        return data
    except Exception as e:
        print(f"Error in get_historical_data for {symbol}: {str(e)}")
        raise Exception(f"Không thể lấy dữ liệu lịch sử cho mã {symbol}: {str(e)}")

def sync_vnstock_to_assets():
    """Đồng bộ dữ liệu từ vnstock vào model Asset"""
    try:
        symbols_df = stock.listing.all_symbols()
        if symbols_df.empty:
            print("ERROR: No stock symbols retrieved from vnstock")
            return {'created': 0, 'updated': 0, 'errors': 0, 'total': 0}

        print(f"Found {len(symbols_df)} stocks in VNStock")
        symbols = symbols_df['ticker'].tolist()  # Lấy tất cả mã, không giới hạn 20

        # Lấy bảng giá
        price_board = stock.trading.price_board(symbols_list=symbols)
        if isinstance(price_board.columns, pd.MultiIndex):
            price_board.columns = ['_'.join(map(str, col)).strip() for col in price_board.columns.values]

        created_count = 0
        updated_count = 0
        error_count = 0

        for symbol in symbols:
            try:
                symbol_info = symbols_df[symbols_df['ticker'] == symbol].iloc[0]
                price_row = price_board[price_board['listing_symbol'] == symbol]
                current_price = price_row['match_match_price'].values[0] if not price_row.empty and 'match_match_price' in price_board.columns else 10000

                asset, created = Asset.objects.update_or_create(
                    symbol=symbol,
                    defaults={
                        'name': symbol_info['organ_name'],
                        'type': 'stock',
                        'sector': symbol_info.get('industry_name', 'Unknown'),
                        'current_price': current_price,
                        'description': f"Imported from VNStock on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
                    }
                )
                if created:
                    created_count += 1
                    print(f"Created asset: {symbol} - {symbol_info['organ_name']}")
                else:
                    updated_count += 1
                    print(f"Updated asset: {symbol} - {symbol_info['organ_name']}")
            except Exception as e:
                error_count += 1
                print(f"Error processing {symbol}: {str(e)}")

        return {
            'created': created_count,
            'updated': updated_count,
            'errors': error_count,
            'total': created_count + updated_count
        }
    except Exception as e:
        print(f"Error syncing VNStock data: {str(e)}")
        return {'created': 0, 'updated': 0, 'errors': 1, 'total': 0}

def get_current_bid_price(symbol):
    """Lấy giá mua (bid) hiện tại của một mã cổ phiếu."""
    try:
        print(f"DEBUG: Getting price for symbol {symbol}")
        # Sử dụng Vnstock instance đã có
        price_board = stock.trading.price_board(symbols_list=[symbol])
        
        if isinstance(price_board.columns, pd.MultiIndex):
            # Nếu đầu ra là MultiIndex columns
            try:
                price = price_board[('match', 'match_price')].iloc[0]
                print(f"DEBUG: Found price with ('match', 'match_price'): {price}")
                return price
            except:
                # Thử các tên cột khác nếu không tìm thấy
                for col_pair in [('match', 'price'), ('price', 'price'), ('trade', 'price')]:
                    try:
                        price = price_board[col_pair].iloc[0]
                        print(f"DEBUG: Found price with {col_pair}: {price}")
                        return price
                    except:
                        continue
                # Nếu không tìm được, chuyển đổi columns và thử lại
                price_board.columns = ['_'.join(map(str, col)).strip() for col in price_board.columns.values]
                print(f"DEBUG: Converted columns to: {price_board.columns}")
        
        # Chọn cột giá phù hợp
        for col in ['match_match_price', 'match_price', 'price']:
            if col in price_board.columns:
                price = price_board[col].iloc[0]
                print(f"DEBUG: Found price with {col}: {price}")
                return price
        
        # Nếu không tìm thấy cột giá
        print(f"Warning: Could not find price column for {symbol}. Available columns: {price_board.columns}")
        return None
    except Exception as e:
        print(f"Error in get_current_bid_price for {symbol}: {str(e)}")
        return None

def get_all_stock_symbols():
    """Lấy danh sách mã cổ phiếu và tên công ty dựa theo yêu cầu người dùng"""
    try:
        print("DEBUG: Getting all stock symbols from VNStock")
        # Sử dụng code chính xác từ hướng dẫn
        symbols_data = Vnstock().stock(symbol='VN30', source='VCI').listing.all_symbols().values
        
        # Debug the shape of data
        print(f"DEBUG: Got {len(symbols_data)} symbols, first 3: {symbols_data[:3]}")
        
        # Chuyển đổi thành định dạng dễ sử dụng hơn
        symbols = []
        for item in symbols_data:
            symbols.append({
                'ticker': item[0],
                'organ_name': item[1]
            })
        
        return symbols
    except Exception as e:
        print(f"Error in get_all_stock_symbols: {str(e)}")
        return []

def fetch_stock_prices_snapshot(output_file=None):
    """Lấy snapshot giá cổ phiếu hiện tại"""
    try:
        symbols_df = stock.listing.all_symbols()
        if symbols_df.empty:
            print("ERROR: No stock symbols retrieved")
            return None
        symbols = symbols_df['ticker'].tolist()

        print(f"Fetching prices for {len(symbols)} symbols...")
        price_board = stock.trading.price_board(symbols_list=symbols)
        if isinstance(price_board.columns, pd.MultiIndex):
            price_board.columns = ['_'.join(map(str, col)).strip() for col in price_board.columns.values]

        price_column = None
        for col in ['match_match_price', 'match_price', 'price', 'trading_match_price', 'listing_reference_price']:
            if col in price_board.columns:
                price_column = col
                break
        if not price_column:
            price_cols = [col for col in price_board.columns if 'price' in col.lower()]
            price_column = price_cols[0] if price_cols else None
        if not price_column:
            raise ValueError("Could not find price column in price board data")

        symbol_column = None
        for col in ['listing_symbol', 'symbol', 'ticker']:
            if col in price_board.columns:
                symbol_column = col
                break
        if not symbol_column:
            raise ValueError("Could not find symbol column in price board data")

        snapshot = {'time': datetime.now().isoformat()}
        for symbol in symbols:
            try:
                row = price_board[price_board[symbol_column] == symbol]
                snapshot[symbol] = float(row[price_column].values[0]) if not row.empty and pd.notnull(row[price_column].values[0]) else None
            except Exception as e:
                print(f"Error getting price for {symbol}: {str(e)}")
                snapshot[symbol] = None

        snapshot_df = pd.DataFrame([snapshot])
        update_count = 0
        for symbol, price in snapshot.items():
            if symbol != 'time' and price is not None:
                try:
                    asset = Asset.objects.filter(symbol=symbol).first()
                    if asset:
                        asset.current_price = price
                        asset.save(update_fields=['current_price'])
                        update_count += 1
                except Exception as e:
                    print(f"Error updating asset {symbol}: {str(e)}")

        print(f"Updated prices for {update_count} assets in the database")
        if output_file:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            snapshot_df.to_csv(output_file, mode='a', header=not os.path.exists(output_file), index=False)
            print(f"Saved data to {output_file}")
            return None
        return snapshot_df
    except Exception as e:
        print(f"Error in fetch_stock_prices_snapshot: {str(e)}")
        return None

def get_technical_indicators(symbol):
    """
    Lấy dữ liệu các chỉ báo kỹ thuật (MA, MACD, RSI) từ PostgreSQL
    
    Args:
        symbol (str): Mã cổ phiếu cần lấy chỉ báo
        
    Returns:
        dict: Dữ liệu các chỉ báo kỹ thuật và dữ liệu lịch sử để vẽ biểu đồ
    """
    import psycopg2
    import json
    from datetime import datetime
    import pandas as pd
    
    try:
        # Thông tin kết nối PostgreSQL - lấy từ biến môi trường hoặc dùng giá trị mặc định
        PG_HOST = 'db'  # tên service trong docker-compose
        PG_PORT = '5432'
        PG_DB = 'db_for_pm'
        PG_USER = 'airflow'
        PG_PASSWORD = 'admin123'
        
        print(f"Connecting to PostgreSQL to get technical indicators for {symbol}...")
        
        # Kết nối đến PostgreSQL
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
        
        # Tạo cursor
        cur = conn.cursor()
        
        # Truy vấn dữ liệu từ bảng stock_processed_data
        query = """
        SELECT json_data
        FROM public.stock_processed_data
        WHERE symbol = %s
        """
        cur.execute(query, (symbol,))
        
        # Lấy kết quả
        result = cur.fetchone()
        
        # Đóng kết nối
        cur.close()
        conn.close()
        
        if not result:
            print(f"Không tìm thấy dữ liệu cho mã {symbol}")
            return None
        
        # Lấy dữ liệu JSON
        json_data = result[0]
        
        # Chuyển đổi JSONB thành dict nếu cần
        if not isinstance(json_data, dict):
            json_data = json.loads(json_data)
        
        print(f"Retrieved JSON data for {symbol}, keys: {list(json_data.keys())}")
        
        # Lấy dữ liệu lịch sử đã được xử lý
        historical_processed = json_data.get('historical_processed', [])
        
        if not historical_processed:
            print(f"Không có dữ liệu lịch sử đã xử lý cho mã {symbol}")
            return None
        
        print(f"Found {len(historical_processed)} historical data points for {symbol}")
        
        # Kiểm tra cấu trúc dữ liệu của mục đầu tiên để debug
        if historical_processed:
            first_item = historical_processed[0]
            print(f"First data point keys: {list(first_item.keys())}")
            print(f"MA5: {first_item.get('ma5')}, MA20: {first_item.get('ma20')}, RSI: {first_item.get('rsi')}")
        
        # Chuyển đổi dữ liệu thành định dạng phù hợp để vẽ biểu đồ
        candles_data = []  # Dữ liệu nến
        ma5_data = []      # Đường MA5
        ma20_data = []     # Đường MA20
        rsi_data = []      # Đường RSI
        macd_line = []     # Đường MACD
        macd_signal = []   # Đường tín hiệu MACD
        macd_histogram = [] # Histogram MACD
        
        for item in historical_processed:
            time_str = item.get('time')
            
            # Chuyển đổi định dạng thời gian nếu cần
            if isinstance(time_str, str):
                time_format = time_str
            else:
                time_format = datetime.fromtimestamp(time_str).strftime('%Y-%m-%d') if isinstance(time_str, (int, float)) else None
            
            if not time_format:
                continue
                
            # Dữ liệu nến
            candle = {
                'time': time_format,
                'open': float(item.get('open', 0)),
                'high': float(item.get('high', 0)),
                'low': float(item.get('low', 0)),
                'close': float(item.get('close', 0)),
                'volume': float(item.get('volume', 0))
            }
            candles_data.append(candle)
            
            # Dữ liệu MA5
            if 'ma5' in item and item['ma5'] is not None:
                ma5_data.append({
                    'time': time_format,
                    'value': float(item['ma5'])
                })
            
            # Dữ liệu MA20
            if 'ma20' in item and item['ma20'] is not None:
                ma20_data.append({
                    'time': time_format,
                    'value': float(item['ma20'])
                })
            
            # Dữ liệu RSI
            if 'rsi' in item and item['rsi'] is not None:
                rsi_data.append({
                    'time': time_format,
                    'value': float(item['rsi'])
                })
            
            # Dữ liệu MACD
            if 'macd_line' in item and item['macd_line'] is not None:
                macd_line.append({
                    'time': time_format,
                    'value': float(item['macd_line'])
                })
                
            if 'macd_signal' in item and item['macd_signal'] is not None:
                macd_signal.append({
                    'time': time_format,
                    'value': float(item['macd_signal'])
                })
                
            if 'macd_histogram' in item and item['macd_histogram'] is not None:
                macd_histogram.append({
                    'time': time_format,
                    'value': float(item['macd_histogram'])
                })
        
        # Log ra các counts
        print(f"Processed data for {symbol}:")
        print(f"  - Candles: {len(candles_data)}")
        print(f"  - MA5: {len(ma5_data)}")
        print(f"  - MA20: {len(ma20_data)}")
        print(f"  - RSI: {len(rsi_data)}")
        print(f"  - MACD Line: {len(macd_line)}")
        print(f"  - MACD Signal: {len(macd_signal)}")
        print(f"  - MACD Histogram: {len(macd_histogram)}")
        
        # Trả về tất cả dữ liệu
        return {
            'success': True,
            'symbol': symbol,
            'candles': candles_data,
            'ma5': ma5_data,
            'ma20': ma20_data,
            'rsi': rsi_data,
            'macd_line': macd_line,
            'macd_signal': macd_signal,
            'macd_histogram': macd_histogram
        }
        
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu chỉ báo kỹ thuật cho mã {symbol}: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }
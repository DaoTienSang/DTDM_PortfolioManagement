from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt # Cần cho ai_chat_api nếu cũng chuyển sang đây, nhưng hiện tại chưa
from django.views.decorators.http import require_POST # Tương tự csrf_exempt
import json
import logging
import random
from datetime import datetime
import pandas as pd # Cần cho get_price_board, get_historical_data_api
import numpy as np # Cần cho NanHandlingJSONEncoder nếu dùng

from .models import Asset # Cần cho get_stock_symbols (fallback), get_stock_price, create_asset_from_symbol
from .vnstock_services import get_price_board, get_historical_data, get_all_stock_symbols, get_current_bid_price, get_technical_indicators
from .postgres_chart_service import get_postgres_chart_service

logger = logging.getLogger(__name__)

# Custom JSON encoder to handle NaN values (nếu cần, có thể copy từ views.py gốc)
class NanHandlingJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, float) and (np.isnan(obj) or pd.isna(obj)):
            return None
        return super().default(obj)

@login_required
def market(request):
    # Get price board data or use fallback data if there's an error
    try:
        price_board = get_price_board()
        if price_board.empty:
            raise ValueError("Empty price board returned")
        
        # Convert to JSON in 'records' orientation for better frontend handling
        price_board_json = price_board.to_json(orient='records')
        print("Successfully fetched price board data")
    except Exception as e:
        print(f"Error in market view: {str(e)}")
        # Simple fallback data with explicit structure
        fallback_data = [
            {"CK": "AAA", "Trần": 25000, "Sàn": 20000, "TC": 22000, "Giá": 22500, "Khối lượng": "10,000"},
            {"CK": "VNM", "Trần": 60000, "Sàn": 50000, "TC": 55000, "Giá": 56000, "Khối lượng": "5,000"},
            {"CK": "FPT", "Trần": 90000, "Sàn": 80000, "TC": 85000, "Giá": 86000, "Khối lượng": "3,000"},
            {"CK": "VIC", "Trần": 45000, "Sàn": 35000, "TC": 40000, "Giá": 41000, "Khối lượng": "2,000"},
            {"CK": "MSN", "Trần": 70000, "Sàn": 60000, "TC": 65000, "Giá": 66000, "Khối lượng": "4,000"}
        ]
        price_board_json = json.dumps(fallback_data)
        messages.warning(request, "Không thể tải dữ liệu thị trường thực. Hiển thị dữ liệu mẫu.")
    
    # Log data for debugging
    print(f"Data structure sent to template: {price_board_json[:100]}...")
    
    context = {
        "price_board_json": price_board_json,
    }
    return render(request, 'portfolio/market.html', context)

# Hàm get_historical_data_api gốc đã bị comment, thay bằng get_stock_historical_data
# def get_historical_data_api(request, stock_code):
#     historical_data = get_historical_data(stock_code)
#     return JsonResponse(historical_data.to_dict(orient='records'), safe=False)

def get_stock_historical_data(request, symbol):
    """Lấy dữ liệu lịch sử giá cổ phiếu cho biểu đồ"""
    try:
        symbol = symbol.upper()
        logger.info(f"Lấy dữ liệu lịch sử cho mã {symbol}")
        
        postgres_service = get_postgres_chart_service()
        technical_data = get_technical_indicators(symbol) # Hàm này đã bao gồm lấy dữ liệu từ PG và tính toán
        
        if technical_data and technical_data.get('success', False):
            chart_data = technical_data.get('candles', [])
            logger.info(f"Trả về dữ liệu biểu đồ cho mã {symbol}: {len(chart_data)} nến (từ get_technical_indicators)")
            return JsonResponse({
                'candles': chart_data,
                'ma5': technical_data.get('ma5', []),
                'ma20': technical_data.get('ma20', []),
                'rsi': technical_data.get('rsi', []),
                'macd_line': technical_data.get('macd_line', []),
                'macd_signal': technical_data.get('macd_signal', []),
                'macd_histogram': technical_data.get('macd_histogram', [])
            }, safe=False)
        
        # Fallback nếu get_technical_indicators không thành công
        logger.warning(f"get_technical_indicators không thành công cho {symbol}, thử lấy trực tiếp từ PG.")
        stock_data = postgres_service.get_stock_data(symbol)
        
        if stock_data is None or not stock_data:
            logger.warning(f"Không tìm thấy dữ liệu cho mã {symbol} trong PostgreSQL, thử vnstock trực tiếp.")
            historical_data_df = get_historical_data(symbol) # Hàm này trả về DataFrame
            if historical_data_df.empty:
                logger.error(f"Không có dữ liệu lịch sử từ vnstock cho {symbol}")
                return JsonResponse({'error': f'Không có dữ liệu lịch sử cho {symbol}'}, status=404)

            chart_data = []
            for _, row in historical_data_df.iterrows():
                chart_data.append({
                    'time': row['time'].strftime('%Y-%m-%d') if hasattr(row['time'], 'strftime') else str(row['time']),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close'])
                })
            return JsonResponse(chart_data, safe=False)
        else:
            formatted_data = postgres_service.format_for_chart(stock_data)
            if not formatted_data or not formatted_data.get('candles'):
                logger.warning(f"Dữ liệu biểu đồ từ PostgreSQL không hợp lệ cho mã {symbol}")
                return JsonResponse({'error': 'Dữ liệu biểu đồ không hợp lệ'}, status=500)
            logger.info(f"Trả về dữ liệu biểu đồ cho mã {symbol} từ postgres_service (fallback)")
            return JsonResponse(formatted_data, safe=False)
        
    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu lịch sử cho {symbol}: {str(e)}")
        return JsonResponse({'error': str(e)}, status=500)

def get_technical_indicators_api(request, symbol):
    """API endpoint để lấy các chỉ báo kỹ thuật cho biểu đồ"""
    try:
        symbol = symbol.upper()
        technical_data = get_technical_indicators(symbol) # Hàm này lấy từ vnstock_services
        
        if not technical_data or not technical_data.get('success', False):
            return JsonResponse({
                'success': False,
                'error': 'Không tìm thấy dữ liệu chỉ báo kỹ thuật'
            }, status=404)
        
        return JsonResponse(technical_data)
        
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@login_required
def get_stock_symbols(request):
    """API view để lấy danh sách mã cổ phiếu phù hợp với từ khóa tìm kiếm"""
    term = request.GET.get('term', '')
    try:
        all_symbols_list = get_all_stock_symbols() # từ vnstock_services
        filtered_symbols = [item['ticker'] for item in all_symbols_list 
                           if term.upper() in item['ticker'].upper()][:10]
        if len(filtered_symbols) < 10 and term:
            remaining_slots = 10 - len(filtered_symbols)
            company_matches = [item['ticker'] for item in all_symbols_list
                              if item['ticker'] not in filtered_symbols 
                              and term.upper() in item['organ_name'].upper()][:remaining_slots]
            filtered_symbols.extend(company_matches)
        return JsonResponse(filtered_symbols, safe=False)
    except Exception as e:
        logger.error(f"Error in get_stock_symbols: {str(e)}")
        assets = Asset.objects.filter(symbol__istartswith=term)[:10]
        symbols = [asset.symbol for asset in assets]
        return JsonResponse(symbols, safe=False)

@login_required
def get_stock_symbols_info(request):
    """API view để lấy thông tin tên công ty cho các mã cổ phiếu"""
    if request.method == 'POST':
        try:
            symbols_json = request.POST.get('symbols', '[]')
            symbols = json.loads(symbols_json)
            if not symbols:
                return JsonResponse({}, safe=False)
            
            all_symbols_list = get_all_stock_symbols() # từ vnstock_services
            symbol_info_dict = {item['ticker']: item['organ_name'] for item in all_symbols_list
                                if item['ticker'] in symbols}
            
            missing_symbols = [s for s in symbols if s not in symbol_info_dict]
            if missing_symbols:
                assets_db = Asset.objects.filter(symbol__in=missing_symbols)
                for asset_db in assets_db:
                    symbol_info_dict[asset_db.symbol] = asset_db.name
            return JsonResponse(symbol_info_dict, safe=False)
        except Exception as e:
            logger.error(f"Error fetching company names in get_stock_symbols_info: {str(e)}")
            return JsonResponse({'error': str(e)}, status=400)
    return JsonResponse({'error': 'Method not allowed'}, status=405)

@login_required
def get_stock_price(request, symbol):
    """API endpoint để lấy giá cổ phiếu hiện tại và giá gợi ý mua/bán."""
    try:
        asset = Asset.objects.filter(symbol=symbol).first()
        current_bid = get_current_bid_price(symbol) # từ vnstock_services
        
        if current_bid is not None:
            match_price = float(current_bid)
            if not asset:
                logger.info(f"Asset {symbol} not found in DB, creating new one.")
                all_symbols_list = get_all_stock_symbols()
                company_info = next((item for item in all_symbols_list if item['ticker'] == symbol), None)
                company_name = company_info.get('organ_name', f"Cổ phiếu {symbol}") if company_info else f"Cổ phiếu {symbol}"
                
                asset = Asset(
                    symbol=symbol, name=company_name, type='stock', sector='Unknown',
                    description=f"Auto-created from VNStock on {datetime.now().strftime('%Y-%m-%d')}",
                    current_price=match_price
                )
                asset.save()
                logger.info(f"Created new asset: {asset.id} - {asset.symbol}")
            elif asset:
                asset.current_price = match_price
                asset.save(update_fields=['current_price', 'last_updated'])
                logger.info(f"Updated price for {symbol} to {match_price}")

            change_percent = random.uniform(-2, 2)
            buy_price = round(match_price * 0.995)
            sell_price = round(match_price * 1.005)
            
            response_data = {
                'success': True, 'symbol': symbol, 'price': match_price, 'change_percent': change_percent,
                'buy_price': buy_price, 'sell_price': sell_price, 'source': 'vnstock',
                'asset_exists': True, 'asset_id': asset.id, 'asset_name': asset.name
            }
            return JsonResponse(response_data)
        else: # current_bid is None
            logger.warning(f"Could not fetch current_bid for {symbol} from vnstock.")
            if asset:
                match_price = float(asset.current_price)
                change_percent = random.uniform(-2, 2) # Giữ lại logic cũ
                buy_price = round(match_price * 0.995)
                sell_price = round(match_price * 1.005)
                response_data = {
                    'success': True, 'symbol': symbol, 'price': match_price, 'change_percent': change_percent,
                    'buy_price': buy_price, 'sell_price': sell_price, 'source': 'database',
                    'asset_exists': True, 'asset_id': asset.id, 'asset_name': asset.name
                }
                return JsonResponse(response_data)
            else:
                logger.error(f"Asset {symbol} not in DB and vnstock fetch failed.")
                # Không tạo asset mới ở đây nếu API không lấy được giá, tránh tạo asset với giá lỗi
                return JsonResponse({'success': False, 'error': f'Không tìm thấy mã cổ phiếu {symbol} và không thể lấy giá từ API'}, status=404)
                
    except Exception as e:
        logger.error(f"ERROR in get_stock_price for {symbol}: {str(e)}")
        return JsonResponse({'success': False, 'error': f'Lỗi khi xử lý thông tin cổ phiếu: {str(e)}'}, status=500)

@login_required
def create_asset_from_symbol(request):
    """API endpoint để tạo mới hoặc cập nhật asset từ mã cổ phiếu"""
    if request.method == 'POST':
        try:
            symbol = request.POST.get('symbol')
            if not symbol:
                return JsonResponse({'success': False, 'error': 'Thiếu mã cổ phiếu'}, status=400)
            
            asset = Asset.objects.filter(symbol=symbol).first()
            current_price_api = get_current_bid_price(symbol) # từ vnstock_services

            if asset:
                if current_price_api is not None:
                    asset.current_price = float(current_price_api)
                    asset.save(update_fields=['current_price', 'last_updated'])
                    msg = f'Đã cập nhật giá cho {symbol}'
                    updated = True
                else:
                    msg = f'Không lấy được giá mới cho {symbol}, giữ nguyên giá cũ'
                    updated = False
                return JsonResponse({
                    'success': True, 'message': msg, 'asset_id': asset.id,
                    'asset_name': asset.name, 'price': float(asset.current_price),
                    'created': False, 'updated': updated
                })
            else: # Asset không tồn tại, tạo mới
                if current_price_api is None:
                    logger.warning(f"Cannot create new asset {symbol} as API price is None.")
                    return JsonResponse({'success': False, 'error': f'Không thể lấy giá từ API để tạo mới {symbol}'}, status=400)

                all_symbols_list = get_all_stock_symbols()
                company_info = next((item for item in all_symbols_list if item['ticker'] == symbol), None)
                company_name = company_info.get('organ_name', f"Cổ phiếu {symbol}") if company_info else f"Cổ phiếu {symbol}"
                
                new_asset = Asset(
                    symbol=symbol, name=company_name, type='stock', sector='Unknown',
                    description=f"Auto-created via API on {datetime.now().strftime('%Y-%m-%d')}",
                    current_price=float(current_price_api)
                )
                new_asset.save()
                return JsonResponse({
                    'success': True, 'message': f'Đã tạo mới cổ phiếu {symbol}',
                    'asset_id': new_asset.id, 'asset_name': new_asset.name,
                    'price': float(new_asset.current_price), 'created': True, 'updated': False
                })
        except Exception as e:
            logger.error(f"Lỗi trong create_asset_from_symbol: {str(e)}")
            return JsonResponse({'success': False, 'error': f'Lỗi: {str(e)}'}, status=500)
    return JsonResponse({'success': False, 'error': 'Chỉ hỗ trợ phương thức POST'}, status=405)

# Hàm refresh_kafka_symbol đã bị xóa
# def refresh_kafka_symbol(request, symbol): ...

# --- Views liên quan đến debug và trang market tĩnh ĐÃ BỊ XÓA --- 
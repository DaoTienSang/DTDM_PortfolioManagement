from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.core.paginator import Paginator
from django.db.models import Q
from django.utils import timezone
from .models import Portfolio, Asset, Transaction, PortfolioAsset, User, Wallet
from .forms import PortfolioForm, AssetForm, TransactionForm, UserRegistrationForm, UserProfileForm
from django.contrib.auth import login, logout, authenticate
from decimal import Decimal
from django.http import JsonResponse, HttpResponse
from .vnstock_services import get_price_board, get_historical_data, sync_vnstock_to_assets, fetch_stock_prices_snapshot, get_all_stock_symbols, get_current_bid_price
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from .utils import get_ai_response, get_auth0_user_profile
from django.urls import reverse
from urllib.parse import quote_plus, urlencode
import os
from django.conf import settings
from vnstock import Vnstock
import json
import pandas as pd
import numpy as np
import random
from django.shortcuts import render
from datetime import datetime
from .postgres_chart_service import get_postgres_chart_service
import logging

# Thiết lập logging
logger = logging.getLogger(__name__)

# Comment out OAuth setup temporarily
"""
oauth = OAuth()
oauth.register(
    "auth0",
    client_id=settings.AUTH0_CLIENT_ID,
    client_secret=settings.AUTH0_CLIENT_SECRET,
    client_kwargs={"scope": "openid profile email"},
    server_metadata_url=f"https://{settings.AUTH0_DOMAIN}/.well-known/openid-configuration",
)
"""

# Khởi tạo Kafka service
# kafka_service = StockKafkaService()

# Custom JSON encoder to handle NaN values
class NanHandlingJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, float) and (np.isnan(obj) or pd.isna(obj)):
            return None
        return super().default(obj)

def home(request):
    return render(request, 'portfolio/home.html')

@login_required
def dashboard(request):
    portfolios = Portfolio.objects.filter(user=request.user)
    total_value = sum(p.total_value for p in portfolios)
    total_cost = sum(p.total_cost for p in portfolios)
    total_profit = ((total_value - total_cost) / total_cost * 100) if total_cost > 0 else 0
    
    total_assets = PortfolioAsset.objects.filter(
        portfolio__user=request.user,
        quantity__gt=0
    ).count()
    
    monthly_transactions = Transaction.objects.filter(
        portfolio__user=request.user,
        transaction_date__gte=timezone.now() - timezone.timedelta(days=30)
    ).count()
    
    recent_transactions = Transaction.objects.filter(
        portfolio__user=request.user
    ).order_by('-transaction_date')[:5]
    
    # Get wallet for displaying balance on dashboard
    wallet, created = Wallet.objects.get_or_create(user=request.user)

    context = {
        'portfolios': portfolios,
        'total_value': total_value,
        'total_profit': total_profit,
        'total_assets': total_assets,
        'monthly_transactions': monthly_transactions,
        'recent_transactions': recent_transactions,
        'wallet': wallet,
    }
    return render(request, 'portfolio/dashboard.html', context)

@login_required
def portfolio_list(request):
    portfolios = Portfolio.objects.filter(user=request.user)
    return render(request, 'portfolio/portfolio_list.html', {'portfolios': portfolios})

@login_required
def portfolio_create(request):
    if request.method == 'POST':
        form = PortfolioForm(request.POST)
        if form.is_valid():
            portfolio = form.save(commit=False)
            portfolio.user = request.user
            portfolio.save()
            
            messages.success(request, 'Danh mục đầu tư đã được tạo thành công!')
            
            # Chuyển hướng đến trang chi tiết danh mục vừa tạo
            return redirect('portfolio_detail', pk=portfolio.pk)
    else:
        form = PortfolioForm()
    
    return render(request, 'portfolio/portfolio_form.html', {
        'form': form,
        'title': 'Tạo danh mục mới'
    })

@login_required
def portfolio_detail(request, pk):
    portfolio = get_object_or_404(Portfolio, pk=pk, user=request.user)
    
    # Lấy danh sách tài sản trong danh mục
    portfolio_assets = PortfolioAsset.objects.filter(
        portfolio=portfolio,
        quantity__gt=0
    ).select_related('asset')
    
    # Lấy các giao dịch gần đây
    recent_transactions = Transaction.objects.filter(
        portfolio=portfolio
    ).select_related('asset').order_by('-transaction_date')[:5]
    
    # Tính toán thống kê
    total_invested = sum(pa.quantity * pa.average_price for pa in portfolio_assets)
    total_current_value = sum(pa.current_value for pa in portfolio_assets)
    total_profit_loss = total_current_value - total_invested
    profit_percentage = (total_profit_loss / total_invested * 100) if total_invested > 0 else 0
    
    # Phân bổ tài sản theo loại
    asset_allocation = {}
    for pa in portfolio_assets:
        asset_type = pa.asset.get_type_display()
        asset_allocation[asset_type] = asset_allocation.get(asset_type, 0) + pa.current_value
    
    context = {
        'portfolio': portfolio,
        'portfolio_assets': portfolio_assets,
        'recent_transactions': recent_transactions,
        'total_invested': total_invested,
        'total_current_value': total_current_value,
        'total_profit_loss': total_profit_loss,
        'profit_percentage': profit_percentage,
        'asset_allocation': asset_allocation,
    }
    
    return render(request, 'portfolio/portfolio_detail.html', context)

@login_required
def portfolio_update(request, pk):
    portfolio = get_object_or_404(Portfolio, pk=pk, user=request.user)
    if request.method == 'POST':
        form = PortfolioForm(request.POST, instance=portfolio)
        if form.is_valid():
            form.save()
            messages.success(request, 'Danh mục đầu tư đã được cập nhật!')
            return redirect('portfolio_detail', pk=pk)
    else:
        form = PortfolioForm(instance=portfolio)
    return render(request, 'portfolio/portfolio_form.html', {
        'form': form,
        'title': 'Chỉnh sửa danh mục'
    })

@login_required
def asset_list(request):
    # Get portfolio assets that user actually owns (quantity > 0)
    portfolio_assets = PortfolioAsset.objects.filter(
        portfolio__user=request.user,
        quantity__gt=0
    ).select_related('asset')
    
    # Get unique assets from portfolio assets
    owned_asset_ids = portfolio_assets.values_list('asset_id', flat=True).distinct()
    owned_assets = Asset.objects.filter(id__in=owned_asset_ids)
    
    # Calculate total values and add portfolio-related attributes to each asset
    assets_with_data = []
    
    for asset in owned_assets:
        # Get portfolio asset data for this asset across all portfolios
        asset_portfolio_items = portfolio_assets.filter(asset=asset)
        
        # Sum quantities and calculate weighted average price
        total_quantity = sum(pa.quantity for pa in asset_portfolio_items)
        total_cost = sum(pa.quantity * pa.average_price for pa in asset_portfolio_items)
        average_price = total_cost / total_quantity if total_quantity > 0 else 0
        
        # Calculate current value and profit/loss
        current_value = total_quantity * asset.current_price
        profit_loss = current_value - total_cost
        profit_loss_percent = (profit_loss / total_cost * 100) if total_cost > 0 else 0
        
        # Add data to asset
        asset.quantity = total_quantity
        asset.average_price = average_price
        asset.total_value = current_value
        asset.profit_loss = profit_loss
        asset.profit_loss_percent = profit_loss_percent
        
        assets_with_data.append(asset)
    
    # Calculate portfolio totals
    total_investment = sum(asset.quantity * asset.average_price for asset in assets_with_data)
    total_current_value = sum(asset.quantity * asset.current_price for asset in assets_with_data)
    print(f"DEBUG: Total investment: {total_investment}, Total current value: {total_current_value}")
    total_profit_loss = total_current_value - total_investment
    total_profit_loss_percent = (total_profit_loss / total_investment * 100) if total_investment > 0 else 0
    
    # Fetch stock data for debug table
    try:
        # Initialize VNStock instance
        vnstock_instance = Vnstock()
        stock = vnstock_instance.stock(symbol='VN30', source='VCI')
        
        # Get stock list and price board
        all_symbols_df = stock.listing.all_symbols()
        symbols_list = all_symbols_df['ticker'].tolist()[:5]  # Get first 5 stocks
        price_board = stock.trading.price_board(symbols_list=symbols_list)
        
        # Convert to list of dictionaries for template
        debug_stocks = []
        for symbol in symbols_list:
            try:
                symbol_info = all_symbols_df[all_symbols_df['ticker'] == symbol].iloc[0]
                price_info = price_board[price_board['listing_symbol'] == symbol].iloc[0]
                
                # Extract needed info
                stock_data = {
                    'ticker': symbol,
                    'organ_name': symbol_info['organ_name'],
                    'price': price_info['match_match_price'] if 'match_match_price' in price_info else None,
                    'change': price_info['match_change_percent'] if 'match_change_percent' in price_info else None,
                    'volume': price_info['match_match_qtty'] if 'match_match_qtty' in price_info else None
                }
                debug_stocks.append(stock_data)
            except (IndexError, KeyError):
                continue
    except Exception as e:
        # Provide mock data for debug stocks if API fails
        debug_stocks = [
            {
                'ticker': 'VNM',
                'organ_name': 'Công ty Cổ phần Sữa Việt Nam',
                'price': 75000,
                'change': 2.5,
                'volume': 1250000
            },
            {
                'ticker': 'FPT',
                'organ_name': 'Công ty Cổ phần FPT',
                'price': 92300,
                'change': 1.8,
                'volume': 980500
            },
            {
                'ticker': 'VIC',
                'organ_name': 'Tập đoàn Vingroup',
                'price': 48600,
                'change': -1.2,
                'volume': 1432000
            },
            {
                'ticker': 'VHM',
                'organ_name': 'Công ty Cổ phần Vinhomes',
                'price': 45800,
                'change': -0.7,
                'volume': 875000
            },
            {
                'ticker': 'MSN',
                'organ_name': 'Tập đoàn Masan',
                'price': 82500,
                'change': 3.2,
                'volume': 654300
            }
        ]
        print(f"Using mock data for debug stocks. Error was: {e}")
    
    # Get asset types for filter
    asset_types = Asset.ASSET_TYPES
    
    # Get unique sectors for filter
    sectors = Asset.objects.values_list('sector', flat=True).distinct()
    
    context = {
        'assets': assets_with_data,
        'debug_stocks': debug_stocks,
        'asset_types': asset_types,
        'sectors': sectors,
        'total_investment': total_investment,
        'total_current_value': total_current_value,
        'total_profit_loss': total_profit_loss,
        'total_profit_loss_percent': total_profit_loss_percent
    }
    
    return render(request, 'portfolio/asset_list.html', context)

@login_required
def asset_create(request):
    if request.method == 'POST':
        form = AssetForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, 'Tài sản đã được thêm thành công!')
            return redirect('asset_list')
    else:
        form = AssetForm()
    return render(request, 'portfolio/asset_form.html', {'form': form, 'title': 'Thêm tài sản mới'})

@login_required
def asset_detail(request, pk):
    asset = get_object_or_404(Asset, pk=pk)
    return render(request, 'portfolio/asset_detail.html', {'asset': asset})

@login_required
def asset_update(request, pk):
    asset = get_object_or_404(Asset, pk=pk)
    if request.method == 'POST':
        form = AssetForm(request.POST, instance=asset)
        if form.is_valid():
            form.save()
            messages.success(request, 'Tài sản đã được cập nhật!')
            return redirect('asset_detail', pk=pk)
    else:
        form = AssetForm(instance=asset)
    return render(request, 'portfolio/asset_form.html', {
        'form': form,
        'title': 'Chỉnh sửa tài sản'
    })

@login_required
def transaction_list(request):
    transactions = Transaction.objects.filter(portfolio__user=request.user)
    
    # Lọc theo danh mục
    portfolio_id = request.GET.get('portfolio')
    if portfolio_id:
        transactions = transactions.filter(portfolio_id=portfolio_id)
    
    # Lọc theo loại giao dịch
    transaction_type = request.GET.get('type')
    if transaction_type:
        transactions = transactions.filter(transaction_type=transaction_type)
    
    # Lọc theo ngày
    from_date = request.GET.get('from_date')
    if from_date:
        transactions = transactions.filter(transaction_date__gte=from_date)
    
    to_date = request.GET.get('to_date')
    if to_date:
        transactions = transactions.filter(transaction_date__lte=to_date)
    
    # Phân trang
    paginator = Paginator(transactions.order_by('-transaction_date'), 10)
    page = request.GET.get('page')
    transactions = paginator.get_page(page)
    
    context = {
        'transactions': transactions,
        'portfolios': Portfolio.objects.filter(user=request.user)
    }
    return render(request, 'portfolio/transaction_list.html', context)

@login_required
def transaction_create(request):
    if request.method == 'POST':
        form = TransactionForm(request.POST)
        if form.is_valid():
            transaction = form.save(commit=False)
            transaction.total_amount = transaction.quantity * transaction.price
            transaction.save()
            messages.success(request, 'Giao dịch đã được tạo thành công!')
            return redirect('transaction_list')
    else:
        form = TransactionForm()
    return render(request, 'portfolio/transaction_form.html', {
        'form': form,
        'title': 'Tạo giao dịch mới'
    })

@login_required
def buy_stock(request, portfolio_id):
    portfolio = get_object_or_404(Portfolio, pk=portfolio_id, user=request.user)
    
    # Lấy thông tin ví điện tử của người dùng
    wallet, created = Wallet.objects.get_or_create(user=request.user)
    
    # Get all assets for the autocomplete feature
    all_assets = Asset.objects.all()
    
    # Lấy ngày hiện tại cho trường ngày mặc định
    today_date = timezone.now().strftime('%Y-%m-%d')
    
    if request.method == 'POST':
        # Get symbol and convert to asset
        symbol = request.POST.get('symbol')
        
        # Debugging
        print(f"DEBUG BUY STOCK: symbol={symbol}, POST data: {request.POST}")
        
        # Direct method bypass form validation
        if symbol:
            try:
                # Find asset by symbol or create new if not exists
                try:
                    # Tìm kiếm asset
                    asset = Asset.objects.get(symbol=symbol)
                    print(f"DEBUG FOUND ASSET: {asset.id} - {asset.symbol} - {asset.name} - current price: {asset.current_price}")
                except Asset.DoesNotExist:
                    # Nếu không có, tạo mới asset từ VNStock API
                    print(f"DEBUG ASSET NOT FOUND: {symbol} - Creating new asset")
                    from .vnstock_services import get_current_bid_price, get_all_stock_symbols
                    
                    # Lấy giá hiện tại
                    current_price = get_current_bid_price(symbol)
                    if current_price is None:
                        current_price = float(request.POST.get('price', 10000))
                    
                    # Lấy thông tin công ty
                    company_name = f"Cổ phiếu {symbol}"
                    try:
                        all_symbols = get_all_stock_symbols()
                        company_info = next((item for item in all_symbols if item['ticker'] == symbol), None)
                        if company_info:
                            company_name = company_info.get('organ_name', company_name)
                    except Exception as e:
                        print(f"DEBUG ERROR getting company info: {str(e)}")
                    
                    # Tạo asset mới
                    asset = Asset(
                        symbol=symbol,
                        name=company_name,
                        type='stock',
                        sector='Unknown',
                        description=f"Auto-created during buy transaction on {datetime.now().strftime('%Y-%m-%d')}",
                        current_price=current_price
                    )
                    asset.save()
                    print(f"DEBUG CREATED NEW ASSET: {asset.id} - {asset.symbol} - {asset.name} - price: {asset.current_price}")
                
                # Get values directly from POST
                quantity = int(request.POST.get('quantity', 0))
                price = float(request.POST.get('price', 0))
                if price == 0 and asset.current_price > 0:
                    price = float(asset.current_price)
                    print(f"DEBUG Using asset current price: {price}")
                
                # Tính tổng tiền giao dịch
                total_amount = quantity * price
                notes = request.POST.get('notes', '')
                transaction_date_str = request.POST.get('transaction_date')
                
                # Xử lý ngày giao dịch, nếu người dùng chọn ngày
                if transaction_date_str:
                    try:
                        transaction_date = datetime.strptime(transaction_date_str, '%Y-%m-%d').date()
                    except ValueError:
                        # Nếu có lỗi khi parse ngày, sử dụng ngày hiện tại
                        transaction_date = timezone.now().date()
                        messages.warning(request, f"Định dạng ngày không hợp lệ. Đã sử dụng ngày hiện tại.")
                else:
                    transaction_date = timezone.now().date()
                
                # Validate values
                if quantity <= 0:
                    messages.error(request, "Số lượng phải lớn hơn 0")
                    form = TransactionForm(initial={'portfolio': portfolio, 'transaction_type': 'buy'})
                    return render(request, 'portfolio/transaction_form.html', {
                        'form': form, 'title': 'Mua cổ phiếu', 'portfolio': portfolio, 'today_date': today_date,
                        'wallet': wallet
                    })
                    
                if price <= 0:
                    messages.error(request, "Giá phải lớn hơn 0")
                    form = TransactionForm(initial={'portfolio': portfolio, 'transaction_type': 'buy'})
                    return render(request, 'portfolio/transaction_form.html', {
                        'form': form, 'title': 'Mua cổ phiếu', 'portfolio': portfolio, 'today_date': today_date,
                        'wallet': wallet
                    })
                
                # Chuyển đổi tổng tiền thành Decimal để tránh lỗi khi so sánh với wallet.balance
                from decimal import Decimal
                total_amount_decimal = Decimal(str(total_amount))
                
                # Kiểm tra số dư ví điện tử
                if wallet.balance < total_amount_decimal:
                    messages.error(request, f"Số dư ví không đủ. Số dư hiện tại: {wallet.balance:,.0f} VNĐ, cần: {total_amount:,.0f} VNĐ")
                    form = TransactionForm(initial={'portfolio': portfolio, 'transaction_type': 'buy'})
                    return render(request, 'portfolio/transaction_form.html', {
                        'form': form, 'title': 'Mua cổ phiếu', 'portfolio': portfolio, 'today_date': today_date,
                        'wallet': wallet 
                    })
                
                # Create transaction directly
                print(f"DEBUG DIRECT CREATE: Creating transaction with: portfolio={portfolio.id}, asset={asset.id}, quantity={quantity}, price={price}, date={transaction_date}")
                
                transaction = Transaction(
                    portfolio=portfolio,
                    asset=asset,
                    transaction_type='buy',
                    quantity=quantity,
                    price=price,
                    total_amount=total_amount,
                    transaction_date=transaction_date,
                    notes=notes
                )
                
                # Thực hiện giao dịch và cập nhật ví
                # Trừ tiền từ ví điện tử (sử dụng Decimal để tránh lỗi kiểu dữ liệu)
                wallet.balance = wallet.balance - total_amount_decimal
                wallet.save()
                
                # Lưu giao dịch
                transaction.save()
                
                print(f"DEBUG TRANSACTION SAVED: {transaction.id} - {transaction.asset.symbol} - {transaction.quantity} units at {transaction.price} on {transaction.transaction_date}")
                messages.success(request, f'Giao dịch mua {quantity} cổ phiếu {symbol} với giá {price:,.0f} VNĐ vào ngày {transaction_date.strftime("%d/%m/%Y")} đã được thực hiện thành công! Số dư ví còn: {wallet.balance:,.0f} VNĐ')
                return redirect('portfolio_detail', pk=portfolio_id)
                
            except ValueError as e:
                print(f"DEBUG VALUE ERROR: {str(e)}")
                messages.error(request, f"Lỗi định dạng dữ liệu: {str(e)}")
            except Exception as e:
                print(f"DEBUG GENERIC ERROR: {str(e)}")
                messages.error(request, f"Lỗi không xác định: {str(e)}")
        else:
            messages.error(request, "Vui lòng chọn mã cổ phiếu")
            
    # Create a form with default values
    form = TransactionForm(initial={'portfolio': portfolio, 'transaction_type': 'buy'})
    
    return render(request, 'portfolio/transaction_form.html', {
        'form': form,
        'title': 'Mua cổ phiếu',
        'portfolio': portfolio,
        'portfolio_assets': portfolio.portfolioasset_set.all(),
        'all_assets': all_assets,
        'today_date': today_date,
        'wallet': wallet  # Thêm thông tin ví vào context
    })

@login_required
def sell_stock(request, portfolio_id):
    # Check that the portfolio exists and belongs to the current user
    portfolio = get_object_or_404(Portfolio, pk=portfolio_id, user=request.user)
    
    # Lấy thông tin ví điện tử của người dùng
    wallet, created = Wallet.objects.get_or_create(user=request.user)
    
    # Get owned assets for this portfolio with quantity > 0
    portfolio_assets = PortfolioAsset.objects.filter(
        portfolio=portfolio,
        quantity__gt=0
    ).select_related('asset')
    
    # Get assets that can be sold (having quantity > 0)
    owned_assets = Asset.objects.filter(
        id__in=portfolio_assets.values_list('asset_id', flat=True)
    )
    
    # Lấy ngày hiện tại cho trường ngày mặc định
    today_date = timezone.now().strftime('%Y-%m-%d')
    
    # Debug info
    print(f"DEBUG: Portfolio ID: {portfolio_id}, User: {request.user.username}")
    print(f"DEBUG: Owned assets count: {owned_assets.count()}")
    print(f"DEBUG: Portfolio assets count: {portfolio_assets.count()}")
    
    if request.method == 'POST':
        # Log ra tất cả dữ liệu form
        print(f"DEBUG: SELL STOCK - Full POST data: {request.POST.dict()}")
        
        # Lấy symbol từ form, ưu tiên dùng selected_symbol nếu có
        symbol = request.POST.get('selected_symbol') or request.POST.get('symbol')
        
        print(f"DEBUG: Using symbol: {symbol}")
        
        if not symbol:
            messages.error(request, "Không tìm thấy mã cổ phiếu để bán, vui lòng chọn lại.")
            return redirect('sell_stock', portfolio_id=portfolio_id)
        
        try:
            # Find asset by symbol
            asset = Asset.objects.get(symbol=symbol)
            
            # Build form data with the asset included
            form_data = request.POST.copy()
            form_data['asset'] = asset.id  # Set the asset ID in the form data
            
            form = TransactionForm(form_data, initial={'portfolio': portfolio, 'transaction_type': 'sell'})
            
            if form.is_valid():
                quantity = form.cleaned_data['quantity']
                price = form.cleaned_data['price']
                
                # Kiểm tra số lượng bán không vượt quá số lượng hiện có
                portfolio_asset = portfolio_assets.filter(asset=asset).first()
                
                if not portfolio_asset or portfolio_asset.quantity < quantity:
                    messages.error(request, 'Số lượng bán vượt quá số lượng hiện có!')
                    # Reset the asset queryset to only owned assets
                    form.fields['asset'].queryset = owned_assets
                    return render(request, 'portfolio/transaction_form.html', {
                        'form': form,
                        'title': 'Bán cổ phiếu',
                        'portfolio': portfolio,
                        'portfolio_assets': portfolio_assets,
                        'owned_assets': owned_assets,
                        'today_date': today_date,
                        'wallet': wallet
                    })
                
                transaction = form.save(commit=False)
                transaction.portfolio = portfolio
                transaction.transaction_type = 'sell'
                
                # Lấy ngày giao dịch từ form
                transaction_date_str = form_data.get('transaction_date')
                if transaction_date_str:
                    try:
                        transaction_date = datetime.strptime(transaction_date_str, '%Y-%m-%d').date()
                        transaction.transaction_date = transaction_date
                    except ValueError:
                        transaction.transaction_date = timezone.now().date()
                        messages.warning(request, f"Định dạng ngày không hợp lệ. Đã sử dụng ngày hiện tại.")
                
                # Tính tổng tiền giao dịch
                total_amount = quantity * price
                
                # Chuyển đổi tổng tiền thành Decimal để tránh lỗi khi cộng với wallet.balance
                from decimal import Decimal
                total_amount_decimal = Decimal(str(total_amount))
                
                # Lưu giao dịch
                transaction.save()
                
                # Cập nhật số dư ví (sử dụng Decimal để tránh lỗi kiểu dữ liệu)
                wallet.balance = wallet.balance + total_amount_decimal
                wallet.save()
                
                messages.success(request, f'Giao dịch bán {quantity} cổ phiếu {symbol} với giá {transaction.price:,.0f} VNĐ vào ngày {transaction.transaction_date.strftime("%d/%m/%Y")} đã được thực hiện thành công! Số dư ví: {wallet.balance:,.0f} VNĐ')
                return redirect('portfolio_detail', pk=portfolio_id)
            else:
                # Debug form errors
                print(f"DEBUG: Form errors: {form.errors}")
                messages.error(request, f"Lỗi nhập dữ liệu: {form.errors}")
                # Reset the asset queryset to only owned assets
                form.fields['asset'].queryset = owned_assets
        except Asset.DoesNotExist:
            messages.error(request, f"Không tìm thấy cổ phiếu với mã {symbol}")
            form = TransactionForm(initial={
                'portfolio': portfolio,
                'transaction_type': 'sell',
                'transaction_date': timezone.now()
            })
            form.fields['asset'].queryset = owned_assets
    else:
        # Create form with initial values
        form = TransactionForm(initial={
            'portfolio': portfolio,
            'transaction_type': 'sell',
            'transaction_date': timezone.now()
        })
        
        # Direct assignment of assets queryset
        form.fields['asset'].queryset = owned_assets
        
        # Log the asset count
        print(f"DEBUG: Owned asset count in sell_stock form: {owned_assets.count()}")
        
        # Debug owned assets
        for asset in owned_assets:
            try:
                portfolio_asset = portfolio_assets.get(asset=asset)
                print(f"  - {asset.symbol}: {portfolio_asset.quantity} shares at {portfolio_asset.average_price}")
            except PortfolioAsset.DoesNotExist:
                print(f"  - Error: No PortfolioAsset found for {asset.symbol}")
        
        # Add warning message if no assets are available
        if not owned_assets.exists():
            messages.warning(request, 'Không có cổ phiếu nào trong danh mục để bán. Hãy mua cổ phiếu trước.')
    
    # Chuẩn bị context với portfolio_assets đã được xử lý để truyền dữ liệu đúng
    prepared_portfolio_assets = []
    for pa in portfolio_assets:
        prepared_portfolio_assets.append({
            'asset': pa.asset,
            'quantity': pa.quantity,
            'average_price': pa.average_price,
            'symbol': pa.asset.symbol,
            'asset_id': pa.asset.id,
            'name': pa.asset.name,
            'current_price': pa.asset.current_price
        })
    
    # Tạo data attributes cho mỗi portfolio asset để sử dụng trong JavaScript
    portfolio_asset_data = {pa['asset'].id: {
        'average_price': float(pa['average_price']),
        'quantity': pa['quantity'],
        'symbol': pa['symbol']
    } for pa in prepared_portfolio_assets}
    
    # Chuyển đổi thành chuỗi JSON để sử dụng trong template
    portfolio_asset_json = json.dumps(portfolio_asset_data)
    
    return render(request, 'portfolio/transaction_form.html', {
        'form': form,
        'title': 'Bán cổ phiếu',
        'portfolio': portfolio,
        'portfolio_assets': prepared_portfolio_assets,
        'portfolio_asset_data': portfolio_asset_json,  # Thêm dữ liệu JSON
        'owned_assets': owned_assets,
        'today_date': today_date,
        'wallet': wallet  # Thêm thông tin ví vào context
    })

@login_required
def portfolio_transactions(request, portfolio_id):
    portfolio = get_object_or_404(Portfolio, pk=portfolio_id, user=request.user)
    transactions = Transaction.objects.filter(portfolio=portfolio).order_by('-transaction_date')
    return render(request, 'portfolio/portfolio_transactions.html', {
        'portfolio': portfolio,
        'transactions': transactions
    })

# Auth0 related views - replaced with Django authentication
def login_view(request):
    """Sử dụng Auth0 để xác thực người dùng"""
    from authlib.integrations.django_client import OAuth
    from django.conf import settings
    from urllib.parse import quote_plus, urlencode
    
    # Khởi tạo OAuth client
    oauth = OAuth()
    oauth.register(
        "auth0",
        client_id=settings.AUTH0_CLIENT_ID,
        client_secret=settings.AUTH0_CLIENT_SECRET,
        client_kwargs={
            "scope": "openid profile email",
        },
        server_metadata_url=f"https://{settings.AUTH0_DOMAIN}/.well-known/openid-configuration",
    )
    
    # Chuyển hướng đến trang đăng nhập Auth0
    return oauth.auth0.authorize_redirect(
        request, request.build_absolute_uri(reverse("callback"))
    )

def callback(request):
    """Xử lý callback từ Auth0 và đăng nhập người dùng"""
    from authlib.integrations.django_client import OAuth
    from django.conf import settings
    import json
    from .utils import get_auth0_user_profile
    
    # Khởi tạo OAuth client
    oauth = OAuth()
    oauth.register(
        "auth0",
        client_id=settings.AUTH0_CLIENT_ID,
        client_secret=settings.AUTH0_CLIENT_SECRET,
        client_kwargs={
            "scope": "openid profile email",
        },
        server_metadata_url=f"https://{settings.AUTH0_DOMAIN}/.well-known/openid-configuration",
    )
    
    # Lấy token từ Auth0
    token = oauth.auth0.authorize_access_token(request)
    userinfo = token.get('userinfo')
    
    # Lấy thêm thông tin từ API Auth0 nếu được
    access_token = token.get('access_token')
    if access_token:
        additional_info = get_auth0_user_profile(access_token)
        if additional_info:
            # Merge additional info vào userinfo
            userinfo.update(additional_info)
    
    if userinfo:
        # Lưu thông tin người dùng vào session
        request.session['userinfo'] = userinfo
        
        # Kiểm tra xem người dùng đã tồn tại trong database chưa
        auth0_user_id = userinfo.get('sub')
        email = userinfo.get('email')
        
        try:
            # Tìm user bằng auth0_user_id
            user = User.objects.get(auth0_user_id=auth0_user_id)
        except User.DoesNotExist:
            try:
                # Tìm user bằng email nếu không tìm thấy qua auth0_user_id
                user = User.objects.get(email=email)
                user.auth0_user_id = auth0_user_id
                user.save()
            except User.DoesNotExist:
                # Tạo user mới nếu chưa tồn tại
                user = User.objects.create_user(
                    username=email.split('@')[0],
                    email=email,
                    auth0_user_id=auth0_user_id,
                    first_name=userinfo.get('given_name', ''),
                    last_name=userinfo.get('family_name', ''),
                    profile_picture_url=userinfo.get('picture', '')
                )
                # Tự động tạo ví điện tử cho người dùng mới
                from .models import Wallet
                Wallet.objects.create(user=user)
        
        # Đăng nhập người dùng
        login(request, user)
        
        # Cập nhật thông tin người dùng từ Auth0 profile
        if 'picture' in userinfo and userinfo['picture']:
            user.profile_picture_url = userinfo['picture']
        if 'given_name' in userinfo and userinfo['given_name']:
            user.first_name = userinfo['given_name']
        if 'family_name' in userinfo and userinfo['family_name']:
            user.last_name = userinfo['family_name']
        for field in user._meta.fields:
            value = getattr(user, field.name)
            if isinstance(value, str) and len(value) > 200:
                print(f"Field {field.name} quá dài: {len(value)} ký tự")

        user.save()

        
        return redirect('dashboard')
    
    return redirect('home')

def logout_view(request):
    """Đăng xuất khỏi Django và Auth0"""
    from django.conf import settings
    from urllib.parse import quote_plus, urlencode
    
    # Đăng xuất khỏi Django
    logout(request)
    
    # Xóa session
    request.session.clear()
    
    # Chuyển hướng đến trang đăng xuất của Auth0
    return redirect(
        f"https://{settings.AUTH0_DOMAIN}/v2/logout?"
        + urlencode(
            {
                "returnTo": request.build_absolute_uri(reverse("home")),
                "client_id": settings.AUTH0_CLIENT_ID,
            },
            quote_via=quote_plus,
        )
    )

def register(request):
    """Chuyển hướng đến trang đăng ký của Auth0"""
    # Sử dụng cùng hàm login_view để chuyển hướng đến Auth0
    return login_view(request)

# User profile view
@login_required
def user_profile(request):
    user = request.user
    
    if request.method == 'POST':
        form = UserProfileForm(request.POST, request.FILES, instance=user)
        if form.is_valid():
            user_data = form.save(commit=False)
            
            # Check if a new profile picture was uploaded
            if 'profile_picture' in request.FILES and request.FILES['profile_picture']:
                # If user uploads a local picture, prioritize it over the Auth0 URL
                user_data.profile_picture = request.FILES['profile_picture']
                # Clear Auth0 profile picture URL to use the uploaded one
                user_data.profile_picture_url = None
            
            user_data.save()
            messages.success(request, 'Thông tin cá nhân đã được cập nhật thành công!')
            return redirect('user_profile')
    else:
        form = UserProfileForm(instance=user)
    
    # Get Auth0 user info from session
    auth0_userinfo = request.session.get('userinfo', {})
    
    return render(request, 'portfolio/user_profile.html', {
        'form': form,
        'auth0_userinfo': auth0_userinfo
    })

# ============ MARKET =======
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

def get_historical_data_api(request, stock_code):
    historical_data = get_historical_data(stock_code)
    return JsonResponse(historical_data.to_dict(orient='records'), safe=False)

def get_stock_historical_data(request, symbol):
    """Lấy dữ liệu lịch sử giá cổ phiếu cho biểu đồ"""
    try:
        symbol = symbol.upper()
        logger.info(f"Lấy dữ liệu lịch sử cho mã {symbol}")
        
        # Sử dụng dịch vụ PostgreSQL để lấy dữ liệu
        from .vnstock_services import get_technical_indicators
        postgres_service = get_postgres_chart_service()
        
        # Ưu tiên lấy dữ liệu từ hàm get_technical_indicators vừa tạo
        technical_data = get_technical_indicators(symbol)
        
        if technical_data and technical_data.get('success', False):
            # Định dạng dữ liệu cho biểu đồ
            chart_data = technical_data.get('candles', [])
            
            # Thêm các chỉ báo kỹ thuật vào từng điểm dữ liệu nếu cần
            # Điều này là tùy chọn, vì trong phần JavaScript chúng ta sẽ sử dụng dữ liệu riêng biệt
            
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
        
        # Nếu không có dữ liệu từ hàm mới, sử dụng lại logic cũ
        stock_data = postgres_service.get_stock_data(symbol)
        
        if stock_data is None or not stock_data:
            logger.warning(f"Không tìm thấy dữ liệu cho mã {symbol} trong PostgreSQL")
            # Fallback to direct vnstock service if PostgreSQL data not available
            historical_data = get_historical_data(symbol)
            
            # Chuyển đổi dữ liệu thành định dạng phù hợp cho biểu đồ
            chart_data = []
            for _, row in historical_data.iterrows():
                chart_data.append({
                    'time': row['time'].strftime('%Y-%m-%d') if hasattr(row['time'], 'strftime') else str(row['time']),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close'])
                })
            return JsonResponse(chart_data, safe=False)
        else:
            # Định dạng dữ liệu cho biểu đồ với đầy đủ các chỉ báo kỹ thuật
            formatted_data = postgres_service.format_for_chart(stock_data)
            
            if not formatted_data or not formatted_data.get('candles'):
                logger.warning(f"Dữ liệu biểu đồ từ PostgreSQL không hợp lệ cho mã {symbol}")
                return JsonResponse({'error': 'Dữ liệu biểu đồ không hợp lệ'}, status=500)
            
            # Trả về dữ liệu định dạng mới với tất cả các chỉ báo
            logger.info(f"Trả về dữ liệu biểu đồ cho mã {symbol} từ postgres_service")
            return JsonResponse(formatted_data, safe=False)
        
    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu cho {symbol}: {str(e)}")
        return JsonResponse({'error': str(e)}, status=500)

def get_technical_indicators_api(request, symbol):
    """API endpoint để lấy các chỉ báo kỹ thuật cho biểu đồ"""
    try:
        symbol = symbol.upper()
        from .vnstock_services import get_technical_indicators
        
        # Lấy dữ liệu chỉ báo kỹ thuật
        technical_data = get_technical_indicators(symbol)
        
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

@csrf_exempt
@require_POST
def ai_chat_api(request):
    """
    API endpoint để xử lý các yêu cầu chat với AI
    """
    try:
        data = json.loads(request.body)
        message = data.get('message', '')
        
        if not message:
            return JsonResponse({
                'success': False,
                'error': 'Tin nhắn không được để trống'
            }, status=400)
        
        # Gọi API AI để nhận phản hồi
        response = get_ai_response(message)
        
        # Đơn giản hóa định dạng phản hồi và loại bỏ ký tự formatting
        response = response.strip()
        # Loại bỏ các ký tự đánh dấu in đậm và in nghiêng
        response = response.replace('**', '').replace('*', '')
        
        return JsonResponse({
            'success': True,
            'response': response
        })
    
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Dữ liệu JSON không hợp lệ'
        }, status=400)
    
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

# Debug view to directly test asset retrieval 
@login_required
def debug_assets(request):
    """Debug view to directly test asset retrieval"""
    # Get all assets
    all_assets = Asset.objects.all()
    
    # Create a simple context
    context = {
        'assets': all_assets,
        'assets_count': all_assets.count(),
        'portfolios': Portfolio.objects.filter(user=request.user),
    }
    
    # Log to console
    print(f"DEBUG: Found {all_assets.count()} assets in database")
    for asset in all_assets:
        print(f"  - Asset: {asset.id} | {asset.symbol} | {asset.name}")
    
    return render(request, 'portfolio/debug_assets.html', context)

@login_required
def sync_assets(request):
    """View to synchronize assets from VNStock to the database"""
    if request.method == 'POST' or request.method == 'GET':  # Allow both GET and POST
        try:
            result = sync_vnstock_to_assets()
            
            if isinstance(result, dict):
                messages.success(
                    request, 
                    f"Đồng bộ thành công! Đã thêm {result['created']} cổ phiếu mới, cập nhật {result['updated']} cổ phiếu. Bây giờ bạn có thể mua/bán những cổ phiếu này."
                )
            else:
                messages.error(request, "Đồng bộ không thành công, không có cổ phiếu nào được thêm.")
        except Exception as e:
            messages.error(request, f"Lỗi khi đồng bộ dữ liệu: {str(e)}")
    
    # Check if the request came from the market page
    referer = request.META.get('HTTP_REFERER', '')
    if 'market' in referer:
        return redirect('market')
    else:
        return redirect('debug_assets')

@login_required
def update_stock_prices(request):
    """View to update current stock prices"""
    if request.method == 'POST':
        try:
            # Call the function to fetch and update prices
            snapshot_df = fetch_stock_prices_snapshot()
            
            if snapshot_df is not None:
                # Count non-null prices
                non_null_prices = sum(1 for col in snapshot_df.columns if col != 'time' and snapshot_df[col].iloc[0] is not None)
                messages.success(
                    request, 
                    f"Cập nhật giá thành công! Đã cập nhật giá cho {non_null_prices} cổ phiếu."
                )
            else:
                messages.error(request, "Không thể cập nhật giá cổ phiếu.")
        except Exception as e:
            messages.error(request, f"Lỗi khi cập nhật giá: {str(e)}")
    
    return redirect('debug_assets')

@login_required
def get_stock_symbols(request):
    """API view để lấy danh sách mã cổ phiếu phù hợp với từ khóa tìm kiếm"""
    term = request.GET.get('term', '')
    
    # Sử dụng hàm vnstock để lấy danh sách cổ phiếu
    from .vnstock_services import get_all_stock_symbols
    
    try:
        # Lấy toàn bộ danh sách cổ phiếu
        all_symbols = get_all_stock_symbols()
        
        # Lọc theo từ khóa tìm kiếm - ưu tiên mã cổ phiếu khớp với từ khóa
        filtered_symbols = [item['ticker'] for item in all_symbols 
                           if term.upper() in item['ticker'].upper()][:10]
        
        # Nếu không đủ 10 kết quả, tìm thêm trong tên công ty
        if len(filtered_symbols) < 10 and term:
            remaining_slots = 10 - len(filtered_symbols)
            company_matches = [item['ticker'] for item in all_symbols 
                              if item['ticker'] not in filtered_symbols 
                              and term.upper() in item['organ_name'].upper()][:remaining_slots]
            filtered_symbols.extend(company_matches)
        
        return JsonResponse(filtered_symbols, safe=False)
    except Exception as e:
        print(f"Error in get_stock_symbols: {str(e)}")
        
        # Fallback to database search if vnstock fails
        assets = Asset.objects.filter(symbol__istartswith=term)[:10]
        symbols = [asset.symbol for asset in assets]
        
        return JsonResponse(symbols, safe=False)

@login_required
def get_stock_symbols_info(request):
    """API view để lấy thông tin tên công ty cho các mã cổ phiếu"""
    if request.method == 'POST':
        try:
            import json
            symbols_json = request.POST.get('symbols', '[]')
            symbols = json.loads(symbols_json)
            
            # Nếu không có mã cổ phiếu, trả về dict trống
            if not symbols:
                return JsonResponse({}, safe=False)
            
            # Lấy thông tin từ vnstock API
            from .vnstock_services import get_all_stock_symbols
            try:
                all_symbols = get_all_stock_symbols()
                # Tạo dict ánh xạ từ mã đến tên công ty
                symbol_info = {item['ticker']: item['organ_name'] for item in all_symbols 
                              if item['ticker'] in symbols}
                
                # Nếu thiếu symbol nào, bổ sung từ database
                missing_symbols = [symbol for symbol in symbols if symbol not in symbol_info]
                if missing_symbols:
                    assets = Asset.objects.filter(symbol__in=missing_symbols)
                    for asset in assets:
                        symbol_info[asset.symbol] = asset.name
                
                return JsonResponse(symbol_info, safe=False)
            except Exception as e:
                print(f"Error fetching company names from vnstock: {str(e)}")
                # Fallback to database
                assets = Asset.objects.filter(symbol__in=symbols)
                symbol_info = {asset.symbol: asset.name for asset in assets}
                return JsonResponse(symbol_info, safe=False)
                
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=400)
    
    return JsonResponse({'error': 'Method not allowed'}, status=405)

@login_required
def get_stock_price(request, symbol):
    """
    API endpoint để lấy giá cổ phiếu hiện tại và giá gợi ý mua/bán.
    """
    try:
        import random
        from .vnstock_services import get_current_bid_price, get_all_stock_symbols
        
        # Tìm asset hoặc tạo mới nếu chưa có
        asset = Asset.objects.filter(symbol=symbol).first()
        
        # Lấy giá từ vnstock theo hướng dẫn của người dùng
        current_bid = get_current_bid_price(symbol)
        
        if current_bid is not None:
            # Nếu lấy giá thành công từ API, sử dụng giá này
            match_price = float(current_bid)
            
            # Nếu asset chưa tồn tại, tạo mới
            if not asset:
                # Tìm thông tin công ty từ VNStock
                try:
                    print(f"DEBUG: Asset {symbol} not found in database, creating new asset")
                    all_symbols = get_all_stock_symbols()
                    company_info = next((item for item in all_symbols if item['ticker'] == symbol), None)
                    
                    if company_info:
                        company_name = company_info.get('organ_name', f"Công ty {symbol}")
                        # Tạo asset mới
                        asset = Asset(
                            symbol=symbol,
                            name=company_name,
                            type='stock',
                            sector='Unknown',
                            description=f"Auto-created from VNStock on {datetime.now().strftime('%Y-%m-%d')}",
                            current_price=match_price
                        )
                        asset.save()
                        print(f"DEBUG: Created new asset: {asset.id} - {asset.symbol} - {asset.name}")
                    else:
                        # Nếu không tìm thấy thông tin công ty, vẫn tạo Asset với thông tin tối thiểu
                        print(f"DEBUG: Company info for {symbol} not found in VNStock, creating minimal asset")
                        asset = Asset(
                            symbol=symbol,
                            name=f"Cổ phiếu {symbol}",
                            type='stock',
                            sector='Unknown',
                            description=f"Auto-created minimal asset on {datetime.now().strftime('%Y-%m-%d')}",
                            current_price=match_price
                        )
                        asset.save()
                        print(f"DEBUG: Created minimal asset: {asset.id} - {asset.symbol}")
                except Exception as e:
                    print(f"ERROR creating new asset: {str(e)}")
                    # Tạo asset đơn giản nhất có thể nếu có lỗi
                    try:
                        asset = Asset(
                            symbol=symbol,
                            name=f"Cổ phiếu {symbol}",
                            type='stock',
                            sector='Unknown',
                            description="Auto-created as fallback after error",
                            current_price=match_price
                        )
                        asset.save()
                        print(f"DEBUG: Created fallback asset after error: {asset.id} - {asset.symbol}")
                    except Exception as e2:
                        print(f"CRITICAL ERROR creating fallback asset: {str(e2)}")
            elif asset:
                # Cập nhật giá mới cho asset
                asset.current_price = match_price
                asset.save(update_fields=['current_price', 'last_updated'])
                print(f"DEBUG: Updated price for {symbol} to {match_price}")
            
            # Tạo giá trị giả lập cho biến động giá
            change_percent = random.uniform(-2, 2)
            
            # Tính giá mua/bán: giá mua thấp hơn 0.5%, giá bán cao hơn 0.5%
            buy_price = round(match_price * 0.995)
            sell_price = round(match_price * 1.005)
            
            # Chuẩn bị dữ liệu phản hồi
            response_data = {
                'success': True,
                'symbol': symbol,
                'price': match_price,
                'change_percent': change_percent,
                'buy_price': buy_price,
                'sell_price': sell_price,
                'source': 'vnstock',
                'asset_exists': asset is not None,
                'asset_id': asset.id if asset else None,
                'asset_name': asset.name if asset else f"Cổ phiếu {symbol}"
            }
            
            # Trả về phản hồi JSON
            return JsonResponse(response_data)
        else:
            # Không lấy được giá từ API, tìm trong cơ sở dữ liệu
            asset = Asset.objects.filter(symbol=symbol).first()
            if asset:
                match_price = float(asset.current_price)
                change_percent = random.uniform(-2, 2)
                buy_price = round(match_price * 0.995)
                sell_price = round(match_price * 1.005)
                
                response_data = {
                    'success': True,
                    'symbol': symbol,
                    'price': match_price,
                    'change_percent': change_percent,
                    'buy_price': buy_price,
                    'sell_price': sell_price,
                    'source': 'database',
                    'asset_exists': True,
                    'asset_id': asset.id,
                    'asset_name': asset.name
                }
                return JsonResponse(response_data)
            else:
                # Tạo cổ phiếu mới với giá mặc định nếu không thể lấy từ API
                try:
                    default_price = 10000  # Giá mặc định
                    asset = Asset(
                        symbol=symbol,
                        name=f"Cổ phiếu {symbol}",
                        type='stock',
                        sector='Unknown',
                        description=f"Auto-created with default price on {datetime.now().strftime('%Y-%m-%d')}",
                        current_price=default_price
                    )
                    asset.save()
                    print(f"DEBUG: Created new asset with default price: {asset.id} - {asset.symbol}")
                    
                    response_data = {
                        'success': True,
                        'symbol': symbol,
                        'price': default_price,
                        'change_percent': 0,
                        'buy_price': round(default_price * 0.995),
                        'sell_price': round(default_price * 1.005),
                        'source': 'default',
                        'asset_exists': True,
                        'asset_id': asset.id,
                        'asset_name': asset.name
                    }
                    return JsonResponse(response_data)
                except Exception as e:
                    print(f"ERROR creating new asset with default price: {str(e)}")
                    # Không tìm thấy cổ phiếu trong cơ sở dữ liệu và không thể tạo mới
                    return JsonResponse({
                        'success': False,
                        'error': f'Không tìm thấy mã cổ phiếu {symbol} và không thể tạo mới'
                    }, status=404)
    except Exception as e:
        print(f"ERROR in get_stock_price: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': f'Lỗi khi xử lý thông tin cổ phiếu: {str(e)}'
        }, status=500)

@login_required
def create_asset_from_symbol(request):
    """
    API endpoint để tạo mới hoặc cập nhật asset từ mã cổ phiếu
    """
    if request.method == 'POST':
        try:
            symbol = request.POST.get('symbol')
            if not symbol:
                return JsonResponse({'success': False, 'error': 'Thiếu mã cổ phiếu'}, status=400)
            
            from .vnstock_services import get_current_bid_price, get_all_stock_symbols
            
            # Kiểm tra xem asset đã tồn tại chưa
            asset = Asset.objects.filter(symbol=symbol).first()
            
            # Nếu đã tồn tại thì cập nhật giá
            if asset:
                # Lấy giá hiện tại từ API
                current_price = get_current_bid_price(symbol)
                if current_price is not None:
                    asset.current_price = float(current_price)
                    asset.save(update_fields=['current_price', 'last_updated'])
                    
                    return JsonResponse({
                        'success': True,
                        'message': f'Đã cập nhật giá cho {symbol}',
                        'asset_id': asset.id,
                        'asset_name': asset.name,
                        'price': float(asset.current_price),
                        'created': False,
                        'updated': True
                    })
                else:
                    return JsonResponse({
                        'success': True,
                        'message': f'Không lấy được giá mới cho {symbol}, giữ nguyên giá cũ',
                        'asset_id': asset.id,
                        'asset_name': asset.name,
                        'price': float(asset.current_price),
                        'created': False,
                        'updated': False
                    })
            
            # Nếu chưa tồn tại, tạo mới
            # Lấy giá hiện tại
            current_price = get_current_bid_price(symbol)
            if current_price is None:
                current_price = 10000  # Giá mặc định nếu không lấy được
            
            # Lấy thông tin công ty
            company_name = f"Cổ phiếu {symbol}"
            try:
                all_symbols = get_all_stock_symbols()
                company_info = next((item for item in all_symbols if item['ticker'] == symbol), None)
                if company_info:
                    company_name = company_info.get('organ_name', company_name)
            except Exception as e:
                print(f"ERROR getting company info for {symbol}: {str(e)}")
            
            # Tạo asset mới
            new_asset = Asset(
                symbol=symbol,
                name=company_name,
                type='stock',
                sector='Unknown',
                description=f"Auto-created via API on {datetime.now().strftime('%Y-%m-%d')}",
                current_price=current_price
            )
            new_asset.save()
            
            return JsonResponse({
                'success': True,
                'message': f'Đã tạo mới cổ phiếu {symbol}',
                'asset_id': new_asset.id,
                'asset_name': new_asset.name,
                'price': float(new_asset.current_price),
                'created': True,
                'updated': False
            })
            
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': f'Lỗi: {str(e)}'
            }, status=500)
    
    return JsonResponse({'success': False, 'error': 'Chỉ hỗ trợ phương thức POST'}, status=405)

def debug_price_board(request):
    """API endpoint để debug dữ liệu bảng giá trực tiếp"""
    try:
        # Lấy dữ liệu bảng giá từ PostgreSQL
        postgres_service = get_postgres_chart_service()
        
        # Sử dụng phương thức để lấy bảng giá
        try:
            price_board_df = postgres_service.get_price_board_data()
            logger.info(f"Debug API: Đã lấy được dữ liệu bảng giá từ PostgreSQL: {price_board_df.shape[0]} dòng, {price_board_df.shape[1]} cột")
            
            # Log cấu trúc DataFrame
            column_info = {col: str(price_board_df[col].dtype) for col in price_board_df.columns}
            logger.info(f"Debug API: Column info: {column_info}")
            
            # Chuyển đổi DataFrame thành dict
            if not price_board_df.empty:
                # Chuyển đổi DataFrame thành list các dict
                stock_data = price_board_df.to_dict('records')
                
                # Log sample data
                if stock_data:
                    logger.info(f"Debug API: Sample data: {stock_data[0]}")
                
                # Đếm số bản ghi với suggestion không rỗng
                suggestions_count = sum(1 for item in stock_data if item.get('suggestion'))
                logger.info(f"Debug API: Số bản ghi có suggestion: {suggestions_count}/{len(stock_data)}")
                
                # Trả về JSON response
                return JsonResponse({
                    'status': 'success',
                    'count': len(stock_data),
                    'suggestions_count': suggestions_count,
                    'columns': list(price_board_df.columns),
                    'sample': stock_data[:3] if stock_data else [],
                    'data': stock_data
                })
            else:
                return JsonResponse({
                    'status': 'error',
                    'message': 'Không có dữ liệu trong DataFrame'
                })
        except Exception as e:
            logger.error(f"Debug API: Lỗi khi lấy dữ liệu: {str(e)}")
            return JsonResponse({
                'status': 'error',
                'message': f'Lỗi khi lấy dữ liệu: {str(e)}'
            }, status=500)
    except Exception as e:
        logger.error(f"Debug API: Lỗi ngoại lệ: {str(e)}")
        return JsonResponse({
            'status': 'error',
            'message': f'Lỗi ngoại lệ: {str(e)}'
        }, status=500)

def simple_market(request):
    """Trang thị trường đơn giản nhất để test"""
    try:
        import psycopg2
        import psycopg2.extras
        
        # Kết nối trực tiếp đến database
        conn = psycopg2.connect(
            host='db',
            database='db_for_pm',
            user='airflow',
            password='admin123',
            port=5432
        )
        
        # Tạo cursor và lấy dữ liệu
        data = []
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Truy vấn dữ liệu
            cursor.execute("""
                SELECT symbol as "CK", ceiling as "Trần", floor as "Sàn", 
                       (ceiling + floor) / 2 as "TC", current_price as "Giá", 
                       vol as "Khối_lượng",
                       json_data->>'suggestion' as suggestion
                FROM stock_processed_data
                ORDER BY symbol
            """)
            rows = cursor.fetchall()
            
            # Chuyển đổi dữ liệu thành dạng dict
            for row in rows:
                data.append(dict(row))
        
        # Đóng kết nối
        conn.close()
        
        # Truyền dữ liệu vào template
        context = {
            "stock_data": data,
            "count": len(data)
        }
        return render(request, 'portfolio/simple_market.html', context)
    except Exception as e:
        return HttpResponse(f"Lỗi kết nối trực tiếp: {str(e)}")

def raw_data_api(request):
    """API đơn giản để lấy dữ liệu trực tiếp từ bảng"""
    try:
        import psycopg2
        import psycopg2.extras
        
        # Kết nối trực tiếp đến database
        conn = psycopg2.connect(
            host='db',
            database='db_for_pm',
            user='airflow',
            password='admin123',
            port=5432
        )
        
        # Tạo cursor và lấy dữ liệu
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Lấy danh sách các cột
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'stock_processed_data'
            """)
            columns = [row[0] for row in cursor.fetchall()]
            
            # Lấy dữ liệu
            cursor.execute("""
                SELECT symbol, ceiling, floor, current_price, vol, json_data
                FROM stock_processed_data
                LIMIT 10
            """)
            rows = cursor.fetchall()
            
            # Chuyển đổi dữ liệu thành dạng dict
            data = []
            for row in rows:
                row_dict = dict(row)
                data.append(row_dict)
        
        # Đóng kết nối
        conn.close()
        
        # Trả về kết quả
        return JsonResponse({
            'status': 'success',
            'columns': columns,
            'count': len(data),
            'data': data
        }, json_dumps_params={'default': str})
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        }, status=500)

def new_market(request):
    """Trang thị trường mới, đơn giản, không sử dụng JavaScript phức tạp"""
    try:
        import psycopg2
        import psycopg2.extras
        
        # Kết nối trực tiếp đến database
        conn = psycopg2.connect(
            host='db',
            database='db_for_pm',
            user='airflow',
            password='admin123',
            port=5432
        )
        
        # Lấy dữ liệu
        data = []
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Truy vấn dữ liệu
            cursor.execute("""
                SELECT 
                    symbol, 
                    ceiling, 
                    floor, 
                    (ceiling + floor) / 2 as tc, 
                    current_price, 
                    vol,
                    coalesce(json_data->>'suggestion', 'HOLD') as suggestion,
                    coalesce(json_data->>'reason', 'Không có lý do') as reason
                FROM stock_processed_data
                ORDER BY symbol
            """)
            rows = cursor.fetchall()
            
            # Chuyển đổi dữ liệu thành dạng dict
            for row in rows:
                item = {}
                item['symbol'] = row['symbol']
                item['ceiling'] = row['ceiling'] 
                item['floor'] = row['floor']
                item['tc'] = row['tc']
                item['price'] = row['current_price']
                item['volume'] = row['vol']
                item['suggestion'] = row['suggestion']
                item['reason'] = row['reason']
                data.append(item)
                
            # Lấy danh sách symbol riêng cho dropdown
            cursor.execute("SELECT DISTINCT symbol FROM stock_processed_data ORDER BY symbol")
            available_symbols = [row[0] for row in cursor.fetchall()]
        
        # Đóng kết nối
        conn.close()
        
        # Ghi log
        logger.info(f"Trang thị trường mới: Đã lấy được {len(data)} bản ghi từ PostgreSQL")
        if data:
            logger.info(f"Trang thị trường mới: Mẫu dữ liệu: {data[0]}")
        
        # Trả về template
        return render(request, 'portfolio/new_market.html', {
            'stock_data': data,
            'count': len(data),
            'total_symbols': len(available_symbols),
            'symbols': available_symbols
        })
        
    except Exception as e:
        logger.error(f"Lỗi trang thị trường mới: {str(e)}")
        return HttpResponse(f"Lỗi kết nối database: {str(e)}")

# Thêm endpoint mới cho dữ liệu lịch sử từ Kafka
def get_kafka_stock_data(request, symbol):
    """
    API endpoint để lấy dữ liệu lịch sử từ Kafka cho mã cổ phiếu
    """
    try:
        logger.info(f"Đang lấy dữ liệu Kafka cho mã {symbol}")
        # Cố gắng lấy dữ liệu từ Kafka
        from .kafka_service import get_price_board_data
        kafka_data = get_price_board_data()
        
        # Kiểm tra xem có dữ liệu và dữ liệu ở định dạng list không
        if kafka_data and isinstance(kafka_data, list):
            # Tìm mã cổ phiếu trong dữ liệu Kafka
            stock_data = next((item for item in kafka_data if item.get('CK') == symbol), None)
            
            if stock_data and 'json_data' in stock_data and 'historical_processed' in stock_data['json_data']:
                historical_data = stock_data['json_data']['historical_processed']
                
                # Chuyển đổi dữ liệu lịch sử thành định dạng phù hợp cho biểu đồ
                candles = []
                volumes = []
                ma5_data = []
                ma20_data = []
                rsi_data = []
                macd_line_data = []
                macd_signal_data = []
                macd_histogram_data = []
                
                # Sắp xếp dữ liệu lịch sử theo thời gian
                historical_data.sort(key=lambda x: x.get('time', ''))
                
                # Lấy suggestion và reason từ điểm dữ liệu gần nhất
                latest_data = historical_data[-1] if historical_data else {}
                suggestion = latest_data.get('suggestion', 'HOLD')
                reason = latest_data.get('reason', 'Không có đề xuất')
                
                # Duyệt qua dữ liệu lịch sử để tạo series cho biểu đồ
                for item in historical_data:
                    time_value = item.get('time')
                    
                    # Candle data
                    candle = {
                        'time': time_value,
                        'open': item.get('open', 0),
                        'high': item.get('high', 0),
                        'low': item.get('low', 0),
                        'close': item.get('close', 0),
                        'volume': item.get('volume', 0)
                    }
                    candles.append(candle)
                    
                    # Volume data
                    volumes.append({
                        'time': time_value,
                        'value': item.get('volume', 0),
                        'color': '#26a69a80' if item.get('close', 0) >= item.get('open', 0) else '#ef535080'
                    })
                    
                    # MA5 data
                    if item.get('ma5') is not None:
                        ma5_data.append({
                            'time': time_value,
                            'value': item.get('ma5')
                        })
                    
                    # MA20 data
                    if item.get('ma20') is not None:
                        ma20_data.append({
                            'time': time_value,
                            'value': item.get('ma20')
                        })
                    
                    # RSI data
                    if item.get('rsi') is not None:
                        rsi_data.append({
                            'time': time_value,
                            'value': item.get('rsi')
                        })
                    
                    # MACD line
                    if item.get('macd_line') is not None:
                        macd_line_data.append({
                            'time': time_value,
                            'value': item.get('macd_line')
                        })
                    
                    # MACD signal
                    if item.get('macd_signal') is not None:
                        macd_signal_data.append({
                            'time': time_value,
                            'value': item.get('macd_signal')
                        })
                    
                    # MACD histogram
                    if item.get('macd_histogram') is not None:
                        macd_histogram_data.append({
                            'time': time_value,
                            'value': item.get('macd_histogram'),
                            'color': '#26a69a' if item.get('macd_histogram', 0) >= 0 else '#ef5350'
                        })
                
                # Trả về dữ liệu cho biểu đồ
                result = {
                    'candles': candles,
                    'volumes': volumes,
                    'ma5': ma5_data,
                    'ma20': ma20_data,
                    'rsi': rsi_data,
                    'macd': macd_line_data,
                    'signal': macd_signal_data,
                    'histogram': macd_histogram_data,
                    'suggestion': suggestion,
                    'reason': reason
                }
                
                logger.info(f"Đã lấy dữ liệu biểu đồ thành công cho {symbol}: {len(candles)} nến")
                return JsonResponse(result)
            
            # Nếu không tìm thấy dữ liệu lịch sử, thử lấy từ PostgreSQL
            logger.warning(f"Không tìm thấy dữ liệu lịch sử cho mã {symbol} trong Kafka, thử lấy từ PostgreSQL")
            return get_stock_historical_data(request, symbol)
        
        # Nếu không có dữ liệu từ Kafka, thử lấy từ PostgreSQL
        logger.warning(f"Không tìm thấy dữ liệu Kafka cho {symbol}, thử lấy từ PostgreSQL")
        return get_stock_historical_data(request, symbol)
        
    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu Kafka cho {symbol}: {str(e)}")
        return JsonResponse({'error': str(e)}, status=500)

def refresh_kafka_symbol(request, symbol):
    """
    API endpoint để làm mới dữ liệu Kafka cho một mã cổ phiếu cụ thể
    """
    try:
        logger.info(f"Đang làm mới dữ liệu Kafka cho mã {symbol}")
        
        # Thử gửi thông báo để làm mới dữ liệu
        from .kafka_service import get_price_board_data
        
        # Gọi hàm để kích hoạt việc làm mới dữ liệu từ Kafka
        kafka_data = get_price_board_data()
        
        # Kiểm tra xem dữ liệu đã được làm mới chưa
        if kafka_data and isinstance(kafka_data, list):
            # Tìm mã cổ phiếu trong dữ liệu Kafka
            stock_data = next((item for item in kafka_data if item.get('CK') == symbol), None)
            
            if stock_data:
                logger.info(f"Đã làm mới thành công dữ liệu Kafka cho mã {symbol}")
                return JsonResponse({
                    'success': True,
                    'message': f'Đã làm mới dữ liệu cho mã {symbol}',
                    'data': {
                        'symbol': symbol,
                        'price': stock_data.get('Giá', 0),
                        'volume': stock_data.get('Khối_lượng', 0),
                        'suggestion': stock_data.get('suggestion', 'HOLD')
                    }
                })
            else:
                logger.warning(f"Không tìm thấy dữ liệu cho mã {symbol} sau khi làm mới")
                return JsonResponse({
                    'success': False,
                    'message': f'Không tìm thấy dữ liệu cho mã {symbol} sau khi làm mới'
                }, status=404)
        else:
            logger.warning(f"Không thể làm mới dữ liệu Kafka: Không có dữ liệu hoặc dữ liệu không đúng định dạng")
            return JsonResponse({
                'success': False,
                'message': 'Không thể làm mới dữ liệu Kafka: Không có dữ liệu hoặc dữ liệu không đúng định dạng'
            }, status=500)
    except Exception as e:
        logger.error(f"Lỗi khi làm mới dữ liệu Kafka cho {symbol}: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)
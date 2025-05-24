import json
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.core.paginator import Paginator
from django.utils import timezone
from .models import Portfolio, Asset, Transaction, PortfolioAsset, User, Wallet
from .forms import PortfolioForm, AssetForm, TransactionForm, UserRegistrationForm, UserProfileForm
from decimal import Decimal
from django.http import JsonResponse, HttpResponse
from .vnstock_services import sync_vnstock_to_assets, fetch_stock_prices_snapshot
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from .utils import get_ai_response
from datetime import datetime
import logging

# Thiết lập logging
logger = logging.getLogger(__name__)

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
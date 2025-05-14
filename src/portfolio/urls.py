from django.urls import path
from django.contrib.auth import views as auth_views
from . import views
from . import views_wallet
from . import db_chart_views
import portfolio.views

urlpatterns = [
    path('', views.home, name='home'),
    path('dashboard/', views.dashboard, name='dashboard'),
    
    path('portfolios/', views.portfolio_list, name='portfolio_list'),
    path('portfolios/create/', views.portfolio_create, name='portfolio_create'),
    path('portfolios/<int:pk>/', views.portfolio_detail, name='portfolio_detail'),
    path('portfolios/<int:pk>/update/', views.portfolio_update, name='portfolio_update'),
    path('portfolios/<int:portfolio_id>/buy/', views.buy_stock, name='buy_stock'),
    path('portfolios/<int:portfolio_id>/sell/', views.sell_stock, name='sell_stock'),
    path('portfolios/<int:portfolio_id>/transactions/', views.portfolio_transactions, name='portfolio_transactions'),
    
    path('assets/', views.asset_list, name='asset_list'),
    path('assets/create/', views.asset_create, name='asset_create'),
    path('assets/<int:pk>/', views.asset_detail, name='asset_detail'),
    path('assets/<int:pk>/update/', views.asset_update, name='asset_update'),
    
    path('transactions/', views.transaction_list, name='transaction_list'),
    path('transactions/create/', views.transaction_create, name='transaction_create'),
    
    # Auth0 authentication
    path('register/', views.register, name='register'),
    path('login/', views.login_view, name='login'),
    path('logout/', views.logout_view, name='logout'),
    path('social/complete/auth0/', views.callback, name='callback'),
    
    # User profile
    path('profile/', views.user_profile, name='user_profile'),
    
    # Trang thị trường và API Kafka
    path('market/', views.market, name='market'),
    path('api/historical-data/<str:symbol>/', views.get_stock_historical_data, name='get_stock_historical_data'),
    
    # Kafka API endpoints
    path('api/kafka/stock-data/<str:symbol>/', views.get_kafka_stock_data, name='get_kafka_stock_data'),
    path('api/kafka/refresh-symbol/<str:symbol>/', views.refresh_kafka_symbol, name='refresh_kafka_symbol'),
    path('api/kafka/symbols/', views.get_stock_symbols, name='get_kafka_available_symbols'),
    
    # URLs cho ví điện tử
    path('wallet/', views_wallet.wallet, name='wallet'),
    path('wallet/deposit/', views_wallet.deposit_money, name='wallet_deposit'),
    path('wallet/withdraw/', views_wallet.withdraw_money, name='wallet_withdraw'),
    path('wallet/transactions/', views_wallet.wallet_transactions, name='wallet_transactions'),
    
    # Thêm paths tương thích với tên sử dụng trong templates
    path('wallet/deposit-money/', views_wallet.deposit_money, name='deposit_money'),
    path('wallet/withdraw-money/', views_wallet.withdraw_money, name='withdraw_money'),
    
    # Thêm URL pattern cho verify_deposit
    path('wallet/verify-deposit/', views_wallet.verify_deposit, name='verify_deposit'),
    path('wallet/verify-deposit/<str:transaction_id>/', views_wallet.verify_deposit, name='verify_deposit_with_id'),
    
    # Thêm đường dẫn cho các chức năng quản lý tài khoản ngân hàng
    path('wallet/bank-accounts/', views_wallet.bank_account_list, name='bank_account_list'),
    path('wallet/bank-accounts/add/', views_wallet.bank_account_create, name='bank_account_create'),
    path('wallet/bank-accounts/<int:pk>/update/', views_wallet.update_bank_account, name='bank_account_update'),
    path('wallet/bank-accounts/<int:pk>/delete/', views_wallet.delete_bank_account, name='bank_account_delete'),
    path('wallet/bank-accounts/<int:pk>/set-default/', views_wallet.set_default_bank_account, name='bank_account_set_default'),
    
    # Thêm URLs mới với tên hàm đúng để khắc phục lỗi
    path('wallet/wallet-transactions/', views_wallet.wallet_transactions, name='wallet_transactions_list'),
    
    # API cho giao dịch
    path('api/get_stock_price/<str:symbol>/', views.get_stock_price, name='get_stock_price'),
    path('api/create_asset_from_symbol/', views.create_asset_from_symbol, name='create_asset_from_symbol'),
    
    # Thêm URL mới để khớp với frontend
    path('api/stock-price/<str:symbol>/', views.get_stock_price, name='get_stock_price_alt'),
    path('api/create-asset/', views.create_asset_from_symbol, name='create_asset_from_symbol_alt'),
    path('api/stock-symbols-info/', views.get_stock_symbols_info, name='get_stock_symbols_info_alt'),
    
    # AI Chat API
    path('api/chat/', views.ai_chat_api, name='ai_chat_api'),
    # Thêm đường dẫn mới cho API Chat AI để phù hợp với frontend
    path('api/ai-chat/', views.ai_chat_api, name='ai_chat_api_alt'),
    
    # Debug và admin
    path('debug/assets/', views.debug_assets, name='debug_assets'),
    path('sync_assets/', views.sync_assets, name='sync_assets'),
    path('update_stock_prices/', views.update_stock_prices, name='update_stock_prices'),
    path('api/stock_symbols/', views.get_stock_symbols, name='get_stock_symbols'),
    path('api/get_stock_symbols_info/', views.get_stock_symbols_info, name='get_stock_symbols_info'),
    
    # Debug API 
    path('api/debug/price-board/', views.debug_price_board, name='debug_price_board'),
    
    # Trang test đơn giản
    path('market/simple/', views.simple_market, name='simple_market'),
    
    # API để lấy dữ liệu dạng raw
    path('api/debug/raw-data/', views.raw_data_api, name='raw_data_api'),
    
    # Trang thị trường mới (không sử dụng JavaScript phức tạp)
    path('market/new/', views.new_market, name='new_market'),
]
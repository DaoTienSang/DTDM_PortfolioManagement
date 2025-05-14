from django.contrib import admin
from django.utils.html import format_html
from django.db.models import Sum, F
from django.utils import timezone
from .models import (
    User, Portfolio, Asset, PortfolioAsset, Transaction,
    Wallet, BankAccount, WalletTransaction
)

# Đăng ký User vào admin
@admin.register(User)
class UserAdmin(admin.ModelAdmin):
    list_display = ('username', 'email', 'first_name', 'last_name', 'phone', 'date_joined', 'is_staff', 'is_active')
    list_filter = ('is_staff', 'is_active', 'gender', 'date_joined')
    search_fields = ('username', 'email', 'first_name', 'last_name', 'phone')
    readonly_fields = ('date_joined', 'last_login')
    fieldsets = (
        ('Thông tin cá nhân', {'fields': ('username', 'email', 'first_name', 'last_name', 'password')}),
        ('Liên hệ', {'fields': ('phone', 'address')}),
        ('Thông tin khác', {'fields': ('gender', 'profile_picture', 'profile_picture_url')}),
        ('Auth0', {'fields': ('auth0_user_id',)}),
        ('Quyền hạn', {'fields': ('is_active', 'is_staff', 'is_superuser', 'groups', 'user_permissions')}),
        ('Thời gian', {'fields': ('date_joined', 'last_login')}),
    )

# Đăng ký Portfolio vào admin
class PortfolioAssetInline(admin.TabularInline):
    model = PortfolioAsset
    extra = 0
    readonly_fields = ('current_value', 'profit_loss')

@admin.register(Portfolio)
class PortfolioAdmin(admin.ModelAdmin):
    list_display = ('name', 'user', 'risk_tolerance', 'total_value_display', 'profit_loss_display', 'created_at')
    list_filter = ('risk_tolerance', 'created_at')
    search_fields = ('name', 'user__username', 'description')
    inlines = [PortfolioAssetInline]
    
    def total_value_display(self, obj):
        return f"{obj.total_value:,.2f} VND"
    total_value_display.short_description = "Tổng giá trị"
    
    def profit_loss_display(self, obj):
        value = obj.profit_loss
        color = "green" if value >= 0 else "red"
        return format_html('<span style="color: {};">{:,.2f} VND ({}%)</span>', 
                          color, value, round(obj.profit_loss_percentage, 2))
    profit_loss_display.short_description = "Lãi/Lỗ"

# Đăng ký Asset vào admin
@admin.register(Asset)
class AssetAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'name', 'type', 'sector', 'current_price', 'last_updated')
    list_filter = ('type', 'sector', 'last_updated')
    search_fields = ('symbol', 'name', 'description')
    readonly_fields = ('last_updated',)
    
    actions = ['update_prices']
    
    def update_prices(self, request, queryset):
        # Hành động cập nhật giá cho tài sản được chọn
        updated = 0
        for asset in queryset:
            # Gọi hàm cập nhật giá (cần triển khai)
            # asset.update_price_from_market()
            updated += 1
        self.message_user(request, f"Đã cập nhật giá cho {updated} tài sản.")
    update_prices.short_description = "Cập nhật giá từ thị trường"

# Đăng ký PortfolioAsset vào admin
@admin.register(PortfolioAsset)
class PortfolioAssetAdmin(admin.ModelAdmin):
    list_display = ('asset', 'portfolio', 'quantity', 'average_price', 'current_value', 'profit_loss_display')
    list_filter = ('asset__type', 'portfolio__user')
    search_fields = ('asset__symbol', 'asset__name', 'portfolio__name')
    
    def profit_loss_display(self, obj):
        value = obj.profit_loss
        color = "green" if value >= 0 else "red"
        profit_percentage = 0
        if obj.quantity * obj.average_price > 0:
            profit_percentage = (value / (obj.quantity * obj.average_price)) * 100
        return format_html('<span style="color: {};">{:,.2f} VND ({}%)</span>', 
                          color, value, round(profit_percentage, 2))
    profit_loss_display.short_description = "Lãi/Lỗ"

# Đăng ký Transaction vào admin
@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    list_display = ('transaction_type', 'asset', 'portfolio', 'quantity', 'price', 'total_amount', 'transaction_date')
    list_filter = ('transaction_type', 'transaction_date', 'asset__type')
    search_fields = ('asset__symbol', 'portfolio__name', 'portfolio__user__username')
    date_hierarchy = 'transaction_date'

# Đăng ký Wallet vào admin
@admin.register(Wallet)
class WalletAdmin(admin.ModelAdmin):
    list_display = ('user', 'balance', 'last_updated')
    search_fields = ('user__username', 'user__email')
    readonly_fields = ('created_at', 'last_updated')

# Đăng ký BankAccount vào admin
@admin.register(BankAccount)
class BankAccountAdmin(admin.ModelAdmin):
    list_display = ('user', 'get_bank_name', 'account_name', 'account_number', 'is_default')
    list_filter = ('bank_name', 'is_default')
    search_fields = ('user__username', 'account_name', 'account_number')
    
    def get_bank_name(self, obj):
        if obj.bank_name == 'other' and obj.other_bank_name:
            return obj.other_bank_name
        return obj.get_bank_name_display()
    get_bank_name.short_description = "Ngân hàng"

# Đăng ký WalletTransaction vào admin
@admin.register(WalletTransaction)
class WalletTransactionAdmin(admin.ModelAdmin):
    list_display = ('user', 'type', 'amount', 'status', 'payment_method', 'created_at', 'completed_at')
    list_filter = ('type', 'status', 'payment_method', 'created_at')
    search_fields = ('user__username', 'transaction_id', 'reference_id')
    readonly_fields = ('created_at', 'updated_at', 'completed_at')
    date_hierarchy = 'created_at'
    
    # Thêm bộ lọc tùy chỉnh để chỉ hiển thị các giao dịch chờ xử lý
    list_filter = [
        ('type', admin.ChoicesFieldListFilter),
        ('status', admin.ChoicesFieldListFilter),
        ('payment_method', admin.ChoicesFieldListFilter),
        'created_at',
        ('user', admin.RelatedOnlyFieldListFilter),
    ]
    
    # Thêm chức năng lọc nhanh cho các giao dịch rút tiền đang chờ xét duyệt
    def get_queryset(self, request):
        qs = super().get_queryset(request)
        # Lọc theo tham số từ URL
        pending_withdrawals = request.GET.get('pending_withdrawals')
        if pending_withdrawals:
            qs = qs.filter(type='withdraw', status='pending')
        return qs
    
    def changelist_view(self, request, extra_context=None):
        # Thêm nút lọc nhanh cho các giao dịch rút tiền đang chờ xét duyệt
        if extra_context is None:
            extra_context = {}
        
        # Đếm số lượng giao dịch rút tiền đang chờ xét duyệt
        pending_withdrawals_count = self.get_queryset(request).filter(
            type='withdraw', status='pending'
        ).count()
        
        extra_context['pending_withdrawals_count'] = pending_withdrawals_count
        
        # Thêm thông tin tổng tiền nạp/rút
        try:
            qs = self.get_queryset(request)
            
            deposit_sum = qs.filter(type='deposit', status='completed').aggregate(Sum('amount'))
            withdraw_sum = qs.filter(type='withdraw', status='completed').aggregate(Sum('amount'))
            
            extra_context['deposit_sum'] = deposit_sum['amount__sum'] or 0
            extra_context['withdraw_sum'] = withdraw_sum['amount__sum'] or 0
            
            # Thống kê số tiền rút đang chờ xét duyệt
            pending_withdraw_sum = qs.filter(
                type='withdraw', status='pending'
            ).aggregate(Sum('amount'))
            extra_context['pending_withdraw_sum'] = pending_withdraw_sum['amount__sum'] or 0
            
        except (AttributeError, KeyError):
            pass
        
        return super().changelist_view(request, extra_context)
    
    actions = ['approve_transactions', 'reject_transactions']
    
    def approve_transactions(self, request, queryset):
        # Đếm giao dịch đã xử lý theo từng loại
        deposit_count = 0
        withdraw_count = 0
        
        # Chỉ xử lý các giao dịch đang chờ xử lý
        pending_transactions = queryset.filter(status='pending')
        
        for transaction in pending_transactions:
            transaction.status = 'completed'
            transaction.completed_at = timezone.now()
            
            # Cập nhật số dư ví
            wallet = transaction.wallet
            
            if transaction.type == 'deposit':
                wallet.balance += transaction.net_amount
                deposit_count += 1
            elif transaction.type == 'withdraw':
                # Kiểm tra lại số dư một lần nữa trước khi xử lý
                # Chú ý: đối với rút tiền, số tiền trừ từ ví là số tiền gốc (amount), không phải net_amount
                if wallet.balance >= transaction.amount:
                    wallet.balance -= transaction.amount
                    withdraw_count += 1
                else:
                    # Nếu số dư không đủ thì từ chối giao dịch
                    transaction.status = 'failed'
                    transaction.notes += " | Từ chối tự động: Số dư không đủ."
            
            wallet.save()
            transaction.save()
        
        message_parts = []
        if deposit_count > 0:
            message_parts.append(f"phê duyệt {deposit_count} giao dịch nạp tiền")
        if withdraw_count > 0:
            message_parts.append(f"phê duyệt {withdraw_count} giao dịch rút tiền")
        
        if message_parts:
            self.message_user(request, f"Đã {' và '.join(message_parts)} thành công.")
        else:
            self.message_user(request, "Không có giao dịch nào được xử lý.")
            
    approve_transactions.short_description = "Phê duyệt giao dịch đã chọn"
    
    def reject_transactions(self, request, queryset):
        # Chỉ xử lý các giao dịch đang chờ xử lý
        count = queryset.filter(status='pending').update(
            status='failed',
            notes=F('notes') + " | Từ chối bởi quản trị viên."
        )
        self.message_user(request, f"Đã từ chối {count} giao dịch.")
    reject_transactions.short_description = "Từ chối giao dịch đã chọn" 
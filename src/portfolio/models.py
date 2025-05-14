# Start of Selection
from django.db import models
from django.contrib.auth.models import AbstractUser
from decimal import Decimal
from django.utils import timezone

# Mô hình người dùng
class User(AbstractUser):
    # Số điện thoại
    phone = models.CharField(max_length=15, blank=True)
    # Địa chỉ
    address = models.TextField(blank=True)
    # Giới tính
    GENDER_CHOICES = [
        ('male', 'Nam'),
        ('female', 'Nữ'),
        ('other', 'Khác'),
    ]
    gender = models.CharField(max_length=10, choices=GENDER_CHOICES, blank=True)
    # Profile picture - URL from Auth0 or uploaded image
    profile_picture = models.ImageField(upload_to='profile_pictures/', blank=True, null=True)
    # Profile picture URL (for Auth0)
    # profile_picture_url = models.URLField(blank=True, null=True)
    profile_picture_url = models.URLField(max_length=2048, blank=True, null=True)
    # Auth0 User ID
    auth0_user_id = models.CharField(max_length=2048, blank=True, null=True, unique=True)
    
    class Meta:
        # Tạo chỉ mục cho email và username
        indexes = [
            models.Index(fields=['email']),
            models.Index(fields=['username'])
        ]

    @property
    def get_profile_picture(self):
        """Return profile picture URL or uploaded image"""
        if self.profile_picture and self.profile_picture.url:
            return self.profile_picture.url
        elif self.profile_picture_url:
            return self.profile_picture_url
        return None

# Mô hình danh mục đầu tư
class Portfolio(models.Model):
    # Lựa chọn mức độ rủi ro
    RISK_CHOICES = [
        ('low', 'Thấp'),
        ('medium', 'Trung bình'),
        ('high', 'Cao'),
    ]
    
    # Tên danh mục
    name = models.CharField(max_length=100, verbose_name="Tên danh mục")
    # Người dùng sở hữu danh mục
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='portfolios')
    # Mô tả danh mục
    description = models.TextField(blank=True, verbose_name="Mô tả")
    # Mục tiêu đầu tư
    investment_goal = models.CharField(max_length=200, blank=True, verbose_name="Mục tiêu đầu tư")
    # Giá trị mục tiêu
    target_value = models.DecimalField(max_digits=15, decimal_places=2, default=0, verbose_name="Giá trị mục tiêu")
    # Mức độ rủi ro
    risk_tolerance = models.CharField(max_length=10, choices=RISK_CHOICES, default='medium', verbose_name="Mức độ rủi ro")
    # Thời gian tạo
    created_at = models.DateTimeField(auto_now_add=True)
    # Thời gian cập nhật
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        # Tạo chỉ mục cho người dùng
        indexes = [
            models.Index(fields=['user'])
        ]
        # Tên hiển thị
        verbose_name = "Danh mục đầu tư"
        verbose_name_plural = "Danh mục đầu tư"

    # Trả về tên danh mục
    def __str__(self):
        return self.name

    # Tính tổng giá trị danh mục
    @property
    def total_value(self):
        return sum(asset.current_value for asset in self.portfolioasset_set.all())

    # Tính tổng chi phí danh mục
    @property
    def total_cost(self):
        return sum(asset.quantity * asset.average_price for asset in self.portfolioasset_set.all())

    # Tính lãi/lỗ danh mục
    @property
    def profit_loss(self):
        return self.total_value - self.total_cost

    # Tính lãi/lỗ danh mục theo phần trăm
    @property
    def profit_loss_percentage(self):
        if self.total_cost == 0:
            return 0
        return (self.profit_loss / self.total_cost) * 100

    # Tính tiến độ đạt được mục tiêu
    @property
    def target_progress(self):
        if self.target_value == 0:
            return 0
        return (self.total_value / self.target_value) * 100

# Mô hình tài sản
class Asset(models.Model):
    # Lựa chọn loại tài sản
    ASSET_TYPES = [
        ('stock', 'Cổ phiếu'),
        ('bond', 'Trái phiếu'),
        ('fund', 'Quỹ đầu tư'),
    ]
    
    # Mã tài sản
    symbol = models.CharField(max_length=10, unique=True)
    # Tên tài sản
    name = models.CharField(max_length=100)
    # Loại tài sản
    type = models.CharField(max_length=10, choices=ASSET_TYPES)
    # Ngành nghề
    sector = models.CharField(max_length=50)
    # Mô tả
    description = models.TextField(blank=True)
    # Giá hiện tại
    current_price = models.DecimalField(max_digits=15, decimal_places=2)
    # Thời gian cập nhật giá
    last_updated = models.DateTimeField(auto_now=True)

    class Meta:
        # Tạo chỉ mục cho mã tài sản, loại tài sản và ngành nghề
        indexes = [
            models.Index(fields=['symbol']),
            models.Index(fields=['type']),
            models.Index(fields=['sector'])
        ]

# Mô hình tài sản trong danh mục
class PortfolioAsset(models.Model):
    # Danh mục sở hữu
    portfolio = models.ForeignKey(Portfolio, on_delete=models.CASCADE)
    # Tài sản sở hữu
    asset = models.ForeignKey(Asset, on_delete=models.CASCADE)
    # Số lượng
    quantity = models.IntegerField(default=0)
    # Giá trung bình
    average_price = models.DecimalField(max_digits=15, decimal_places=2)
    # Giá trị hiện tại
    current_value = models.DecimalField(max_digits=15, decimal_places=2)
    # Lãi/lỗ
    profit_loss = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    
    class Meta:
        # Tạo chỉ mục cho danh mục và tài sản
        indexes = [
            models.Index(fields=['portfolio']),
            models.Index(fields=['asset'])
        ]

    # Cập nhật giá trị và lãi/lỗ
    def update_values(self):
        """Cập nhật giá trị và lãi/lỗ của tài sản"""
        self.current_value = self.quantity * self.asset.current_price
        self.profit_loss = self.current_value - (self.quantity * self.average_price)
        self.save()

    # Tính lại giá trung bình
    def recalculate_average_price(self):
        """Tính lại giá trung bình sau khi có giao dịch mới"""
        transactions = Transaction.objects.filter(
            portfolio=self.portfolio,
            asset=self.asset,
            transaction_type='buy'
        )
        total_quantity = sum(t.quantity for t in transactions)
        total_cost = sum(t.quantity * t.price for t in transactions)
        
        if total_quantity > 0:
            self.average_price = total_cost / total_quantity
        self.save()

# Mô hình giao dịch
class Transaction(models.Model):
    # Lựa chọn loại giao dịch
    TYPE_CHOICES = [
        ('buy', 'Mua'),
        ('sell', 'Bán'),
    ]
    
    # Danh mục sở hữu
    portfolio = models.ForeignKey(Portfolio, on_delete=models.CASCADE)
    # Tài sản sở hữu
    asset = models.ForeignKey(Asset, on_delete=models.CASCADE)
    # Loại giao dịch
    transaction_type = models.CharField(max_length=4, choices=TYPE_CHOICES)
    # Số lượng
    quantity = models.IntegerField()
    # Giá
    price = models.DecimalField(max_digits=15, decimal_places=2)
    # Tổng giá trị
    total_amount = models.DecimalField(max_digits=15, decimal_places=2)
    # Thời gian giao dịch
    transaction_date = models.DateTimeField()
    # Ghi chú
    notes = models.TextField(blank=True)
    
    class Meta:
        # Tạo chỉ mục cho danh mục, tài sản, thời gian giao dịch và loại giao dịch
        indexes = [
            models.Index(fields=['portfolio']),
            models.Index(fields=['asset']),
            models.Index(fields=['transaction_date']),
            models.Index(fields=['transaction_type'])
        ]

    # Trả về thông tin giao dịch
    def __str__(self):
        return f"{self.get_transaction_type_display()} {self.asset.symbol}"

    # Lưu giao dịch
    def save(self, *args, **kwargs):
        # Tính tổng giá trị giao dịch
        if not self.total_amount:
            self.total_amount = self.quantity * self.price

        # Lưu giao dịch
        super().save(*args, **kwargs)

        # Cập nhật PortfolioAsset
        portfolio_asset, created = PortfolioAsset.objects.get_or_create(
            portfolio=self.portfolio,
            asset=self.asset,
            defaults={
                'quantity': 0,
                'average_price': 0,
                'current_value': 0,
                'profit_loss': 0
            }
        )

        # Cập nhật số lượng
        if self.transaction_type == 'buy':
            portfolio_asset.quantity += self.quantity
        else:  # sell
            portfolio_asset.quantity -= self.quantity

        # Tính lại giá trung bình nếu là giao dịch mua
        if self.transaction_type == 'buy':
            portfolio_asset.recalculate_average_price()

        # Cập nhật giá trị hiện tại và lãi/lỗ
        portfolio_asset.update_values()

# ===== Mô hình cho ví điện tử =====
# Mô hình ví điện tử
class Wallet(models.Model):
    # Người dùng sở hữu
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='wallet')
    # Số dư
    balance = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    # Thời gian tạo
    created_at = models.DateTimeField(auto_now_add=True)
    # Thời gian cập nhật
    last_updated = models.DateTimeField(auto_now=True)

    # Trả về thông tin ví
    def __str__(self):
        return f"Ví của {self.user.username}"
    
    class Meta:
        # Tên hiển thị
        verbose_name = "Ví điện tử"
        verbose_name_plural = "Ví điện tử"

# Mô hình tài khoản ngân hàng
class BankAccount(models.Model):
    # Lựa chọn ngân hàng
    BANK_CHOICES = [
        ('vietcombank', 'Vietcombank'),
        ('techcombank', 'Techcombank'),
        ('bidv', 'BIDV'),
        ('vietinbank', 'Vietinbank'),
        ('mbbank', 'MB Bank'),
        ('tpbank', 'TPBank'),
        ('acb', 'ACB'),
        ('sacombank', 'Sacombank'),
        ('vpbank', 'VPBank'),
        ('other', 'Ngân hàng khác'),
    ]
    
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='bank_accounts')
    bank_name = models.CharField(max_length=100, choices=BANK_CHOICES, verbose_name="Tên ngân hàng")
    other_bank_name = models.CharField(max_length=100, blank=True, null=True, verbose_name="Tên ngân hàng khác")
    account_name = models.CharField(max_length=255, verbose_name="Tên chủ tài khoản")
    account_number = models.CharField(max_length=50, verbose_name="Số tài khoản")
    branch = models.CharField(max_length=255, blank=True, null=True, verbose_name="Chi nhánh")
    is_default = models.BooleanField(default=False, verbose_name="Là tài khoản mặc định")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = "Tài khoản ngân hàng"
        verbose_name_plural = "Tài khoản ngân hàng"
        indexes = [
            models.Index(fields=['user']),
            models.Index(fields=['account_number']),
        ]
        
    def __str__(self):
        display_name = self.other_bank_name if self.bank_name == 'other' and self.other_bank_name else self.get_bank_name_display()
        return f"{display_name} - {self.account_number}"
    
    def save(self, *args, **kwargs):
        # Nếu đánh dấu tài khoản là mặc định, đặt các tài khoản khác là không mặc định
        if self.is_default:
            BankAccount.objects.filter(user=self.user).update(is_default=False)
        
        # Nếu là tài khoản đầu tiên, đặt làm mặc định
        elif not BankAccount.objects.filter(user=self.user).exists():
            self.is_default = True
            
        super().save(*args, **kwargs)

class WalletTransaction(models.Model):
    TYPE_CHOICES = [
        ('deposit', 'Nạp tiền'),
        ('withdraw', 'Rút tiền'),
    ]
    
    STATUS_CHOICES = [
        ('pending', 'Đang xử lý'),
        ('completed', 'Hoàn thành'),
        ('failed', 'Thất bại'),
        ('cancelled', 'Đã hủy'),
    ]
    
    PAYMENT_METHOD_CHOICES = [
        ('bank_transfer', 'Chuyển khoản ngân hàng'),
        ('momo', 'Ví MoMo'),
        ('vnpay', 'VNPay'),
        ('other', 'Khác'),
    ]
    
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='wallet_transactions')
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name='transactions')
    type = models.CharField(max_length=10, choices=TYPE_CHOICES, verbose_name="Loại giao dịch")
    amount = models.DecimalField(max_digits=15, decimal_places=2, verbose_name="Số tiền")
    fee = models.DecimalField(max_digits=15, decimal_places=2, default=0, verbose_name="Phí giao dịch")
    net_amount = models.DecimalField(max_digits=15, decimal_places=2, verbose_name="Số tiền thực nhận/thanh toán")
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='pending', verbose_name="Trạng thái")
    bank_account = models.ForeignKey(BankAccount, on_delete=models.SET_NULL, null=True, blank=True, related_name='transactions', verbose_name="Tài khoản ngân hàng")
    payment_method = models.CharField(max_length=50, choices=PAYMENT_METHOD_CHOICES, verbose_name="Phương thức thanh toán")
    transaction_id = models.CharField(max_length=100, unique=True, verbose_name="Mã giao dịch")
    reference_id = models.CharField(max_length=100, blank=True, null=True, verbose_name="Mã tham chiếu")
    notes = models.TextField(blank=True, null=True, verbose_name="Ghi chú")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    completed_at = models.DateTimeField(blank=True, null=True, verbose_name="Thời gian hoàn thành")
    
    class Meta:
        verbose_name = "Giao dịch ví điện tử"
        verbose_name_plural = "Giao dịch ví điện tử"
        indexes = [
            models.Index(fields=['user']),
            models.Index(fields=['wallet']),
            models.Index(fields=['type']),
            models.Index(fields=['status']),
            models.Index(fields=['transaction_id']),
            models.Index(fields=['created_at']),
        ]
        
    def __str__(self):
        return f"{self.get_type_display()} - {self.amount} - {self.get_status_display()}"
    
    def save(self, *args, **kwargs):
        # Cập nhật số dư ví khi giao dịch hoàn thành
        if self.status == 'completed' and not self.completed_at:
            self.completed_at = timezone.now()
            
            # Cập nhật số dư ví
            if self.type == 'deposit':
                self.wallet.balance += self.amount
            elif self.type == 'withdraw':
                self.wallet.balance -= self.amount
                
            self.wallet.save()
            
        super().save(*args, **kwargs)
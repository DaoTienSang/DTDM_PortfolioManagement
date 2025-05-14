from django import forms
from django.contrib.auth.forms import UserCreationForm
from .models import User, Portfolio, Asset, Transaction, BankAccount, WalletTransaction, Wallet
from django.utils import timezone
from django.core.validators import MinValueValidator
from decimal import Decimal

class UserRegistrationForm(UserCreationForm):
    email = forms.EmailField()
    
    class Meta:
        model = User
        fields = ['username', 'email', 'password1', 'password2']

class UserProfileForm(forms.ModelForm):
    class Meta:
        model = User
        fields = ['first_name', 'last_name', 'email', 'phone', 'address', 'gender', 'profile_picture']
        widgets = {
            'first_name': forms.TextInput(attrs={
                'class': 'form-control',
                'placeholder': 'Nhập tên của bạn'
            }),
            'last_name': forms.TextInput(attrs={
                'class': 'form-control',
                'placeholder': 'Nhập họ của bạn'
            }),
            'email': forms.EmailInput(attrs={
                'class': 'form-control',
                'placeholder': 'Nhập email của bạn'
            }),
            'phone': forms.TextInput(attrs={
                'class': 'form-control',
                'placeholder': 'Nhập số điện thoại của bạn'
            }),
            'address': forms.Textarea(attrs={
                'class': 'form-control',
                'rows': 3,
                'placeholder': 'Nhập địa chỉ của bạn'
            }),
            'gender': forms.Select(attrs={
                'class': 'form-control'
            }),
            'profile_picture': forms.FileInput(attrs={
                'class': 'form-control',
                'id': 'profilePictureInput'
            }),
        }

class PortfolioForm(forms.ModelForm):
    class Meta:
        model = Portfolio
        fields = ['name', 'description', 'investment_goal', 'target_value', 'risk_tolerance']
        widgets = {
            'name': forms.TextInput(attrs={
                'class': 'form-control',
                'placeholder': 'Nhập tên danh mục đầu tư'
            }),
            'description': forms.Textarea(attrs={
                'class': 'form-control',
                'rows': 3,
                'placeholder': 'Mô tả về danh mục đầu tư của bạn'
            }),
            'investment_goal': forms.TextInput(attrs={
                'class': 'form-control',
                'placeholder': 'Ví dụ: Tăng trưởng dài hạn, Thu nhập cổ tức...'
            }),
            'target_value': forms.NumberInput(attrs={
                'class': 'form-control',
                'min': '0',
                'step': '1000000'
            }),
            'risk_tolerance': forms.Select(attrs={
                'class': 'form-control'
            })
        }
        
    def clean_target_value(self):
        target_value = self.cleaned_data.get('target_value')
        if target_value < 0:
            raise forms.ValidationError("Giá trị mục tiêu không thể là số âm")
        return target_value

    def clean_name(self):
        name = self.cleaned_data.get('name')
        if len(name) < 3:
            raise forms.ValidationError("Tên danh mục phải có ít nhất 3 ký tự")
        return name

class AssetForm(forms.ModelForm):
    class Meta:
        model = Asset
        fields = ['symbol', 'name', 'type', 'sector', 'description', 'current_price']

class TransactionForm(forms.ModelForm):
    class Meta:
        model = Transaction
        fields = ['portfolio', 'asset', 'transaction_type', 'quantity', 'price', 'transaction_date', 'notes']
        widgets = {
            'transaction_date': forms.DateTimeInput(
                attrs={'type': 'datetime-local'},
                format='%Y-%m-%dT%H:%M'
            ),
            'notes': forms.Textarea(attrs={'rows': 3}),
            'portfolio': forms.HiddenInput(),
            'asset': forms.Select(attrs={
                'class': 'form-select',
                'id': 'id_asset',
                'placeholder': 'Chọn mã cổ phiếu'
            })
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Cho phép field 'asset' là không bắt buộc, vì chúng ta sẽ xử lý thông qua symbol
        self.fields['asset'].required = False
        
        if 'initial' in kwargs and 'portfolio' in kwargs['initial']:
            portfolio = kwargs['initial']['portfolio']
            if kwargs['initial'].get('transaction_type') == 'sell':
                # Chỉ hiển thị các tài sản có trong danh mục khi bán
                self.fields['asset'].queryset = Asset.objects.filter(
                    portfolioasset__portfolio=portfolio,
                    portfolioasset__quantity__gt=0
                ).distinct()
            else:
                self.fields['asset'].queryset = Asset.objects.all()

    def clean(self):
        cleaned_data = super().clean()
        quantity = cleaned_data.get('quantity')
        price = cleaned_data.get('price')
        
        if quantity and quantity <= 0:
            raise forms.ValidationError('Số lượng phải lớn hơn 0')
        
        if price and price <= 0:
            raise forms.ValidationError('Giá phải lớn hơn 0')
            
        # Không validate asset vì asset sẽ được set từ symbol 
        # trong view trước khi lưu transaction
        if 'asset' not in cleaned_data:
            cleaned_data['asset'] = None
        
        return cleaned_data

# ===== Forms cho ví điện tử =====
class BankAccountForm(forms.ModelForm):
    class Meta:
        model = BankAccount
        fields = ['bank_name', 'other_bank_name', 'account_name', 'account_number', 'branch', 'is_default']
        widgets = {
            'bank_name': forms.Select(attrs={
                'class': 'form-select',
                'id': 'bankName'
            }),
            'other_bank_name': forms.TextInput(attrs={
                'class': 'form-control',
                'id': 'otherBankName',
                'placeholder': 'Nhập tên ngân hàng'
            }),
            'account_name': forms.TextInput(attrs={
                'class': 'form-control',
                'placeholder': 'Nhập tên chủ tài khoản'
            }),
            'account_number': forms.TextInput(attrs={
                'class': 'form-control',
                'placeholder': 'Nhập số tài khoản'
            }),
            'branch': forms.TextInput(attrs={
                'class': 'form-control',
                'placeholder': 'Nhập chi nhánh (không bắt buộc)'
            }),
            'is_default': forms.CheckboxInput(attrs={
                'class': 'form-check-input'
            })
        }
        
    def clean_account_number(self):
        account_number = self.cleaned_data.get('account_number')
        if not account_number.isdigit():
            raise forms.ValidationError("Số tài khoản chỉ được chứa các chữ số")
        return account_number
        
    def clean(self):
        cleaned_data = super().clean()
        bank_name = cleaned_data.get('bank_name')
        other_bank_name = cleaned_data.get('other_bank_name')
        
        if bank_name == 'other' and not other_bank_name:
            raise forms.ValidationError({'other_bank_name': "Vui lòng nhập tên ngân hàng"})
            
        return cleaned_data

class DepositForm(forms.Form):
    amount = forms.DecimalField(
        label="Số tiền muốn nạp",
        min_value=50000,
        max_digits=15,
        decimal_places=0,
        widget=forms.NumberInput(attrs={
            'class': 'form-control form-control-lg',
            'placeholder': 'Nhập số tiền muốn nạp',
            'min': '50000',
            'step': '50000'
        })
    )
    
    payment_method = forms.ChoiceField(
        label="Phương thức thanh toán",
        choices=WalletTransaction.PAYMENT_METHOD_CHOICES,
        widget=forms.RadioSelect(attrs={
            'class': 'form-check-input'
        })
    )
    
    bank_account = forms.ModelChoiceField(
        label="Tài khoản ngân hàng",
        queryset=BankAccount.objects.none(),
        required=False,
        widget=forms.RadioSelect(attrs={
            'class': 'form-check-input'
        })
    )
    
    agree_terms = forms.BooleanField(
        label="Tôi đồng ý với điều khoản nạp tiền",
        widget=forms.CheckboxInput(attrs={
            'class': 'form-check-input',
            'id': 'agreeTerms'
        })
    )
    
    # Các trường cho tài khoản ngân hàng mới
    new_bank_name = forms.ChoiceField(
        label="Ngân hàng",
        choices=BankAccount.BANK_CHOICES,
        required=False,
        widget=forms.Select(attrs={
            'class': 'form-select',
            'id': 'bankName'
        })
    )
    
    new_other_bank_name = forms.CharField(
        label="Tên ngân hàng khác",
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'id': 'otherBankName',
            'placeholder': 'Nhập tên ngân hàng'
        })
    )
    
    new_account_name = forms.CharField(
        label="Chủ tài khoản",
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'id': 'accountName',
            'placeholder': 'Nhập tên chủ tài khoản'
        })
    )
    
    new_account_number = forms.CharField(
        label="Số tài khoản",
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'id': 'accountNumber',
            'placeholder': 'Nhập số tài khoản'
        })
    )
    
    new_branch = forms.CharField(
        label="Chi nhánh",
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'id': 'branch',
            'placeholder': 'Nhập chi nhánh (không bắt buộc)'
        })
    )
    
    new_is_default = forms.BooleanField(
        label="Đặt làm tài khoản mặc định",
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={
            'class': 'form-check-input',
            'id': 'defaultAccount'
        })
    )
    
    def __init__(self, user, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['bank_account'].queryset = BankAccount.objects.filter(user=user).order_by('-is_default')
        self.user = user
        
    def clean(self):
        cleaned_data = super().clean()
        payment_method = cleaned_data.get('payment_method')
        bank_account = cleaned_data.get('bank_account')
        
        # Nếu phương thức thanh toán là chuyển khoản ngân hàng,
        # cần có tài khoản ngân hàng hoặc thông tin tài khoản mới
        if payment_method == 'bank_transfer' and not bank_account:
            # Kiểm tra xem đã nhập đủ thông tin tài khoản mới chưa
            new_bank_name = cleaned_data.get('new_bank_name')
            new_account_name = cleaned_data.get('new_account_name')
            new_account_number = cleaned_data.get('new_account_number')
            
            if not (new_bank_name and new_account_name and new_account_number):
                if BankAccount.objects.filter(user=self.user).exists():
                    self.add_error('bank_account', 'Vui lòng chọn tài khoản ngân hàng')
                else:
                    self.add_error(None, 'Vui lòng thêm thông tin tài khoản ngân hàng')
        
        return cleaned_data

class WithdrawForm(forms.Form):
    amount = forms.DecimalField(
        label="Số tiền muốn rút",
        min_value=100000,
        max_digits=15,
        decimal_places=0,
        widget=forms.NumberInput(attrs={
            'class': 'form-control form-control-lg',
            'placeholder': 'Nhập số tiền muốn rút',
            'min': '100000',
            'step': '50000'
        })
    )
    
    bank_account = forms.ModelChoiceField(
        label="Tài khoản ngân hàng",
        queryset=BankAccount.objects.none(),
        widget=forms.RadioSelect(attrs={
            'class': 'form-check-input'
        })
    )
    
    notes = forms.CharField(
        label="Ghi chú",
        required=False,
        widget=forms.Textarea(attrs={
            'class': 'form-control',
            'id': 'withdrawNote',
            'rows': 3,
            'placeholder': 'Nhập ghi chú của bạn (nếu có)'
        })
    )
    
    agree_terms = forms.BooleanField(
        label="Tôi đồng ý với điều khoản rút tiền",
        widget=forms.CheckboxInput(attrs={
            'class': 'form-check-input',
            'id': 'agreeTerms'
        })
    )
    
    # Các trường cho tài khoản ngân hàng mới
    new_bank_name = forms.ChoiceField(
        label="Ngân hàng",
        choices=BankAccount.BANK_CHOICES,
        required=False,
        widget=forms.Select(attrs={
            'class': 'form-select',
            'id': 'bankName'
        })
    )
    
    new_other_bank_name = forms.CharField(
        label="Tên ngân hàng khác",
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'id': 'otherBankName',
            'placeholder': 'Nhập tên ngân hàng'
        })
    )
    
    new_account_name = forms.CharField(
        label="Chủ tài khoản",
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'id': 'accountName',
            'placeholder': 'Nhập tên chủ tài khoản'
        })
    )
    
    new_account_number = forms.CharField(
        label="Số tài khoản",
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'id': 'accountNumber',
            'placeholder': 'Nhập số tài khoản'
        })
    )
    
    new_branch = forms.CharField(
        label="Chi nhánh",
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'id': 'branch',
            'placeholder': 'Nhập chi nhánh (không bắt buộc)'
        })
    )
    
    new_is_default = forms.BooleanField(
        label="Đặt làm tài khoản mặc định",
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={
            'class': 'form-check-input',
            'id': 'defaultAccount'
        })
    )
    
    def __init__(self, user, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['bank_account'].queryset = BankAccount.objects.filter(user=user).order_by('-is_default')
        self.user = user
        
    def clean(self):
        cleaned_data = super().clean()
        amount = cleaned_data.get('amount')
        bank_account = cleaned_data.get('bank_account')
        
        # Nếu không có tài khoản ngân hàng nào được chọn
        if not bank_account:
            # Kiểm tra xem đã nhập đủ thông tin tài khoản mới chưa
            new_bank_name = cleaned_data.get('new_bank_name')
            new_account_name = cleaned_data.get('new_account_name')
            new_account_number = cleaned_data.get('new_account_number')
            
            if not (new_bank_name and new_account_name and new_account_number):
                if BankAccount.objects.filter(user=self.user).exists():
                    self.add_error('bank_account', 'Vui lòng chọn tài khoản ngân hàng')
                else:
                    self.add_error(None, 'Vui lòng thêm thông tin tài khoản ngân hàng')
        
        return cleaned_data
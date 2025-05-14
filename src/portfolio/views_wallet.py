from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.utils import timezone
from django.db.models import Sum
from django.core.paginator import Paginator
from datetime import timedelta
import uuid
from django.db import transaction
from django.utils.formats import date_format
from decimal import Decimal

from .models import Wallet, BankAccount, WalletTransaction
from .forms import BankAccountForm, DepositForm, WithdrawForm
from .templatetags.currency_filters import dinh_dang_tien
from .utils import generate_qr_code, check_paid

@login_required
def wallet(request):
    # Lấy hoặc tạo ví cho người dùng
    wallet, created = Wallet.objects.get_or_create(user=request.user)
    
    # Hiển thị thông báo về các giao dịch rút tiền đã được xử lý mà người dùng chưa xem
    # Lấy các giao dịch rút tiền đã hoàn thành và chưa được đánh dấu là đã thông báo
    completed_withdrawals = WalletTransaction.objects.filter(
        user=request.user,
        type='withdraw',
        status='completed',
        completed_at__isnull=False,
    ).exclude(
        notes__icontains='đã thông báo'
    ).order_by('-completed_at')[:5]  # Lấy 5 giao dịch gần nhất
    
    # Hiển thị thông báo cho từng giao dịch
    for withdrawal in completed_withdrawals:
        messages.success(
            request,
            f'Yêu cầu rút tiền #{withdrawal.transaction_id} của bạn đã được phê duyệt! '
            f'<br>Số tiền rút: {dinh_dang_tien(withdrawal.amount)} VNĐ'
            f'<br>Phí giao dịch: {dinh_dang_tien(withdrawal.fee)} VNĐ'
            f'<br>Số tiền thực nhận: {dinh_dang_tien(withdrawal.net_amount)} VNĐ'
            f'<br>Thời gian xử lý: {timezone.localtime(withdrawal.completed_at).strftime("%H:%M %d-%m-%Y")}'
        )
        # Đánh dấu là đã thông báo
        withdrawal.notes = f"{withdrawal.notes or ''} | đã thông báo"
        withdrawal.save(update_fields=['notes'])
    
    # Hiển thị thông báo cho các giao dịch bị từ chối
    rejected_withdrawals = WalletTransaction.objects.filter(
        user=request.user,
        type='withdraw',
        status='failed',
        updated_at__isnull=False,
    ).exclude(
        notes__icontains='đã thông báo'
    ).order_by('-updated_at')[:5]
    
    for withdrawal in rejected_withdrawals:
        messages.error(
            request,
            f'Yêu cầu rút tiền #{withdrawal.transaction_id} của bạn đã bị từ chối. '
            f'<br>Số tiền rút: {dinh_dang_tien(withdrawal.amount)} VNĐ'
            f'<br>Thời gian xử lý: {timezone.localtime(withdrawal.updated_at).strftime("%H:%M %d-%m-%Y")}'
            f'<br>Lý do: {withdrawal.notes if "Từ chối" in withdrawal.notes else "Không đáp ứng điều kiện rút tiền"}'
        )
        # Đánh dấu là đã thông báo
        withdrawal.notes = f"{withdrawal.notes or ''} | đã thông báo"
        withdrawal.save(update_fields=['notes'])
    
    # Lấy các giao dịch gần đây
    transactions = WalletTransaction.objects.filter(user=request.user).order_by('-created_at')[:5]
    
    # Các thống kê
    transaction_type = request.GET.get('type', None)
    from_date = request.GET.get('from_date', None)
    to_date = request.GET.get('to_date', None)
    
    # Bộ lọc
    filter_params = {'user': request.user}
    if transaction_type in ['deposit', 'withdraw']:
        filter_params['type'] = transaction_type
    
    if from_date:
        filter_params['created_at__gte'] = from_date
    
    if to_date:
        filter_params['created_at__lte'] = to_date
    
    # Tính tổng nạp và rút
    total_deposit = WalletTransaction.objects.filter(
        user=request.user,
        type='deposit',
        status='completed'
    ).aggregate(Sum('amount'))['amount__sum'] or 0
    
    total_withdraw = WalletTransaction.objects.filter(
        user=request.user,
        type='withdraw',
        status='completed'
    ).aggregate(Sum('amount'))['amount__sum'] or 0
    
    # Tính tổng nạp và rút trong 30 ngày gần đây
    thirty_days_ago = timezone.now() - timedelta(days=30)
    monthly_deposit = WalletTransaction.objects.filter(
        user=request.user,
        type='deposit',
        created_at__gte=thirty_days_ago,
        status='completed'
    ).aggregate(Sum('amount'))['amount__sum'] or 0
    
    monthly_withdraw = WalletTransaction.objects.filter(
        user=request.user,
        type='withdraw',
        created_at__gte=thirty_days_ago,
        status='completed'
    ).aggregate(Sum('amount'))['amount__sum'] or 0
    
    context = {
        'wallet': wallet,
        'transactions': transactions,
        'total_deposit': total_deposit,
        'total_withdraw': total_withdraw,
        'monthly_deposit': monthly_deposit,
        'monthly_withdraw': monthly_withdraw
    }
    
    return render(request, 'portfolio/wallet.html', context)

@login_required
def deposit_money(request):
    # Lấy hoặc tạo ví cho người dùng
    wallet, created = Wallet.objects.get_or_create(user=request.user)
    
    # Lấy danh sách tài khoản ngân hàng
    bank_accounts = BankAccount.objects.filter(user=request.user).order_by('-is_default', '-created_at')
    
    # Comprueba si hay un transaction_id en el formulario
    if request.method == 'POST' and 'transaction_id' in request.POST and request.POST.get('transaction_id'):
        transaction_id = request.POST.get('transaction_id')
    else:
        # Tạo transaction_id duy nhất
        transaction_id = f"DEP{uuid.uuid4().hex[:8].upper()}"
    
    # Mặc định amount
    default_amount = Decimal('100000')
    
    # Tạo URL mã QR VietQR
    qr_code_url = generate_qr_code(
        amount=default_amount,
        transaction_id=transaction_id,
        username=request.user.username
    )
    
    # Kiểm tra nếu có xác nhận nạp tiền
    verification_result = None
    if 'verify_deposit' in request.POST:
        # Lấy thông tin giao dịch cần xác nhận
        verify_transaction_id = request.POST.get('verify_transaction_id', transaction_id)
        verify_amount = request.POST.get('verify_amount')
        
        # Log para depuración
        print(f"Verificando depósito: ID={verify_transaction_id}, Monto={verify_amount}")
        
        if verify_transaction_id and verify_amount:
            # Kiểm tra thông tin giao dịch
            verification_result = check_paid(
                transaction_id=verify_transaction_id,
                amount=Decimal(verify_amount)
            )
            
            # Nếu xác nhận thành công
            if verification_result['success']:
                # Tạo transaction_obj trong DB
                with transaction.atomic():
                    # Tạo giao dịch nạp tiền
                    transaction_obj = WalletTransaction.objects.create(
                        user=request.user,
                        wallet=wallet,
                        type='deposit',
                        amount=Decimal(verify_amount),
                        fee=0,  # Miễn phí nạp tiền
                        net_amount=Decimal(verify_amount),
                        status='completed',  # Trạng thái hoàn thành
                        payment_method='bank_transfer',
                        transaction_id=verify_transaction_id,
                        notes=f"Nạp tiền xác nhận thủ công: {verification_result['message']}"
                    )
                
                messages.success(request, verification_result['message'])
                return redirect('wallet')
            else:
                messages.error(request, verification_result['message'])
    
    # Xử lý form deposit bình thường
    if request.method == 'POST' and 'confirm_transfer' in request.POST:
        print("POST request received for deposit_money")
        print("POST data:", request.POST)
        
        form = DepositForm(request.user, request.POST)
        if form.is_valid():
            print("Form is valid, processing deposit...")
            # Xử lý form nạp tiền
            amount = form.cleaned_data['amount']
            payment_method = form.cleaned_data['payment_method']
            bank_account = form.cleaned_data.get('bank_account')
            agree_terms = form.cleaned_data['agree_terms']
            
            # Kiểm tra xem giao dịch có trùng lặp không (trong vòng 5 phút gần đây)
            five_minutes_ago = timezone.now() - timedelta(minutes=5)
            recent_deposits = WalletTransaction.objects.filter(
                user=request.user,
                type='deposit',
                amount=amount,
                payment_method=payment_method,
                created_at__gte=five_minutes_ago
            )
            
            if recent_deposits.exists():
                messages.warning(request, 'Một giao dịch nạp tiền tương tự đã được thực hiện trong vòng 5 phút qua. Vui lòng đợi một lát và kiểm tra số dư của bạn trước khi thử lại.')
                return redirect('wallet')
            
            # Nếu là phương thức thanh toán chuyển khoản ngân hàng nhưng không chọn tài khoản nào
            if payment_method == 'bank_transfer' and not bank_account:
                # Xử lý tài khoản ngân hàng mới
                bank_name = form.cleaned_data.get('new_bank_name')
                other_bank_name = form.cleaned_data.get('new_other_bank_name')
                account_name = form.cleaned_data.get('new_account_name')
                account_number = form.cleaned_data.get('new_account_number')
                branch = form.cleaned_data.get('new_branch')
                is_default = form.cleaned_data.get('new_is_default', False)
                
                if bank_name and account_name and account_number:
                    if bank_name == 'other' and other_bank_name:
                        display_name = other_bank_name
                    else:
                        display_name = dict(BankAccount.BANK_CHOICES)[bank_name]
                    
                    # Tạo tài khoản ngân hàng mới
                    bank_account = BankAccount.objects.create(
                        user=request.user,
                        bank_name=bank_name,
                        other_bank_name=other_bank_name,
                        account_name=account_name,
                        account_number=account_number,
                        branch=branch,
                        is_default=is_default
                    )
                    
                    messages.success(request, f'Đã thêm tài khoản {display_name} - {account_number}')
                else:
                    messages.error(request, 'Vui lòng chọn hoặc thêm tài khoản ngân hàng để tiếp tục nạp tiền.')
                    return redirect('deposit_money')
            
            # Chuyển sang trang xác nhận nạp tiền
            context = {
                'wallet': wallet,
                'amount': amount,
                'transaction_id': transaction_id,
                'payment_method': payment_method,
                'qr_code_url': generate_qr_code(
                    amount=amount,
                    transaction_id=transaction_id,
                    username=request.user.username
                ),
                'bank_account': bank_account,
                'verify_mode': True
            }
            
            return render(request, 'portfolio/deposit.html', context)
        else:
            print("Form is invalid")
            print("Form errors:", form.errors)
            for field, errors in form.errors.items():
                for error in errors:
                    messages.error(request, f"{error}")
            
            # Nếu có amount trong form không hợp lệ, tạo lại mã QR với amount từ form
            amount = request.POST.get('amount', default_amount)
            qr_code_url = generate_qr_code(
                amount=amount,
                transaction_id=transaction_id,
                username=request.user.username
            )
    else:
        form = DepositForm(request.user)
    
    context = {
        'wallet': wallet,
        'bank_accounts': bank_accounts,
        'form': form,
        'qr_code_url': qr_code_url,
        'transaction_id': transaction_id,
        'verification_result': verification_result,
        'verify_mode': False
    }
    
    return render(request, 'portfolio/deposit.html', context)

@login_required
def verify_deposit(request, transaction_id=None):
    """View để kiểm tra giao dịch nạp tiền cuối cùng"""
    if request.method != 'POST':
        return redirect('deposit_money')
        
    # Lấy thông tin từ form
    # Prioriza obtener el transaction_id del parámetro, luego del formulario
    if not transaction_id:
        # Usamos el campo oculto transaction_id si está presente
        transaction_id = request.POST.get('transaction_id')
        # Como fallback, usamos verify_transaction_id
        if not transaction_id:
            transaction_id = request.POST.get('verify_transaction_id')
    
    verify_transaction_id = transaction_id  # Aseguramos que se use el mismo ID
    verify_amount = request.POST.get('verify_amount')
    
    if not verify_transaction_id or not verify_amount:
        messages.error(request, "Vui lòng cung cấp đầy đủ mã giao dịch và số tiền để xác nhận nạp tiền")
        return redirect('deposit_money')
    
    # Kiểm tra thông tin giao dịch
    verification_result = check_paid(
        transaction_id=verify_transaction_id,
        amount=Decimal(verify_amount)
    )
    
    # Log para depuración
    print(f"Verificando transacción: ID={verify_transaction_id}, Monto={verify_amount}")
    
    # Lấy hoặc tạo ví cho người dùng
    wallet, created = Wallet.objects.get_or_create(user=request.user)
    
    # Nếu xác nhận thành công
    if verification_result['success']:
        # Kiểm tra xem giao dịch này đã được xử lý chưa
        if WalletTransaction.objects.filter(
            user=request.user, 
            transaction_id=verify_transaction_id, 
            status='completed'
        ).exists():
            messages.warning(request, f"Giao dịch với mã {verify_transaction_id} đã được xử lý trước đó.")
            return redirect('wallet')
            
        # Tạo transaction_obj trong DB
        with transaction.atomic():
            # Tạo giao dịch nạp tiền
            transaction_obj = WalletTransaction.objects.create(
                user=request.user,
                wallet=wallet,
                type='deposit',
                amount=Decimal(verify_amount),
                fee=0,  # Miễn phí nạp tiền
                net_amount=Decimal(verify_amount),
                status='completed',  # Trạng thái hoàn thành
                payment_method='bank_transfer',
                transaction_id=verify_transaction_id,
                notes=f"Nạp tiền xác nhận thủ công: {verification_result['message']}"
            )
        
        messages.success(request, verification_result['message'])
        return redirect('wallet')
    else:
        messages.error(request, verification_result['message'])
        
    # Quay lại trang nạp tiền với kết quả xác nhận
    return redirect('deposit_money')

@login_required
def withdraw_money(request):
    # Lấy hoặc tạo ví cho người dùng
    wallet, created = Wallet.objects.get_or_create(user=request.user)
    
    # Lấy danh sách tài khoản ngân hàng
    bank_accounts = BankAccount.objects.filter(user=request.user).order_by('-is_default', '-created_at')
    
    # Số dư tối thiểu để rút tiền (50,000 VND)
    MINIMUM_BALANCE = Decimal('50000')
    
    # Phí cố định cho mỗi giao dịch rút tiền (10,000 VND)
    WITHDRAWAL_FEE = Decimal('10000')
    
    if request.method == 'POST':
        form = WithdrawForm(request.user, request.POST)
        if form.is_valid():
            # Xử lý form rút tiền
            amount = form.cleaned_data['amount']
            bank_account = form.cleaned_data.get('bank_account')
            withdraw_note = form.cleaned_data.get('notes', '')
            
            # Tính phí rút tiền và số tiền thực nhận
            fee = WITHDRAWAL_FEE
            net_amount = amount - fee
            
            # Kiểm tra số tiền rút hợp lệ (sau khi trừ phí phải > 0)
            if net_amount <= 0:
                messages.error(request, f'Số tiền rút phải lớn hơn phí giao dịch ({dinh_dang_tien(WITHDRAWAL_FEE)} VNĐ).')
                return redirect('withdraw_money')
            
            # Kiểm tra số dư - phải đảm bảo đủ rút và đảm bảo số dư còn lại trên mức tối thiểu
            if amount > wallet.balance:
                messages.error(request, 'Số dư của bạn không đủ để thực hiện giao dịch này.')
                return redirect('withdraw_money')
            
            # Kiểm tra số dư còn lại sau khi rút phải lớn hơn hoặc bằng số dư tối thiểu
            if (wallet.balance - amount) < MINIMUM_BALANCE:
                messages.error(request, f'Số dư còn lại sau khi rút phải lớn hơn hoặc bằng {dinh_dang_tien(MINIMUM_BALANCE)} VNĐ.')
                return redirect('withdraw_money')
            
            # Nếu không chọn tài khoản nào
            if not bank_account:
                # Xử lý tài khoản ngân hàng mới
                bank_name = form.cleaned_data.get('new_bank_name')
                other_bank_name = form.cleaned_data.get('new_other_bank_name')
                account_name = form.cleaned_data.get('new_account_name')
                account_number = form.cleaned_data.get('new_account_number')
                branch = form.cleaned_data.get('new_branch')
                is_default = form.cleaned_data.get('new_is_default', False)
                
                if bank_name and account_name and account_number:
                    if bank_name == 'other' and other_bank_name:
                        display_name = other_bank_name
                    else:
                        display_name = dict(BankAccount.BANK_CHOICES)[bank_name]
                    
                    # Tạo tài khoản ngân hàng mới
                    bank_account = BankAccount.objects.create(
                        user=request.user,
                        bank_name=bank_name,
                        other_bank_name=other_bank_name,
                        account_name=account_name,
                        account_number=account_number,
                        branch=branch,
                        is_default=is_default
                    )
                    
                    messages.success(request, f'Đã thêm tài khoản {display_name} - {account_number}')
                else:
                    messages.error(request, 'Vui lòng chọn hoặc thêm tài khoản ngân hàng để tiếp tục rút tiền.')
                    return redirect('withdraw_money')
            
            # Tạo transaction_id duy nhất để tránh giao dịch trùng lặp
            transaction_id = f"WIT{uuid.uuid4().hex[:8].upper()}"
            
            # Kiểm tra xem giao dịch có trùng lặp không (trong vòng 5 phút gần đây)
            five_minutes_ago = timezone.now() - timedelta(minutes=5)
            recent_withdrawals = WalletTransaction.objects.filter(
                user=request.user,
                type='withdraw',
                amount=amount,
                created_at__gte=five_minutes_ago
            )
            
            if recent_withdrawals.exists():
                messages.warning(request, 'Một giao dịch rút tiền tương tự đã được thực hiện trong vòng 5 phút qua. Vui lòng đợi một lát và kiểm tra số dư của bạn trước khi thử lại.')
                return redirect('wallet')
            
            # Sử dụng transaction.atomic để đảm bảo tính toàn vẹn dữ liệu
            with transaction.atomic():
                # Tạo giao dịch rút tiền với trạng thái 'pending' - chờ admin phê duyệt
                transaction_obj = WalletTransaction.objects.create(
                    user=request.user,
                    wallet=wallet,
                    type='withdraw',
                    amount=amount,
                    fee=fee,
                    net_amount=net_amount,
                    status='pending', # Thay đổi trạng thái thành 'pending' thay vì 'completed'
                    bank_account=bank_account,
                    payment_method='bank_transfer',
                    transaction_id=transaction_id,
                    notes=withdraw_note
                )
            
            messages.success(
                request, 
                f'Yêu cầu rút tiền đã được gửi đi và đang chờ xét duyệt! '
                f'<br>Số tiền rút: {dinh_dang_tien(amount)} VNĐ'
                f'<br>Phí giao dịch: {dinh_dang_tien(fee)} VNĐ'
                f'<br>Số tiền thực nhận: {dinh_dang_tien(net_amount)} VNĐ'
                f'<br>Số dư hiện tại: {dinh_dang_tien(wallet.balance)} VNĐ'
                f'<br>Trạng thái: Đang chờ xử lý'
            )
            return redirect('wallet')
        else:
            for field, errors in form.errors.items():
                for error in errors:
                    messages.error(request, f"{error}")
    else:
        form = WithdrawForm(request.user)
    
    context = {
        'wallet': wallet,
        'bank_accounts': bank_accounts,
        'form': form,
        'minimum_balance': MINIMUM_BALANCE,
        'withdrawal_fee': WITHDRAWAL_FEE
    }
    
    return render(request, 'portfolio/withdraw.html', context)

@login_required
def bank_account_list(request):
    bank_accounts = BankAccount.objects.filter(user=request.user).order_by('-is_default', '-created_at')
    
    context = {
        'bank_accounts': bank_accounts
    }
    
    return render(request, 'portfolio/bank_account_list.html', context)

@login_required
def bank_account_create(request):
    if request.method == 'POST':
        form = BankAccountForm(request.POST)
        if form.is_valid():
            bank_account = form.save(commit=False)
            bank_account.user = request.user
            bank_account.save()
            
            bank_name = form.cleaned_data['bank_name']
            if bank_name == 'other':
                display_name = form.cleaned_data['other_bank_name']
            else:
                display_name = dict(BankAccount.BANK_CHOICES)[bank_name]
                
            account_number = form.cleaned_data['account_number']
            
            messages.success(request, f'Đã thêm tài khoản {display_name} - {account_number}')
            return redirect('bank_account_list')
    else:
        form = BankAccountForm()
    
    context = {
        'form': form,
        'title': 'Thêm tài khoản ngân hàng'
    }
    
    return render(request, 'portfolio/bank_account_form.html', context)

@login_required
def update_bank_account(request, pk):
    bank_account = get_object_or_404(BankAccount, id=pk, user=request.user)
    
    if request.method == 'POST':
        form = BankAccountForm(request.POST, instance=bank_account)
        if form.is_valid():
            form.save()
            
            bank_name = form.cleaned_data['bank_name']
            if bank_name == 'other':
                display_name = form.cleaned_data['other_bank_name']
            else:
                display_name = dict(BankAccount.BANK_CHOICES)[bank_name]
                
            account_number = form.cleaned_data['account_number']
            
            messages.success(request, f'Đã cập nhật tài khoản {display_name} - {account_number}')
            return redirect('bank_account_list')
    else:
        form = BankAccountForm(instance=bank_account)
    
    context = {
        'form': form,
        'bank_account': bank_account,
        'title': 'Cập nhật tài khoản ngân hàng'
    }
    
    return render(request, 'portfolio/bank_account_form.html', context)

@login_required
def delete_bank_account(request, pk):
    bank_account = get_object_or_404(BankAccount, id=pk, user=request.user)
    
    if request.method == 'POST':
        # Kiểm tra xem có giao dịch đang sử dụng tài khoản này không
        transactions_using_account = WalletTransaction.objects.filter(
            bank_account=bank_account,
            status='pending'
        ).exists()
        
        if transactions_using_account:
            messages.error(request, 'Không thể xóa tài khoản này vì có giao dịch đang xử lý.')
            return redirect('bank_account_list')
        
        # Lưu thông tin để hiển thị thông báo
        bank_name = bank_account.bank_name
        if bank_name == 'other':
            display_name = bank_account.other_bank_name
        else:
            display_name = dict(BankAccount.BANK_CHOICES)[bank_name]
            
        account_number = bank_account.account_number
        
        # Xóa tài khoản
        bank_account.delete()
        
        messages.success(request, f'Đã xóa tài khoản {display_name} - {account_number}')
        return redirect('bank_account_list')
    
    context = {
        'bank_account': bank_account
    }
    
    return render(request, 'portfolio/bank_account_confirm_delete.html', context)

@login_required
def set_default_bank_account(request, pk):
    bank_account = get_object_or_404(BankAccount, id=pk, user=request.user)
    
    # Đặt tất cả tài khoản khác thành không mặc định
    BankAccount.objects.filter(user=request.user).update(is_default=False)
    
    # Đặt tài khoản hiện tại thành mặc định
    bank_account.is_default = True
    bank_account.save()
    
    bank_name = bank_account.bank_name
    if bank_name == 'other':
        display_name = bank_account.other_bank_name
    else:
        display_name = dict(BankAccount.BANK_CHOICES)[bank_name]
        
    account_number = bank_account.account_number
    
    messages.success(request, f'Đã đặt {display_name} - {account_number} làm tài khoản mặc định')
    
    return redirect('bank_account_list')

@login_required
def wallet_transactions(request):
    # Lấy tất cả giao dịch của người dùng
    transactions = WalletTransaction.objects.filter(user=request.user).order_by('-created_at')
    
    # Lọc theo loại giao dịch nếu có
    transaction_type = request.GET.get('type')
    if transaction_type in ['deposit', 'withdraw']:
        transactions = transactions.filter(type=transaction_type)
    
    # Lọc theo trạng thái nếu có
    status = request.GET.get('status')
    if status in ['pending', 'completed', 'failed', 'cancelled']:
        transactions = transactions.filter(status=status)
    
    # Lọc theo ngày
    from_date = request.GET.get('from_date')
    if from_date:
        transactions = transactions.filter(created_at__gte=from_date)
    
    to_date = request.GET.get('to_date')
    if to_date:
        transactions = transactions.filter(created_at__lte=to_date)
    
    # Phân trang
    paginator = Paginator(transactions, 10)  # 10 giao dịch mỗi trang
    page_number = request.GET.get('page')
    paged_transactions = paginator.get_page(page_number)
    
    context = {
        'transactions': paged_transactions,
        'transaction_types': WalletTransaction.TYPE_CHOICES,
        'status_choices': WalletTransaction.STATUS_CHOICES
    }
    
    return render(request, 'portfolio/wallet_transactions.html', context)
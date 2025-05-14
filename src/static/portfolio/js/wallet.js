/**
 * Wallet and Banking Functionality for Portfolio Management System
 */
document.addEventListener('DOMContentLoaded', function() {
    // Xử lý tài khoản ngân hàng khác trong form thêm tài khoản
    const bankNameSelect = document.getElementById('id_bank_name');
    const otherBankGroup = document.getElementById('otherBankGroup');
    
    if (bankNameSelect && otherBankGroup) {
        // Kiểm tra giá trị ban đầu
        if (bankNameSelect.value === 'other') {
            otherBankGroup.classList.remove('d-none');
        } else {
            otherBankGroup.classList.add('d-none');
        }
        
        bankNameSelect.addEventListener('change', function() {
            if (this.value === 'other') {
                otherBankGroup.classList.remove('d-none');
            } else {
                otherBankGroup.classList.add('d-none');
            }
        });
    }
    
    // Xử lý hiển thị số tiền trong form nạp/rút tiền
    const amountInput = document.getElementById('amount');
    const amountDisplayElements = document.querySelectorAll('.amount-display');
    const feeDisplayElement = document.querySelector('.fee-display');
    const totalAmountDisplayElement = document.querySelector('.total-amount-display');
    
    if (amountInput && amountDisplayElements.length > 0) {
        // Set initial value if amount has a value
        if (amountInput.value) {
            updateAmountDisplay();
        }
        
        amountInput.addEventListener('input', updateAmountDisplay);
        
        function updateAmountDisplay() {
            const amount = parseFloat(amountInput.value) || 0;
            const formattedAmount = formatCurrency(amount);
            
            amountDisplayElements.forEach(element => {
                element.textContent = formattedAmount;
            });
            
            // Tính phí và tổng tiền cho form rút tiền
            if (feeDisplayElement && totalAmountDisplayElement) {
                // Phí rút tiền: 0.5% (tối thiểu 10,000 VNĐ, tối đa 50,000 VNĐ)
                const fee = Math.min(Math.max(amount * 0.005, 10000), 50000);
                const totalAmount = amount - fee;
                
                feeDisplayElement.textContent = formatCurrency(fee);
                totalAmountDisplayElement.textContent = formatCurrency(totalAmount);
            }
            
            // Cập nhật mã QR khi số tiền thay đổi
            if (typeof generateBankTransferQR === 'function') {
                generateBankTransferQR();
                generateMomoQR();
                generateVNPayQR();
            }
        }
    }
    
    // Xử lý hiển thị phương thức thanh toán trong form nạp tiền
    const paymentMethodRadios = document.querySelectorAll('input[name="payment_method"]');
    const bankTransferSection = document.getElementById('bankTransferSection');
    const momoSection = document.getElementById('momoSection');
    const vnpaySection = document.getElementById('vnpaySection');
    
    // Các trường VNPay
    const fullNameField = document.getElementById('fullName');
    const emailField = document.getElementById('email');
    
    if (paymentMethodRadios.length > 0 && bankTransferSection) {
        // Mặc định hiển thị phần chuyển khoản ngân hàng (nếu đã chọn)
        const selectedPaymentMethod = document.querySelector('input[name="payment_method"]:checked');
        if (selectedPaymentMethod) {
            handlePaymentMethodChange(selectedPaymentMethod.value);
        } else if (paymentMethodRadios[0]) {
            // Nếu không có phương thức nào được chọn, chọn phương thức đầu tiên
            paymentMethodRadios[0].checked = true;
            handlePaymentMethodChange(paymentMethodRadios[0].value);
        }
        
        paymentMethodRadios.forEach(radio => {
            radio.addEventListener('change', function() {
                handlePaymentMethodChange(this.value);
            });
        });
    }
    
    // Hàm xử lý thay đổi phương thức thanh toán
    function handlePaymentMethodChange(method) {
        // Ẩn tất cả các phần
        if (bankTransferSection) bankTransferSection.classList.add('d-none');
        if (momoSection) momoSection.classList.add('d-none');
        if (vnpaySection) vnpaySection.classList.add('d-none');
        
        // Hiển thị phần tương ứng
        if (method === 'bank_transfer' && bankTransferSection) {
            bankTransferSection.classList.remove('d-none');
        } else if (method === 'momo' && momoSection) {
            momoSection.classList.remove('d-none');
        } else if (method === 'vnpay' && vnpaySection) {
            vnpaySection.classList.remove('d-none');
        }
        
        // Cập nhật các trường required
        toggleRequiredFields();
    }
    
    // Hàm xử lý việc bật/tắt các trường dựa trên phương thức thanh toán
    function toggleRequiredFields() {
        // Nếu đang sử dụng VNPay, bật required cho các trường VNPay
        if (fullNameField && emailField) {
            if (vnpaySection && !vnpaySection.classList.contains('d-none')) {
                fullNameField.disabled = false;
                emailField.disabled = false;
                emailField.required = true;
                fullNameField.required = true;
            } else {
                fullNameField.disabled = true;
                emailField.disabled = true;
                emailField.required = false;
                fullNameField.required = false;
            }
        }
    }
    
    // Xử lý tài khoản ngân hàng mới trong form rút/nạp tiền
    const newBankAccountRadio = document.getElementById('newBankAccount');
    const newBankAccountForm = document.getElementById('newBankAccountForm');
    
    if (newBankAccountRadio && newBankAccountForm) {
        newBankAccountRadio.addEventListener('change', function() {
            if (this.checked) {
                newBankAccountForm.classList.remove('d-none');
            } else {
                newBankAccountForm.classList.add('d-none');
            }
        });
        
        // Kiểm tra ban đầu
        if (newBankAccountRadio.checked) {
            newBankAccountForm.classList.remove('d-none');
        } else {
            newBankAccountForm.classList.add('d-none');
        }
    }
    
    // Hàm định dạng tiền tệ
    function formatCurrency(amount) {
        return amount.toLocaleString('vi-VN') + ' VNĐ';
    }
});

// QR Code Generation Functions
function generateBankTransferQR() {
    if (!window.QRCode) return;
    
    const amountInput = document.getElementById('amount');
    if (!amountInput) return;
    
    const amount = amountInput.value || 0;
    const username = document.querySelector('meta[name="username"]')?.content || 'user';
    const qrContent = `2|99|19033868888016|CÔNG TY CP CHỨNG KHOÁN ...|Techcombank|NAP ${username}|0|${amount}|0|0|0`;
    
    const qrContainer = document.getElementById('bankTransferQR');
    if (!qrContainer) return;
    
    qrContainer.innerHTML = '';
    
    new QRCode(qrContainer, {
        text: qrContent,
        width: 200,
        height: 200,
        colorDark: "#000000",
        colorLight: "#ffffff",
        correctLevel: QRCode.CorrectLevel.H
    });
}

function generateMomoQR() {
    if (!window.QRCode) return;
    
    const amountInput = document.getElementById('amount');
    if (!amountInput) return;
    
    const amount = amountInput.value || 0;
    const username = document.querySelector('meta[name="username"]')?.content || 'user';
    const phone = '0987654321'; // Số điện thoại MoMo của hệ thống
    const qrContent = `2|99|${phone}|ASTROLUX|0|0|0|NAP ${username}|transfer_mo|${amount}`;
    
    const qrContainer = document.getElementById('momoQR');
    if (!qrContainer) return;
    
    qrContainer.innerHTML = '';
    
    new QRCode(qrContainer, {
        text: qrContent,
        width: 200,
        height: 200,
        colorDark: "#ae2070",
        colorLight: "#ffffff",
        correctLevel: QRCode.CorrectLevel.H
    });
}

function generateVNPayQR() {
    if (!window.QRCode) return;
    
    const amountInput = document.getElementById('amount');
    if (!amountInput) return;
    
    const amount = amountInput.value || 0;
    const username = document.querySelector('meta[name="username"]')?.content || 'user';
    const merchantId = 'VNPAY12345'; // Mã đơn vị VNPay
    const qrContent = `https://vnpayqr.vn/${merchantId}?amount=${amount}&addInfo=NAP ${username}&accountName=ASTROLUX`;
    
    const qrContainer = document.getElementById('vnpayQR');
    if (!qrContainer) return;
    
    qrContainer.innerHTML = '';
    
    new QRCode(qrContainer, {
        text: qrContent,
        width: 200,
        height: 200,
        colorDark: "#004a9c",
        colorLight: "#ffffff",
        correctLevel: QRCode.CorrectLevel.H
    });
}

function downloadQR(elementId) {
    const canvas = document.querySelector(`#${elementId} canvas`);
    if (!canvas) return;
    
    const link = document.createElement('a');
    link.download = `qrcode-${elementId}.png`;
    link.href = canvas.toDataURL('image/png').replace('image/png', 'image/octet-stream');
    link.click();
} 
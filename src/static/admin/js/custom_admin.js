// Thêm JavaScript tùy chỉnh cho trang admin
document.addEventListener('DOMContentLoaded', function() {
    // Đánh dấu các hàng theo trạng thái
    var rows = document.querySelectorAll('#result_list tbody tr');
    
    rows.forEach(function(row) {
        // Tìm cột trạng thái (status)
        var statusCell = row.querySelector('td.field-status_colored');
        if (!statusCell) return;
        
        var statusText = statusCell.textContent.trim();
        
        // Thêm class dựa vào trạng thái
        if (statusText.includes('Đang xử lý')) {
            row.classList.add('status-pending');
        } else if (statusText.includes('Hoàn thành')) {
            row.classList.add('status-completed');
        } else if (statusText.includes('Thất bại')) {
            row.classList.add('status-failed');
        } else if (statusText.includes('Đã hủy')) {
            row.classList.add('status-cancelled');
        }
    });
    
    // Thêm nút chức năng nhanh
    var actionBar = document.querySelector('.actions');
    if (actionBar) {
        // Tạo nút "Phê duyệt tất cả"
        var bulkApproveBtn = document.createElement('button');
        bulkApproveBtn.type = 'button';
        bulkApproveBtn.className = 'button';
        bulkApproveBtn.textContent = 'Chọn tất cả đang chờ xử lý';
        bulkApproveBtn.style.marginLeft = '10px';
        bulkApproveBtn.style.backgroundColor = '#f39c12';
        bulkApproveBtn.style.color = 'white';
        
        bulkApproveBtn.addEventListener('click', function() {
            // Chọn tất cả giao dịch đang chờ xử lý
            rows.forEach(function(row) {
                if (row.classList.contains('status-pending')) {
                    var checkbox = row.querySelector('input[type="checkbox"]');
                    if (checkbox) checkbox.checked = true;
                }
            });
        });
        
        actionBar.appendChild(bulkApproveBtn);
    }
    
    // Thêm hiệu ứng khi hover vào các hàng
    rows.forEach(function(row) {
        row.addEventListener('mouseover', function() {
            this.style.transition = 'background-color 0.2s ease';
        });
    });
    
    // Hiển thị thông báo khi chọn hành động phê duyệt hoặc từ chối
    var actionSelect = document.querySelector('select[name="action"]');
    if (actionSelect) {
        actionSelect.addEventListener('change', function() {
            var selectedAction = this.value;
            
            // Đếm số giao dịch đã chọn
            var selectedCount = document.querySelectorAll('#result_list tbody input[type="checkbox"]:checked').length;
            
            if (selectedCount > 0) {
                if (selectedAction === 'approve_transactions') {
                    alert('Bạn đang phê duyệt ' + selectedCount + ' giao dịch. Hãy nhấn "Go" để tiếp tục.');
                } else if (selectedAction === 'reject_transactions') {
                    alert('Bạn đang từ chối ' + selectedCount + ' giao dịch. Hãy nhấn "Go" để tiếp tục.');
                }
            }
        });
    }
}); 
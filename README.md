# Hệ Thống Quản Lý Danh Mục Đầu Tư

- **Thành viên nhóm**
- **Thành viên 1**: Kiều Trương Hàm Hương - 22719241
- **Thành viên 2**: Đào Tiến Sang - 22705971
- **Thành viên 3**: Nguyễn Chí Trung - 22719231
- **Thành viên 4**: Nguyễn Thị Thúy Kiều - 22733091 

## Giới Thiệu
Hệ thống quản lý danh mục đầu tư giúp người dùng theo dõi, mua bán tài sản tài chính và quản lý danh mục đầu tư của họ. Hệ thống cung cấp các chức năng như nạp tiền vào ví, giao dịch tài sản, theo dõi biến động thị trường, và báo cáo hiệu suất danh mục, chat với AI có sử dụng data từ DB Realtime.

## Công Nghệ Sử Dụng
- **Font-End**: HTML, CSS, JavaScript
- **Back-End**: Django (Python)
- **Database**: PostgreSQL

## Các Tính Năng Chính
- Đăng ký, đăng nhập, xác thực người dùng
- Quản lý ví tiền, nạp tiền vào ví
- Tạo và quản lý danh mục đầu tư
- Mua, bán tài sản tài chính
- Theo dõi biến động giá thị trường
- Xem biểu đồ nến cho các mã cổ phiếu với đầy đủ:  ( Phần này hiện tại đang lỗi, chỉ có data đã tính toán chưa vẽ được biểu đồ )
  - Đường trung bình động MA5, MA20
  - Chỉ báo MACD (Moving Average Convergence Divergence)
  - Chỉ báo RSI (Relative Strength Index)
  - Khuyến nghị mua/bán dựa trên phân tích kỹ thuật
- Xem báo cáo hiệu suất danh mục đầu tư
- Chat Box AI

## Cấu Trúc Dự Án
```
├── airflow/                        # Cấu hình airflow & dag
├── spark/                          # Cấu hình spark xử lý
├── README.md                       # Giới thiệu & Tổng quan
├── docker-compose.yml              # Cấu hình Docker Compose
├── Dockerfile                      # Cấu hình Docker
├── requirements.txt                # Các thư viện Python cần thiết
├── run.bat                         # Script khởi động cho Windows
├── run.sh                          # Script khởi động cho Linux/macOS
├── .env                            # Cấu hình biến môi trường
└── src/                            # Mã nguồn chính của dự án
    ├── manage.py                   # Tệp quản lý Django
    ├── config/                     # Cấu hình Django
    │   ├── settings.py             # Cài đặt Django
    │   ├── urls.py                 # URL chính của hệ thống
    │   ├── asgi.py                 # Cấu hình ASGI
    │   └── wsgi.py                 # Cấu hình WSGI
    ├── portfolio/                  # Ứng dụng chính
    │   ├── admin.py                # Cấu hình admin
    │   ├── apps.py                 # Cấu hình ứng dụng
    │   ├── models.py               # Định nghĩa model dữ liệu
    │   ├── views.py                # Xử lý logic và hiển thị
    │   ├── urls.py                 # Định nghĩa URL cho ứng dụng
    │   ├── tests.py                # Kiểm thử đơn vị
    │   ├── migrations/             # Migration database
    │   ├── ai_chat.py              # Cấu hình Chat Box AI
    │   ├── kafka_services.py       # Cấu hình kafka_services
    │   ├── kafka_producer_test.py  # File kiểm tra chức năng gửi dữ liệu đến Kafka.
    │   ├── middleware.py           # Chứa các middleware Django xử lý request/response.
    │   ├── pipeline.py             # Định nghĩa quy trình xử lý dữ liệu tuần tự cho ứng dụng.
    │   ├── postgres_chart_service  # Dịch vụ lấy và xử lý dữ liệu biểu đồ từ PostgreSQL.
    │   ├── utils.py                # Chứa các hàm tiện ích dùng chung cho cả dự án.
    │   ├── view_fixed.py           # Phiên bản cải tiến của file views.py với các sửa lỗi.
    │   ├── view_wallet.py          # Xử lý các chức năng liên quan đến ví điện tử.
    │   ├── vnstock_kafka.py        # Kết nối dữ liệu cho Kafka.
    │   └── vnstock_services.py     # Cung cấp các dịch vụ tương tác với thư viện vnstock, lấy dữ liệu cổ phiếu.
    ├── static/                     # Tài nguyên tĩnh (CSS, JS, hình ảnh)
    ├── media/                      # File người dùng tải lên
    └── templates/                  # Template HTML
        ├── base.html               # Template cơ sở
        └── portfolio/              # Template cho ứng dụng portfolio
            ├── home.html           # Trang chủ
            ├── dashboard.html      # Bảng điều khiển
            ├── login.html          # Trang đăng nhập ( Đã thay bằng Auth0 )
            ├── register.html       # Trang đăng ký ( Đã thay bằng Auth0 )
            ├── asset_list.html     # Hiển thị danh sách tài sản/cổ phiếu của người dùng.
            ├── bank_account_confirm_delete.html     # Xác nhận xóa tài khoản ngân hàng.
            ├── bank_account_form.html               # Biểu mẫu thêm/sửa tài khoản ngân hàng.
            ├── bank_account_list.html               # Hiển thị danh sách tài khoản ngân hàng.
            ├── base.html           # Template cơ sở chứa cấu trúc chung cho tất cả trang.
            ├── debug_assets.html   # Trang kiểm tra và gỡ lỗi thông tin tài sản.
            ├── deposit.html        # Giao diện nạp tiền vào ví điện tử.
            ├── market.html         # Hiển thị thông tin thị trường chứng khoán.
            ├── portfolio_detail.html                # Chi tiết một danh mục đầu tư cụ thể.
            ├── portfolio_form.html # Biểu mẫu tạo/chỉnh sửa danh mục đầu tư.
            ├── portfolio_list.html # Danh sách các danh mục đầu tư.
            ├── profile.html        # Hiển thị thông tin cá nhân người dùng.
            ├── transaction_form.html                # Biểu mẫu tạo giao dịch mua/bán.
            ├── transaction_list.html                # Danh sách các giao dịch đã thực hiện.
            ├── user_profile.html   # Trang thông tin chi tiết và chỉnh sửa hồ sơ người dùng.
            ├── wallet_transactions.html             # Lịch sử giao dịch ví điện tử.
            ├── wallet.html         # Hiển thị thông tin ví điện tử.
            └── withdraw.html       # Giao diện rút tiền từ ví điện tử.
```

## Mô Tả Các Thành Phần Chính

### Models
- `User`: Mô hình người dùng mở rộng từ Django User
- `Portfolio`: Danh mục đầu tư
- `Asset`: Tài sản tài chính
- `Transaction`: Giao dịch mua/bán
- `Wallet`: Ví tiền của người dùng
- `BankAccount`: Tài khoản ngân hàng liên kết

### Views
- `home`: Hiển thị trang chủ
- `dashboard`: Bảng điều khiển chính
- `register`, `login_view`: Xử lý đăng ký và đăng nhập
- `portfolio_*`: Các view xử lý danh mục đầu tư
- `transaction_*`: Các view xử lý giao dịch
- `wallet_*`: Các view xử lý ví điện tử
- `market`: Hiển thị trang thị trường chứng khoán với dữ liệu từ Kafka
- `get_kafka_stock_data`: API lấy dữ liệu biểu đồ nến cho mã cổ phiếu

### Templates
- `base.html`: Template cơ sở chung
- `home.html`: Trang chủ với giới thiệu hệ thống
- `dashboard.html`: Bảng điều khiển người dùng
- `login.html`, `register.html`: Form đăng nhập và đăng ký
- `market.html`: Hiển thị bảng giá và biểu đồ thị trường với các chỉ báo kỹ thuật

### Tính Năng Thị Trường Chứng Khoán ( Phần này hiện tại đang lỗi, chỉ có data đã tính toán chưa vẽ được biểu đồ)
- **Bảng giá thị trường**: Hiển thị giá trần, giá sàn, giá tham chiếu, giá khớp lệnh và khối lượng của các mã cổ phiếu từ dữ liệu Kafka
- **Biểu đồ nến**: Hiển thị biểu đồ nến với đường MA5 và MA20
- **Phân tích kỹ thuật**: Hiển thị các chỉ số MACD, RSI và phần khuyến nghị dựa trên phân tích kỹ thuật
- **Tìm kiếm**: Tìm kiếm nhanh mã cổ phiếu trong bảng giá
- **Tự động tính giá tham chiếu**: Tính toán giá tham chiếu từ giá trần và giá sàn nếu chưa có


## Cài đặt
**Yêu cầu:**
PostgresSQL
Python3 trở lên

### Khởi động server Django với Docker
**Chú ý:** trong file `.env` khi chạy trên môi trường máy docker-compose thì mở file và đặt
```
DATABASE_HOST=db
```

Chạy lệnh: `docker-compose up --build` để build các Image \

- **Truy cập trình duyệt ở địa chỉ: http://localhost:8000/**
# MangaDex Data Pipeline

[English](#english) | [Tiếng Việt](#tiếng-việt)

## English

### Overview
This project is a data pipeline system built with Apache Airflow to collect, process, and store manga data from MangaDex. The system is containerized using Docker and includes components for web crawling, data processing, and database population. It also features a Streamlit dashboard for data visualization and analysis.

### Project Structure
```
mangadex-pipeline/
├── airflow/                 # Airflow configuration and DAGs
│   ├── dags/               # Airflow DAG definitions
│   ├── config/             # Airflow configuration files
│   ├── plugins/            # Custom Airflow plugins
│   ├── logs/               # Airflow execution logs
│   └── docker-compose.yaml # Docker compose configuration
├── src/                    # Source code
│   ├── crawler/           # Web crawling components
│   ├── populate_db/       # Database population scripts
│   ├── dashboard/         # Streamlit dashboard
│   │   ├── app.py        # Main dashboard application
│   │   └── core/         # Dashboard core components
│   └── utils/             # Utility functions
```

### Features
- Automated data collection from MangaDex
- Data processing and transformation pipeline
- Database integration (MongoDB and PostgreSQL)
- Containerized deployment with Docker
- Scalable architecture using Airflow
- Interactive Streamlit dashboard for data visualization and analysis

### Dashboard Features
The Streamlit dashboard provides:
- Real-time manga and chapter statistics
- Interactive data visualization with charts and graphs
- Advanced search and filtering capabilities
- Manga status and publication trends
- Chapter analysis and metrics
- Export functionality for data and charts
- Responsive design for all devices

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- MongoDB
- PostgreSQL
- Streamlit (for dashboard)

### Installation
1. Clone the repository:
```bash
git clone https://github.com/yourusername/mangadex-pipeline.git
cd mangadex-pipeline
```

2. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

3. Start the Airflow services:
```bash
cd airflow
docker-compose up -d
```

4. Access the Airflow web interface at `http://localhost:8080`

5. Start the Streamlit dashboard:
```bash
# Activate your virtual environment if you have one
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install required packages
pip install -r requirements.txt

# Start the dashboard
cd src/dashboard
streamlit run app.py
```

6. Access the dashboard at `http://localhost:8501`

### Usage
1. The default Airflow credentials are:
   - Username: airflow
   - Password: airflow

2. Navigate to the DAGs section to monitor and trigger data pipeline workflows

3. Using the Dashboard:
   - Search for manga using the search bar
   - Filter manga by status using the quick filter
   - View detailed statistics in the Overview tab
   - Analyze manga data in the Manga Analysis tab
   - Examine chapter data in the Chapter Analysis tab
   - Export data and charts using the export options

### License
This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

---

## Tiếng Việt

### Tổng quan
Dự án này là một hệ thống pipeline dữ liệu được xây dựng bằng Apache Airflow để thu thập, xử lý và lưu trữ dữ liệu manga từ MangaDex. Hệ thống được container hóa bằng Docker và bao gồm các thành phần cho việc crawl web, xử lý dữ liệu và cập nhật cơ sở dữ liệu. Dự án cũng có một bảng điều khiển Streamlit để trực quan hóa và phân tích dữ liệu.

### Cấu trúc dự án
```
mangadex-pipeline/
├── airflow/                 # Cấu hình và DAGs của Airflow
│   ├── dags/               # Định nghĩa các DAG của Airflow
│   ├── config/             # Các file cấu hình Airflow
│   ├── plugins/            # Các plugin tùy chỉnh cho Airflow
│   ├── logs/               # Log thực thi của Airflow
│   └── docker-compose.yaml # Cấu hình Docker compose
├── src/                    # Mã nguồn
│   ├── crawler/           # Các thành phần crawl web
│   ├── populate_db/       # Script cập nhật cơ sở dữ liệu
│   ├── dashboard/         # Bảng điều khiển Streamlit
│   │   ├── app.py        # Ứng dụng bảng điều khiển chính
│   │   └── core/         # Các thành phần cốt lõi của bảng điều khiển
│   └── utils/             # Các hàm tiện ích
```

### Tính năng
- Tự động thu thập dữ liệu từ MangaDex
- Pipeline xử lý và chuyển đổi dữ liệu
- Tích hợp cơ sở dữ liệu (MongoDB và PostgreSQL)
- Triển khai container hóa với Docker
- Kiến trúc có khả năng mở rộng sử dụng Airflow
- Bảng điều khiển Streamlit tương tác để trực quan hóa và phân tích dữ liệu

### Tính năng bảng điều khiển
Bảng điều khiển Streamlit cung cấp:
- Thống kê manga và chapter theo thời gian thực
- Trực quan hóa dữ liệu tương tác với biểu đồ
- Khả năng tìm kiếm và lọc nâng cao
- Xu hướng trạng thái và xuất bản manga
- Phân tích chapter và các chỉ số
- Chức năng xuất dữ liệu và biểu đồ
- Thiết kế responsive cho mọi thiết bị

### Yêu cầu hệ thống
- Docker và Docker Compose
- Python 3.8+
- MongoDB
- PostgreSQL
- Streamlit (cho bảng điều khiển)

### Cài đặt
1. Clone repository:
```bash
git clone https://github.com/yourusername/mangadex-pipeline.git
cd mangadex-pipeline
```

2. Thiết lập biến môi trường:
```bash
cp .env.example .env
# Chỉnh sửa .env với cấu hình của bạn
```

3. Khởi động các dịch vụ Airflow:
```bash
cd airflow
docker-compose up -d
```

4. Truy cập giao diện web Airflow tại `http://localhost:8080`

5. Khởi động bảng điều khiển Streamlit:
```bash
# Kích hoạt môi trường ảo nếu có
python -m venv .venv
source .venv/bin/activate  # Trên Windows: .venv\Scripts\activate

# Cài đặt các gói cần thiết
pip install -r requirements.txt

# Khởi động bảng điều khiển
cd src/dashboard
streamlit run app.py
```

6. Truy cập bảng điều khiển tại `http://localhost:8501`

### Sử dụng
1. Thông tin đăng nhập mặc định của Airflow:
   - Tên đăng nhập: airflow
   - Mật khẩu: airflow

2. Điều hướng đến phần DAGs để giám sát và kích hoạt các quy trình pipeline dữ liệu

3. Sử dụng bảng điều khiển:
   - Tìm kiếm manga bằng thanh tìm kiếm
   - Lọc manga theo trạng thái bằng bộ lọc nhanh
   - Xem thống kê chi tiết trong tab Tổng quan
   - Phân tích dữ liệu manga trong tab Phân tích Manga
   - Kiểm tra dữ liệu chapter trong tab Phân tích Chapter
   - Xuất dữ liệu và biểu đồ bằng các tùy chọn xuất

### Giấy phép
Dự án này được cấp phép theo Apache License 2.0 - xem file LICENSE để biết thêm chi tiết. 
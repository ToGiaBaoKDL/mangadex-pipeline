# MangaDex Data Pipeline

[English](#english) | [Tiếng Việt](#tiếng-việt)

## English

### Overview
This project is a data pipeline system built with Apache Airflow to collect, process, and store manga data from MangaDex. The system is containerized using Docker and includes components for web crawling, data processing, and database population.

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
│   └── utils/             # Utility functions
```

### Features
- Automated data collection from MangaDex
- Data processing and transformation pipeline
- Database integration (MongoDB and PostgreSQL)
- Containerized deployment with Docker
- Scalable architecture using Airflow

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- MongoDB
- PostgreSQL

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

### Usage
1. The default Airflow credentials are:
   - Username: airflow
   - Password: airflow

2. Navigate to the DAGs section to monitor and trigger data pipeline workflows

### License
This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

---

## Tiếng Việt

### Tổng quan
Dự án này là một hệ thống pipeline dữ liệu được xây dựng bằng Apache Airflow để thu thập, xử lý và lưu trữ dữ liệu manga từ MangaDex. Hệ thống được container hóa bằng Docker và bao gồm các thành phần cho việc crawl web, xử lý dữ liệu và cập nhật cơ sở dữ liệu.

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
│   └── utils/             # Các hàm tiện ích
```

### Tính năng
- Tự động thu thập dữ liệu từ MangaDex
- Pipeline xử lý và chuyển đổi dữ liệu
- Tích hợp cơ sở dữ liệu (MongoDB và PostgreSQL)
- Triển khai container hóa với Docker
- Kiến trúc có khả năng mở rộng sử dụng Airflow

### Yêu cầu hệ thống
- Docker và Docker Compose
- Python 3.8+
- MongoDB
- PostgreSQL

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

### Sử dụng
1. Thông tin đăng nhập mặc định của Airflow:
   - Tên đăng nhập: airflow
   - Mật khẩu: airflow

2. Điều hướng đến phần DAGs để giám sát và kích hoạt các quy trình pipeline dữ liệu

### Giấy phép
Dự án này được cấp phép theo Apache License 2.0 - xem file LICENSE để biết thêm chi tiết. 
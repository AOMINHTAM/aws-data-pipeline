#!/usr/bin/env python3
"""
=============================================================================
AWS E-Commerce Data Pipeline - Export Redshift Marts to CSV
=============================================================================
Script tự động kết nối vào Amazon Redshift Serverless và xuất các bảng
marts (Star Schema) đã được dbt biến đổi và làm sạch thành các file CSV.

Output folder: exports/
- 1_master_orders_analytics.csv   (Đơn hàng + Khách hàng + Vận chuyển + Đánh giá)
- 2_dim_products.csv             (Danh mục sản phẩm đã dịch tiếng Anh, kích thước)
- 3_dim_customers.csv            (Khách hàng, LTV, phân khúc One-time/Returning)
- 4_order_items_product_sales.csv (Chi tiết từng món hàng kèm phân loại danh mục)
=============================================================================
"""

import os
import sys
import time
from pathlib import Path

# --- Tự động load file .env nếu chưa có môi trường ---
def load_dotenv_fallback(env_path: Path):
    """Fallback tự động đọc file .env nếu chưa cài thư viện python-dotenv."""
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key not in os.environ:
                os.environ[key] = val

# Tìm file .env ở thư mục gốc project
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv_fallback(PROJECT_ROOT / ".env")

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

# --- Kiểm tra dependencies ---
try:
    import redshift_connector
except ImportError:
    print("\n❌ LỖI: Chưa cài đặt thư viện 'redshift-connector'!")
    print("👉 Hãy chạy lệnh sau trên terminal để cài đặt:")
    print("   pip install redshift-connector pandas\n")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("\n❌ LỖI: Chưa cài đặt thư viện 'pandas'!")
    print("👉 Hãy chạy lệnh sau trên terminal để cài đặt:")
    print("   pip install pandas\n")
    sys.exit(1)

# --- Cấu hình kết nối Redshift ---
REDSHIFT_HOST = os.getenv(
    "REDSHIFT_HOST", 
    "ecommerce-workgroup.205506434601.us-east-1.redshift-serverless.amazonaws.com"
)
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", "5439"))
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DB", "dev")
REDSHIFT_USER = os.getenv("REDSHIFT_USER", "admin")
REDSHIFT_PASSWORD = os.getenv("DBT_REDSHIFT_PASSWORD") or os.getenv("REDSHIFT_PASSWORD")

if not REDSHIFT_PASSWORD:
    print("⚠️  Không tìm thấy mật khẩu Redshift trong biến môi trường hoặc file .env!")
    REDSHIFT_PASSWORD = input("🔑 Vui lòng nhập mật khẩu admin Redshift: ").strip()

OUTPUT_DIR = PROJECT_ROOT / "exports"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --- Danh sách các truy vấn cần xuất file CSV ---
QUERIES = {
    "1_master_orders_analytics.csv": {
        "description": "Bảng phân tích tổng hợp đơn hàng (Orders + Customers + Delivery + Revenue)",
        "sql": """
            SELECT 
                o.order_id,
                o.customer_id,
                c.customer_unique_id,
                c.city AS customer_city,
                c.state AS customer_state,
                c.customer_segment,
                o.order_status,
                o.order_purchase_timestamp,
                o.order_delivered_customer_date,
                o.order_estimated_delivery_date,
                o.total_items,
                o.total_item_value,
                o.total_freight_value,
                o.total_payment_value,
                o.primary_payment_type,
                o.max_installments,
                o.delivery_duration_days,
                o.estimated_delivery_duration_days,
                o.delay_days,
                o.is_delayed,
                o.review_score
            FROM public_marts.fct_orders o
            LEFT JOIN public_marts.dim_customers c
                ON o.customer_id = c.customer_id
            ORDER BY o.order_purchase_timestamp DESC;
        """
    },
    "2_dim_products.csv": {
        "description": "Danh mục sản phẩm (Tên tiếng Anh chuẩn hóa, kích thước, trọng lượng)",
        "sql": """
            SELECT 
                product_id,
                category_name,
                category_name_portuguese,
                product_name_length,
                product_description_length,
                product_photos_qty,
                product_weight_g,
                product_length_cm,
                product_height_cm,
                product_width_cm
            FROM public_marts.dim_products
            ORDER BY category_name;
        """
    },
    "3_dim_customers.csv": {
        "description": "Khách hàng & Giá trị vòng đời (LTV, số lượng đơn, phân khúc)",
        "sql": """
            SELECT 
                customer_id,
                customer_unique_id,
                city,
                state,
                zip_code,
                first_order_date,
                most_recent_order_date,
                total_orders,
                lifetime_spend,
                customer_segment
            FROM public_marts.dim_customers
            ORDER BY lifetime_spend DESC;
        """
    },
    "4_order_items_product_sales.csv": {
        "description": "Chi tiết từng item trong đơn hàng kèm tên danh mục sản phẩm",
        "sql": """
            SELECT 
                oi.order_id,
                oi.order_item_id,
                oi.product_id,
                p.category_name,
                oi.price,
                oi.freight_value,
                oi.total_item_value
            FROM public_staging.stg_order_items oi
            LEFT JOIN public_marts.dim_products p
                ON oi.product_id = p.product_id;
        """
    }
}

def main():
    print("=" * 70)
    print("🚀 BẮT ĐẦU XUẤT DỮ LIỆU TỪ AWS REDSHIFT SERVERLESS SANG CSV")
    print("=" * 70)
    print(f"📍 Host:     {REDSHIFT_HOST}")
    print(f"📍 Database: {REDSHIFT_DATABASE}")
    print(f"📍 User:     {REDSHIFT_USER}")
    print(f"📁 Output:   {OUTPUT_DIR}\n")

    start_total_time = time.time()

    # 1. Kết nối vào Redshift
    print("🔌 Đang kết nối tới Redshift Serverless...")
    try:
        conn = redshift_connector.connect(
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT,
            database=REDSHIFT_DATABASE,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
            ssl=True
        )
        print("✅ Kết nối Redshift thành công!\n")
    except Exception as e:
        print(f"\n❌ Kết nối thất bại: {e}")
        print("\n💡 GỢI Ý KHẮC PHỤC:")
        print("1. Kiểm tra Redshift Workgroup đã bật 'Publicly Accessible = Turn On' chưa.")
        print("2. Kiểm tra Security Group Inbound Rule đã mở Port 5439 cho IP hiện tại (hoặc 0.0.0.0/0) chưa.")
        print("3. Nếu bạn đang bật Cloudflare WARP (1.1.1.1) hoặc VPN, hãy tạm tắt để tránh xung đột IP.")
        print("4. Kiểm tra mật khẩu trong file .env (DBT_REDSHIFT_PASSWORD).")
        sys.exit(1)

    # 2. Thực thi từng query và lưu vào CSV
    cursor = conn.cursor()
    results_summary = []

    try:
        for idx, (filename, query_info) in enumerate(QUERIES.items(), 1):
            file_path = OUTPUT_DIR / filename
            desc = query_info["description"]
            sql = query_info["sql"]

            print(f"[{idx}/{len(QUERIES)}] Đang truy vấn: {filename}...")
            print(f"    ℹ️  {desc}")
            query_start = time.time()

            cursor.execute(sql)
            
            # Sử dụng native fetch_dataframe của redshift_connector để có hiệu năng cao nhất
            df = cursor.fetch_dataframe()
            query_duration = time.time() - query_start

            # Xuất ra CSV
            df.to_csv(file_path, index=False, encoding="utf-8")
            file_size_mb = file_path.stat().st_size / (1024 * 1024)

            print(f"    ✅ Hoàn thành: {len(df):,} dòng | {file_size_mb:.2f} MB | {query_duration:.2f}s")
            print(f"    💾 Lưu tại: {file_path}\n")

            results_summary.append({
                "file": filename,
                "rows": len(df),
                "size_mb": file_size_mb,
                "time_sec": query_duration
            })

    except Exception as e:
        print(f"\n❌ Đã xảy ra lỗi trong quá trình xuất dữ liệu: {e}")
    finally:
        cursor.close()
        conn.close()
        print("🔌 Đã đóng kết nối Redshift.")

    # 3. Bảng tổng kết
    total_duration = time.time() - start_total_time
    print("\n" + "=" * 70)
    print("📊 TỔNG KẾT QUÁ TRÌNH XUẤT DỮ LIỆU:")
    print("=" * 70)
    total_rows = 0
    total_size = 0.0

    for item in results_summary:
        print(f" • {item['file']:<32} | {item['rows']:>8,} dòng | {item['size_mb']:>6.2f} MB | {item['time_sec']:>5.2f}s")
        total_rows += item["rows"]
        total_size += item["size_mb"]

    print("-" * 70)
    print(f"🎉 TỔNG CỘNG: {len(results_summary)} files | {total_rows:,} dòng | {total_size:.2f} MB | Thời gian: {total_duration:.2f}s")
    print(f"📂 Thư mục chứa file: {OUTPUT_DIR.resolve()}")
    print("=" * 70)

if __name__ == "__main__":
    main()

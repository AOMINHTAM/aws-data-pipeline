-- ====================================================================
-- DDL TẠO SCHEMA VÀ CÁC BẢNG THÔ CHO BỘ DỮ LIỆU OLIST E-COMMERCE
-- Lưu ý: Đổi tên schema thành `raw_data` vì trong Redshift từ `raw` là Reserved Keyword.
-- ====================================================================

CREATE SCHEMA IF NOT EXISTS raw_data;

-- 1. Bảng Đơn hàng (Orders)
CREATE TABLE IF NOT EXISTS raw_data.orders (
    order_id                        VARCHAR(50),
    customer_id                     VARCHAR(50),
    order_status                    VARCHAR(30),
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP
);

-- 2. Bảng Chi tiết mục đơn hàng (Order Items)
CREATE TABLE IF NOT EXISTS raw_data.order_items (
    order_id                        VARCHAR(50),
    order_item_id                   INT,
    product_id                      VARCHAR(50),
    seller_id                       VARCHAR(50),
    shipping_limit_date             TIMESTAMP,
    price                           DECIMAL(10, 2),
    freight_value                   DECIMAL(10, 2)
);

-- 3. Bảng Thanh toán (Payments)
CREATE TABLE IF NOT EXISTS raw_data.order_payments (
    order_id                        VARCHAR(50),
    payment_sequential              INT,
    payment_type                    VARCHAR(30),
    payment_installments            INT,
    payment_value                   DECIMAL(10, 2)
);

-- 4. Bảng Đánh giá đơn hàng (Reviews)
CREATE TABLE IF NOT EXISTS raw_data.order_reviews (
    review_id                       VARCHAR(50),
    order_id                        VARCHAR(50),
    review_score                    INT,
    review_comment_title            VARCHAR(255),
    review_comment_message          VARCHAR(2000),
    review_creation_date            TIMESTAMP,
    review_answer_timestamp         TIMESTAMP
);

-- 5. Bảng Khách hàng (Customers)
CREATE TABLE IF NOT EXISTS raw_data.customers (
    customer_id                     VARCHAR(50),
    customer_unique_id              VARCHAR(50),
    customer_zip_code_prefix        VARCHAR(20),
    customer_city                   VARCHAR(100),
    customer_state                  VARCHAR(10)
);

-- 6. Bảng Sản phẩm (Products)
CREATE TABLE IF NOT EXISTS raw_data.products (
    product_id                      VARCHAR(50),
    product_category_name           VARCHAR(100),
    product_name_lenght             INT,
    product_description_lenght      INT,
    product_photos_qty              INT,
    product_weight_g                INT,
    product_length_cm               INT,
    product_height_cm               INT,
    product_width_cm                INT
);

-- 7. Bảng Người bán (Sellers)
CREATE TABLE IF NOT EXISTS raw_data.sellers (
    seller_id                       VARCHAR(50),
    seller_zip_code_prefix          VARCHAR(20),
    seller_city                     VARCHAR(100),
    seller_state                    VARCHAR(10)
);

-- 8. Bảng Dịch tên danh mục sản phẩm (Category Translation)
CREATE TABLE IF NOT EXISTS raw_data.product_category_name_translation (
    product_category_name           VARCHAR(100),
    product_category_name_english   VARCHAR(100)
);

-- 9. Bảng Tọa độ địa lý (Geolocation)
CREATE TABLE IF NOT EXISTS raw_data.geolocation (
    geolocation_zip_code_prefix     VARCHAR(20),
    geolocation_lat                 FLOAT,
    geolocation_lng                 FLOAT,
    geolocation_city                VARCHAR(100),
    geolocation_state               VARCHAR(10)
);

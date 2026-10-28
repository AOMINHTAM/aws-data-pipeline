-- ====================================================================
-- CÂU LỆNH COPY DỮ LIỆU TỪ S3 VÀO CÁC BẢNG RAW_DATA TRÊN REDSHIFT
-- S3 Bucket: olist-raw-data-amt-us-east-1-205506434601-us-east-1-an
-- IAM Role:  arn:aws:iam::205506434601:role/RedshiftS3ReadOnlyRole
-- ====================================================================

-- 1. Load Orders
COPY raw_data.orders
FROM 's3://olist-raw-data-amt-us-east-1-205506434601-us-east-1-an/raw/olist_orders_dataset.csv'
IAM_ROLE 'arn:aws:iam::205506434601:role/RedshiftS3ReadOnlyRole'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1'
TIMEFORMAT 'auto';

-- 2. Load Order Items
COPY raw_data.order_items
FROM 's3://olist-raw-data-amt-us-east-1-205506434601-us-east-1-an/raw/olist_order_items_dataset.csv'
IAM_ROLE 'arn:aws:iam::205506434601:role/RedshiftS3ReadOnlyRole'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1'
TIMEFORMAT 'auto';

-- 3. Load Order Payments
COPY raw_data.order_payments
FROM 's3://olist-raw-data-amt-us-east-1-205506434601-us-east-1-an/raw/olist_order_payments_dataset.csv'
IAM_ROLE 'arn:aws:iam::205506434601:role/RedshiftS3ReadOnlyRole'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1'
TIMEFORMAT 'auto';

-- 4. Load Order Reviews
COPY raw_data.order_reviews
FROM 's3://olist-raw-data-amt-us-east-1-205506434601-us-east-1-an/raw/olist_order_reviews_dataset.csv'
IAM_ROLE 'arn:aws:iam::205506434601:role/RedshiftS3ReadOnlyRole'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1'
TIMEFORMAT 'auto';

-- 5. Load Customers
COPY raw_data.customers
FROM 's3://olist-raw-data-amt-us-east-1-205506434601-us-east-1-an/raw/olist_customers_dataset.csv'
IAM_ROLE 'arn:aws:iam::205506434601:role/RedshiftS3ReadOnlyRole'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1'
TIMEFORMAT 'auto';

-- 6. Load Products
COPY raw_data.products
FROM 's3://olist-raw-data-amt-us-east-1-205506434601-us-east-1-an/raw/olist_products_dataset.csv'
IAM_ROLE 'arn:aws:iam::205506434601:role/RedshiftS3ReadOnlyRole'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1'
TIMEFORMAT 'auto';

-- 7. Load Sellers
COPY raw_data.sellers
FROM 's3://olist-raw-data-amt-us-east-1-205506434601-us-east-1-an/raw/olist_sellers_dataset.csv'
IAM_ROLE 'arn:aws:iam::205506434601:role/RedshiftS3ReadOnlyRole'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1'
TIMEFORMAT 'auto';

-- 8. Load Product Category Translation
COPY raw_data.product_category_name_translation
FROM 's3://olist-raw-data-amt-us-east-1-205506434601-us-east-1-an/raw/product_category_name_translation.csv'
IAM_ROLE 'arn:aws:iam::205506434601:role/RedshiftS3ReadOnlyRole'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1';

-- 9. Load Geolocation
COPY raw_data.geolocation
FROM 's3://olist-raw-data-amt-us-east-1-205506434601-us-east-1-an/raw/olist_geolocation_dataset.csv'
IAM_ROLE 'arn:aws:iam::205506434601:role/RedshiftS3ReadOnlyRole'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1';

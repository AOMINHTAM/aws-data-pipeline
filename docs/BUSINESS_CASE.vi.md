# 🎯 BÀI TOÁN NGHIỆP VỤ, GIÁ TRỊ KINH DOANH & CÁC CHỈ SỐ PHÂN TÍCH (BUSINESS CASE)

> 🌐 **Ngôn ngữ / Language:** [English](BUSINESS_CASE.md) | **Tiếng Việt**
>
> Tài liệu này giải thích **vì sao pipeline này tồn tại dưới góc nhìn nghiệp vụ (Business)**, các "bẫy dữ liệu" thực tế của sàn TMĐT Olist được xử lý như thế nào, và từng bảng **dbt Star Schema** cùng bộ CSV xuất ra phục vụ trực tiếp cho các phòng ban **Tài chính/Bán hàng, CRM/Marketing, Vận hành Logistics và Quản lý Ngành hàng**.

---

## 1. Bối cảnh Nghiệp vụ: Vì sao dữ liệu thô chưa thể dùng để ra quyết định?

Trên một sàn thương mại điện tử (như Olist tại Brazil), hệ thống giao dịch gốc sinh ra **9 file CSV tách rời** (đơn hàng, chi tiết sản phẩm, thanh toán, đánh giá, khách hàng, sản phẩm, người bán, bảng dịch danh mục, tọa độ địa lý). Nếu đưa thẳng dữ liệu thô này lên Power BI hoặc báo cáo cho ban điều hành, doanh nghiệp sẽ gặp ngay 4 rủi ro lớn:

1. **Lệch số liệu Doanh thu & Thực thu:** Giá hàng hóa, phí vận chuyển, giá trị hóa đơn và tiền khách quẹt thẻ/trả góp trên mỗi đơn hàng không giống nhau — khiến báo cáo của Sales và Kế toán mâu thuẫn.
2. **Đếm sai Khách hàng & Tỷ lệ Quay lại:** Hệ thống gốc cấp **một `customer_id` mới cho mỗi đơn hàng**. Nếu đếm hoặc gom theo `customer_id`, doanh nghiệp sẽ tưởng nhầm gần như 100% là khách mới và tính sai hoàn toàn **Customer Lifetime Value (LTV)**.
3. **Không đo được SLA Giao hàng & Tác động tới Hài lòng Khách hàng:** Dữ liệu gốc chỉ có các mốc thời gian thô (`order_purchase_timestamp`, `order_delivered_customer_date`, `order_estimated_delivery_date`), chưa có thời gian giao thực tế, số ngày trễ hay trạng thái SLA để đối chiếu với điểm `review_score`.
4. **Danh mục chưa chuẩn hóa & Rủi ro hỏng dữ liệu báo cáo:** Tên ngành hàng dùng tiếng Bồ Đào Nha (`product_category_name`), tên cột gốc có lỗi chính tả (`product_name_lenght`), và cách nạp `TRUNCATE + COPY` ngây thơ có thể xóa trắng bảng báo cáo nếu gặp một file CSV rỗng hoặc lỗi cấu trúc.

Pipeline này giải quyết trọn vẹn các vấn đề trên bằng cách biến đổi dữ liệu thô thành **Kimball Star Schema (`public_marts`)** trên Amazon Redshift Serverless và xuất ra **4 bộ dữ liệu sạch** phục vụ phân tích BI.

---

## 2. Năm Bài toán Nghiệp vụ Cốt lõi Đã Giải quyết

| Phòng ban / Đối tượng | Câu hỏi Kinh doanh cần trả lời | Vấn đề ở Dữ liệu gốc | Giải pháp Triển khai trong Pipeline |
| :--- | :--- | :--- | :--- |
| **Tài chính & Bán hàng (Finance / Sales)** | Tiền hàng thuần, phí ship, tổng hóa đơn và tiền thực thu khác nhau ra sao? Khách thanh toán bằng hình thức nào? | Một đơn có thể gồm nhiều `order_items` và nhiều dòng `order_payments` (trả góp thẻ tín dụng, boleto, voucher). | [`fct_orders.sql`](../dbt/models/marts/fct_orders.sql) tách bạch `total_item_value`, `total_freight_value`, `total_order_cost` và `total_payment_value`; xác định `primary_payment_type` theo phương thức có **giá trị thanh toán lớn nhất** (không dùng `MIN` theo bảng chữ cái) và lưu `max_installments`. |
| **CRM & Growth Marketing** | Có bao nhiêu khách hàng thật? Tỷ lệ khách quay lại và LTV thực tế của từng khách là bao nhiêu? | Mỗi đơn hàng sinh một `customer_id` mới, trong khi con người thật được định danh qua `customer_unique_id`. | [`dim_customers.sql`](../dbt/models/marts/dim_customers.sql) gom toàn bộ lịch sử đơn và thanh toán theo **`customer_unique_id`** (`first_order_date`, `most_recent_order_date`, `total_orders`, `lifetime_spend`) và phân khúc `One-Time Customer` vs. `Returning Customer` — giữ chính xác tuyệt đối cả khi chạy incremental. |
| **Vận hành & Logistics (Operations)** | Tỷ lệ giao đúng hạn theo từng bang/thành phố là bao nhiêu? Giao trễ kéo điểm đánh giá xuống thế nào? | Bảng `orders` chỉ chứa timestamp thô, chưa có chỉ số SLA hay trạng thái giao hàng chuẩn hóa. | [`fct_orders.sql`](../dbt/models/marts/fct_orders.sql) tính sẵn `delivery_duration_days`, `estimated_delivery_duration_days`, `delay_days`, `is_delayed` (`1`/`0`/`NULL`), chuẩn hóa `delivery_status` (`On Time`, `Delayed`, `Pending Delivery`) gắn với `review_score` và địa lý (`city`, `state`). |
| **Quản lý Ngành hàng (Merchandising)** | Ngành hàng nào bán chạy nhất (bằng tiếng Anh)? Kích thước/trọng lượng sản phẩm ảnh hưởng phí ship ra sao? | Tên ngành hàng bằng tiếng Bồ Đào Nha, có sản phẩm thiếu bản dịch và cột gốc bị sai chính tả (`_lenght`). | [`stg_products.sql`](../dbt/models/staging/stg_products.sql) sửa tên cột (`product_name_length`, `product_description_length`); [`dim_products.sql`](../dbt/models/marts/dim_products.sql) chuẩn hóa `category_name` qua `coalesce(product_category_name_english, product_category_name, 'unknown')` kèm `product_weight_g` và kích thước dài/rộng/cao. |
| **Quản trị Dữ liệu & FinOps** | Dữ liệu có cập nhật kịp thời, minh bạch nguồn gốc và không đốt tiền cloud nhàn rỗi không? | Polling theo lịch vừa chậm vừa tốn request; `TRUNCATE` dễ làm mất dữ liệu lịch sử khi file lỗi; thiếu nhật ký đối soát. | Kiến trúc Event-Driven `S3 → SQS → Airflow 3 AssetWatcher` nạp dữ liệu ngay khi file tới; `validate_csv_header` + `ensure_non_empty_stage` + giao dịch nguyên tử bảo vệ bảng đích; [`ops.pipeline_ingestion_audit`](../scripts/create_ops_audit_table.sql) lưu vết từng lô (`SWAPPED` → `ARCHIVED` → `COMPLETED` / `FAILED`); Redshift Serverless tự ngủ `$0` compute khi rỗng. |

---

## 3. Tầng Dữ liệu Phân tích & Đầu ra cho BI

### 3.1. Mô hình Kimball Star Schema (`public_marts`)
1. **[`fct_orders`](../dbt/models/marts/fct_orders.sql)** *(Incremental Fact — `unique_key: order_id`, `delete+insert`)*
   * **Độ mịn (Grain):** 1 dòng = 1 đơn hàng (`order_id`).
   * **Đảm bảo số liệu luôn đúng khi chạy Incremental:** CTE `affected_order_ids` quét thay đổi từ **cả 4 bảng nguồn** (`stg_orders`, `stg_order_items`, `stg_payments`, `stg_reviews`), nhờ đó các đơn hàng cũ mới được giao xong, mới cập nhật thanh toán hoặc mới có đánh giá sao đều tự động đồng bộ vào Fact mà không cần dựng lại toàn bảng.
2. **[`dim_customers`](../dbt/models/marts/dim_customers.sql)** *(Incremental Dimension — `unique_key: customer_id`, `delete+insert`)*
   * **Độ mịn (Grain):** 1 dòng = 1 `customer_id` theo đơn, được làm giàu bằng **chỉ số trọn đời cấp độ khách thật (`customer_unique_id`)**.
   * **Bảo toàn LTV khi chạy Incremental:** Khi phát hiện `customer_unique_id` có đơn mới (lookback 3 ngày), `customer_id` mới hoặc thay đổi `lifetime_spend`/`total_orders`/địa chỉ, model tự động gom lại toàn bộ lịch sử đơn của đúng khách đó trước khi `delete+insert` để LTV và nhãn `Returning Customer` không bao giờ bị lệch.
3. **[`dim_products`](../dbt/models/marts/dim_products.sql)** *(Dimension Table)*
   * **Độ mịn (Grain):** 1 dòng = 1 mã sản phẩm (`product_id`) kèm `category_name` tiếng Anh, tên gốc tiếng Bồ Đào Nha, số lượng ảnh (`product_photos_qty`), khối lượng và kích thước đóng gói.

### 3.2. Bộ 4 File CSV Sạch Phục vụ BI ([`scripts/export_to_csv.py`](../scripts/export_to_csv.py) → `exports/`)
Để Power BI và các công cụ phân tích offline chạy mượt mà với **chi phí `$0` Redshift RPU nhàn rỗi**, script xuất sẵn 4 bộ dataset chuẩn hóa:

| Tên file xuất | Nguồn từ Redshift | Bài toán phân tích phục vụ |
| :--- | :--- | :--- |
| **`1_master_orders_analytics.csv`** | `public_marts.fct_orders` + `dim_customers` | Dashboard điều hành (GMV, tiền thực thu, số đơn, AOV), phân bố đơn theo `customer_state`/`city`, tỷ lệ giao đúng hạn (`delivery_status`), và tương quan giữa giao trễ với `review_score`. |
| **`2_dim_products.csv`** | `public_marts.dim_products` | Lọc theo ngành hàng tiếng Anh, phân tích đặc tính vật lý sản phẩm (khối lượng/kích thước ảnh hưởng chi phí vận chuyển) và chất lượng hiển thị (số ảnh, độ dài mô tả). |
| **`3_dim_customers.csv`** | `public_marts.dim_customers` | Phân tích chân dung khách hàng (`One-Time Customer` vs. `Returning Customer`), cohort giữ chân khách (`first_order_date` vs. `most_recent_order_date`), và phân tích Pareto nhóm khách hàng LTV cao. |
| **`4_order_items_product_sales.csv`** | `public_staging.stg_order_items` + `dim_products` | Phân tích doanh thu thuần và phí vận chuyển chi tiết tới từng món hàng theo `category_name`, số lượng món trung bình mỗi đơn và top ngành hàng/SKU bán chạy. |

---

## 4. Hợp đồng Chất lượng Dữ liệu Bảo vệ Chỉ số Kinh doanh

Mỗi con số kinh doanh trước khi tới tay người dùng cuối đều được bảo vệ qua 2 lớp kiểm soát:

* **Lớp Bảo vệ Tầng Nạp (Trước khi ghi vào DWH):**
  * [`validate_csv_header`](../include/ingestion_guards.py) phát hiện sớm sai lệch schema/header của file CSV nguồn trước khi nạp vào Redshift.
  * [`ensure_non_empty_stage`](../include/ingestion_guards.py) chặn đứng file CSV 0 dòng dữ liệu và `rollback()` giao dịch (`autocommit=False`), bảo vệ dữ liệu báo cáo hiện có không bị xóa nhầm.
* **Lớp Kiểm thử Chất lượng dbt (`9/9` Automated Tests trong [`dbt/models/marts/schema.yml`](../dbt/models/marts/schema.yml)):**
  * Đảm bảo khóa chính không trùng/không rỗng (`unique`, `not_null`) trên `fct_orders.order_id`, `dim_customers.customer_id`, `dim_products.product_id`, và `not_null` trên `customer_unique_id`.
  * Đảm bảo toàn vẹn tham chiếu (`relationships`) từ `fct_orders.customer_id` sang `dim_customers.customer_id`.
  * Khóa tập giá trị hợp lệ (`accepted_values`) cho `fct_orders.delivery_status` chỉ thuộc `['Pending Delivery', 'Delayed', 'On Time']`.

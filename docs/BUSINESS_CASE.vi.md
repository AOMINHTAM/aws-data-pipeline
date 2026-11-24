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

### 3.1. Mô hình Kimball Star Schema (`public_marts` — 2 Facts + 2 Dimensions)
1. **[`fct_orders`](../dbt/models/marts/fct_orders.sql)** *(Incremental Fact — `unique_key: order_id`, `delete+insert`)*
   * **Độ mịn (Grain):** 1 dòng = 1 đơn hàng (`order_id`, `99,441` dòng), gắn khóa ngoại `customer_unique_id` (`N:1`) nối tới `dim_customers`.
   * **Đảm bảo số liệu luôn đúng khi chạy Incremental:** CTE `affected_order_ids` quét thay đổi từ các bảng nguồn (`stg_orders`, `stg_customers`, `stg_order_items`, `stg_payments`, `stg_reviews`), giúp các đơn hàng cũ mới giao xong, mới cập nhật thanh toán hoặc mới có đánh giá sao tự động đồng bộ vào Fact.
2. **[`fct_order_items`](../dbt/models/marts/fct_order_items.sql)** *(Incremental Line-Item Fact — `unique_key: order_item_key`, `delete+insert`)*
   * **Độ mịn (Grain):** 1 dòng = 1 món hàng trong đơn (`(order_id, order_item_id)`, `112,650` dòng), kết nối trực tiếp `fct_orders`, `dim_customers` và `dim_products` ngay ở tầng Gold để BI không phải truy vấn ngược về tầng Silver.
3. **[`dim_customers`](../dbt/models/marts/dim_customers.sql)** *(Incremental Dimension — `unique_key: customer_unique_id`, `delete+insert`)*
   * **Độ mịn (Grain):** 1 dòng = 1 khách hàng thật (`customer_unique_id`, **`96,096` dòng**), chọn địa chỉ của đơn hàng gần nhất qua `row_number()` và xóa bỏ hoàn toàn bẫy nhân đôi LTV trên BI (`SUM(lifetime_spend) == SUM(fct_orders.total_payment_value)`).
4. **[`dim_products`](../dbt/models/marts/dim_products.sql)** *(Dimension Table — `DISTSTYLE ALL`)*
   * **Độ mịn (Grain):** 1 dòng = 1 mã sản phẩm (`product_id`, `32,951` dòng) kèm `category_name` tiếng Anh, tên gốc tiếng Bồ Đào Nha, số lượng ảnh (`product_photos_qty`), khối lượng và kích thước đóng gói.

### 3.2. Bộ 4 File CSV Sạch Phục vụ BI ([`scripts/export_to_csv.py`](../scripts/export_to_csv.py) → `exports/` — 100% Tầng Gold)
Để Power BI và các công cụ phân tích offline chạy mượt mà với **chi phí `$0` Redshift RPU nhàn rỗi**, script xuất sẵn 4 bộ dataset chuẩn hóa (`341,138` dòng / `49.84 MB`) hoàn toàn từ tầng `public_marts`:

| Tên file xuất | Nguồn từ tầng Gold (`public_marts`) | Bài toán phân tích phục vụ |
| :--- | :--- | :--- |
| **`1_master_orders_analytics.csv`** | `public_marts.fct_orders` + `dim_customers` | Dashboard điều hành (GMV, tiền thực thu, số đơn, AOV), phân bố đơn theo `customer_state`/`city`, tỷ lệ giao đúng hạn (`delivery_status`), và tương quan giữa giao trễ với `review_score` (`99,441` dòng). |
| **`2_dim_products.csv`** | `public_marts.dim_products` | Lọc theo ngành hàng tiếng Anh, phân tích đặc tính vật lý sản phẩm (khối lượng/kích thước ảnh hưởng chi phí vận chuyển) và chất lượng hiển thị (số ảnh, độ dài mô tả) (`32,951` dòng). |
| **`3_dim_customers.csv`** | `public_marts.dim_customers` | Phân tích chân dung khách hàng không trùng lặp (`One-Time Customer` vs. `Returning Customer`), cohort giữ chân khách và Pareto nhóm LTV cao (**`96,096` khách hàng thật**, cộng thẳng `SUM(lifetime_spend)` không lệch). |
| **`4_order_items_product_sales.csv`** | `public_marts.fct_order_items` + `dim_products` | Phân tích doanh thu thuần và phí vận chuyển chi tiết tới từng món hàng theo `category_name`, số lượng món trung bình mỗi đơn và top ngành hàng/SKU bán chạy (`112,650` dòng). |

---

## 4. Hợp đồng Chất lượng Dữ liệu Bảo vệ Chỉ số Kinh doanh

Mỗi con số kinh doanh trước khi tới tay người dùng cuối đều được bảo vệ qua **lớp nạp Python + toàn bộ 3 tầng Medallion (`146` dbt tests đã LIVE-verify trên Redshift Serverless: `PASS=156, WARN=1, ERROR=0` full-refresh; `PASS=56, WARN=0, ERROR=0` incremental)**:

* **Lớp Bảo vệ Tầng Nạp (Trước khi ghi vào DWH):**
  * [`validate_csv_header`](../include/ingestion_guards.py) phát hiện sớm sai lệch schema/header của file CSV nguồn trước khi nạp vào Redshift.
  * [`ensure_non_empty_stage`](../include/ingestion_guards.py) chặn đứng file CSV 0 dòng dữ liệu và `rollback()` giao dịch (`autocommit=False`), bảo vệ dữ liệu báo cáo hiện có không bị xóa nhầm.
* **Lớp Kiểm thử Tầng Bronze / Raw (`44` Tests trong [`dbt/models/staging/sources.yml`](../dbt/models/staging/sources.yml)):**
  * Khóa chặt tính duy nhất đơn & tổ hợp (`order_id`, `[order_id, order_item_id]`, `[order_id, payment_sequential]`, `[review_id, order_id]`, `customer_id`, `product_id`, `seller_id`), giá trị không âm (`non_negative`), và tập giá trị hợp lệ (`order_status`, `payment_type`, `review_score` `1–5`) ngay tại nguồn trước khi chạy Silver/Gold.
* **Lớp Kiểm thử Tầng Silver / Staging (`45` Tests trong [`dbt/models/staging/schema.yml`](../dbt/models/staging/schema.yml)):**
  * Kiểm tra hợp đồng đầu ra của 7 views chuẩn hóa, `non_negative` trên `total_item_value`, toàn vẹn tham chiếu (`relationships`) giữa đơn hàng, món hàng, thanh toán, đánh giá, khách hàng, sản phẩm, và cảnh báo `severity: warn` cho `623` dòng sản phẩm trống/chưa dịch danh mục.
* **Lớp Kiểm thử Tầng Gold / Marts & Đối soát Chéo tầng (`51` Schema Tests trong [`dbt/models/marts/schema.yml`](../dbt/models/marts/schema.yml) + `6` Singular Tests trong [`dbt/tests/`](../dbt/tests/)):**
  * Khóa chính đơn & tổ hợp, khóa ngoại Star Schema (`fct_orders` & `fct_order_items` → `dim_customers`, `dim_products`), các chỉ số doanh thu/LTV `non_negative`, và `accepted_values` cho `delivery_status` & `customer_segment`.
  * 6 singular tests đối soát nghiệp vụ: phương trình `total_order_cost = total_item_value + total_freight_value`, khớp doanh thu Silver ↔ Gold (`fct_orders` vs `stg_order_items`), đối soát 2 bảng Fact (`fct_order_items` vs `fct_orders`), đối soát LTV không nhân đôi (`dim_customers.lifetime_spend` vs `fct_orders.total_payment_value`), tính nhất quán `delivery_status` / `is_delayed` / `delay_days`, và khớp `customer_segment` với `total_orders`.

cới (2025-26 trở đi)
- Merge với dữ liệu cũ, loại bỏ duplicates

#### **Bước 2: Transform (Chuyển đổi dữ liệu)**

**Tạo Dimension Tables:**
1. `create_dim_player()`: Hợp nhất dữ liệu từ player_season và player_match, tạo player_id
2. `create_dim_team()`: Đọc từ dim_team.csv, chuẩn hóa tên đội, tạo short_name
3. `create_dim_stadium()`: Trích xuất thông tin sân vận động
4. `create_dim_match()`: Tạo match_id từ các trận đấu unique, chuẩn hóa ngày tháng
5. `create_dim_season()`: Tạo bảng mùa giải

**Tạo Fact Tables:**
1. `create_fact_team_match()`: 
   - Map team → team_id, opponent → opponent_id, game → game_id
   - Chuẩn hóa round, venue, result
   - Giữ lại các metrics: GF, GA, xG, xGA, Possession, Formation

2. `create_fact_player_match()`:
   - Map player → player_id, team → team_id, game → game_id
   - Giữ lại 25+ metrics: goals, assists, xG, xA, passes, tackles, v.v.

3. `create_fact_team_point()`:
   - Map team → team_id, chuẩn hóa season (2024/25 → 2425)
   - Tách GF:GA thành GF và GA riêng biệt
   - Giữ lại Rank, MP, W, D, L, GD, Pts, Recent_Form

**Xử lý đặc biệt:**
- Chuẩn hóa tên đội (loại bỏ F.C., A.F.C., normalize variants)
- Xử lý multi-index headers từ FBref
- Loại bỏ dữ liệu không hợp lệ (NULL trong key columns)

#### **Bước 3: Load (Nạp vào database)**

**Dimension Tables (UPSERT):**
- Sử dụng `INSERT ... ON CONFLICT DO UPDATE`
- Cập nhật thông tin nếu đã tồn tại (ví dụ: cập nhật tên đội mới)

**Fact Tables (INSERT only new):**
- Sử dụng `INSERT ... ON CONFLICT DO NOTHING`
- Chỉ insert records mới, skip records đã tồn tại
- Đảm bảo không có duplicates

**Foreign Key Constraints:**
- Tất cả fact tables có foreign keys đến dimension tables
- Đảm bảo tính toàn vẹn dữ liệu

### 3.3. Triển khai và vận hành

**Setup ban đầu:**
```bash
# 1. Clone repository và cài đặt dependencies
pip install -r requirements.txt

# 2. Cấu hình database trong scr/database.ini

# 3. Khởi động Docker services
docker-compose up -d postgres
docker-compose up airflow-init
docker-compose up -d airflow-webserver airflow-scheduler
```

**Chạy thủ công (nếu cần):**
```bash
# Extract
python scr/Extract.py

# Transform
python scr/Transform.py

# Load
python scr/Load.py
```

**Giám sát:**
- Truy cập Airflow UI: `http://localhost:8080`
- Xem logs của từng task
- Kiểm tra dữ liệu trong PostgreSQL
- Chạy `quick_quality_check.py` để kiểm tra chất lượng dữ liệu

**Dashboard:**
```bash
streamlit run scr/ui.py
```

### 3.4. Xử lý lỗi và retry

- **Retry mechanism**: Mỗi task có thể retry tối đa 2 lần với delay 5 phút
- **Error handling**: Ghi log chi tiết khi có lỗi
- **Data validation**: Kiểm tra và loại bỏ dữ liệu không hợp lệ trong Transform
- **Incremental safety**: Merge logic đảm bảo không mất dữ liệu cũ

---

## 4. KẾT QUẢ ĐẠT ĐƯỢC

### 4.1. Dữ liệu thu thập được

**Phạm vi dữ liệu:**
- **5 mùa giải**: Từ 2020-21 đến 2024-25 (và tiếp tục cập nhật)
- **20 đội bóng**: Tất cả các đội tham gia Premier League
- **Hàng trăm cầu thủ**: Dữ liệu chi tiết về từng cầu thủ
- **Hàng nghìn trận đấu**: Thống kê đầy đủ cho mỗi trận đấu

**Loại dữ liệu:**
- **Thống kê đội bóng**: Kết quả, bàn thắng, xG, possession, formation
- **Thống kê cầu thủ**: Bàn thắng, kiến tạo, passes, tackles, xG, xA, v.v.
- **Bảng xếp hạng**: Overall, home, away standings với recent form

### 4.2. Hiệu quả hệ thống

**Tự động hóa:**
- ✅ Tự động thu thập dữ liệu mỗi tuần (thứ Tư 2:00 AM)
- ✅ Tự động xử lý và làm sạch dữ liệu
- ✅ Tự động cập nhật vào database
- ✅ Giảm thời gian xử lý từ vài giờ xuống còn vài phút

**Chất lượng dữ liệu:**
- ✅ Chuẩn hóa và nhất quán
- ✅ Loại bỏ duplicates và dữ liệu không hợp lệ
- ✅ Đảm bảo tính toàn vẹn với foreign keys
- ✅ Có thể audit và trace về nguồn gốc

**Khả năng mở rộng:**
- ✅ Dễ dàng thêm mùa giải mới
- ✅ Dễ dàng thêm metrics mới
- ✅ Có thể mở rộng sang các giải đấu khác

### 4.3. Dashboard và phân tích

**Dashboard Streamlit cung cấp:**

1. **Tổng quan mùa giải:**
   - KPI tổng quan (tổng số trận, bàn thắng, thẻ vàng/đỏ)
   - Bảng xếp hạng với đầy đủ thống kê
   - Top scorers và top assisters
   - Phân tích xG vs Goals (scatter plot)

2. **Phân tích đội bóng:**
   - KPI theo từng đội (GF, GA, xG, xGA, possession)
   - Top scorers của từng đội
   - So sánh hiệu suất giữa các đội

**Lợi ích:**
- Trực quan hóa dữ liệu một cách dễ hiểu
- Hỗ trợ ra quyết định dựa trên dữ liệu
- Theo dõi xu hướng và hiệu suất theo thời gian

### 4.4. Kiến trúc và công nghệ

**Ưu điểm của kiến trúc:**
- ✅ **Modular**: Tách biệt rõ ràng Extract, Transform, Load
- ✅ **Maintainable**: Code dễ đọc, dễ bảo trì
- ✅ **Scalable**: Có thể mở rộng dễ dàng
- ✅ **Reliable**: Có cơ chế retry và error handling
- ✅ **Containerized**: Dễ triển khai và quản lý với Docker

**Công nghệ hiện đại:**
- Apache Airflow cho workflow orchestration
- PostgreSQL cho data warehouse
- Docker cho containerization
- Streamlit cho interactive dashboard

### 4.5. Tác động và giá trị

**Đối với phân tích dữ liệu:**
- Cung cấp nền tảng dữ liệu sẵn sàng cho phân tích
- Giảm thời gian chuẩn bị dữ liệu từ vài giờ xuống vài phút
- Hỗ trợ các phân tích phức tạp với dữ liệu đầy đủ và chính xác

**Đối với nghiên cứu và phát triển:**
- Có thể sử dụng để nghiên cứu xu hướng bóng đá
- Hỗ trợ phân tích hiệu suất cầu thủ và đội bóng
- Có thể mở rộng cho các ứng dụng machine learning

**Đối với người dùng cuối:**
- Dashboard trực quan, dễ sử dụng
- Cập nhật dữ liệu tự động, luôn có thông tin mới nhất
- Hỗ trợ đưa ra quyết định dựa trên dữ liệu

---

## KẾT LUẬN

Hệ thống ETL dữ liệu bóng đá Premier League đã giải quyết thành công các thách thức ban đầu:

✅ **Tự động hóa** việc thu thập và xử lý dữ liệu  
✅ **Chuẩn hóa** dữ liệu từ nhiều nguồn khác nhau  
✅ **Cung cấp** nền tảng dữ liệu sẵn sàng cho phân tích  
✅ **Hỗ trợ** dashboard trực quan để xem và phân tích dữ liệu  

Hệ thống đã sẵn sàng để mở rộng và phát triển thêm các tính năng mới trong tương lai.

---

**Thời gian trình bày**: ~10 phút  
**Cấu trúc**: 4 phần chính (Đặt vấn đề, Giải pháp, Vận hành, Kết quả)  
**Slide đề xuất**: 12-15 slides

c
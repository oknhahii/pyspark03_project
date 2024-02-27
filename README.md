I) Xử lý dữ liệu có cấu trúc.
Dự án sẽ lấy các file dữ liệu có cấu trúc từ hai website:
+ Dữ liệu về dược sĩ tại Mĩ: https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug/data****
+ Dữ liệu về các thành phố tại Mĩ: https://simplemaps.com/data/us-cities
Sau khi load dữ liệu từ hai file vào dataframe ta thực hiện chuẩn hóa các dữ liệu. VD với dữ liệu số (Định lượng) bị null ta sẽ thay bằng mean với các dữ liệu chữ (Định tính) thì thay bằng giá trị mode ...
Ta thực hiện transform để tạo 2 báo cáo:
  + Thống kê dược sĩ theo các thành phố, khu vực.
  + Xếp hạng các dược sĩ.
Load các bảng đã transform vào PostgresQL database.

II) Xử lý dữ liệu không cấu trúc.
Lấy dữ liệu không có cấu trúc từ các email vị dụ. Email này được tạo ra nhờ việc hỏi ChatGPT.
Sau khi load các email và Rdd. Ta sẽ thực hiện đếm số từ của email, lấy ra tên người nhận và đánh giá thái độ của email.
Đưa các thông tin trên cùng với tên email vào 1 dataframe và load thành table trong PostgresQl database.

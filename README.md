# streaming-pyspark
 Ý tưởng chính của bài toán này là tạo ra được Streaming Image DataSource sử dụng Kafka và tiến hành xử lý Streaming với PySpark và lưu lại vào Kafka với 1 topic khác. 
Ở đây ta chọn kafka làm nơi lưu dữ liệu là do Kafka là nền tảng mạnh mẽ và linh hoạt cho việc xử lý dữ liệu streaming. Với nhiều ưu điểm như khả năng mở rộng cao, độ trễ thấp, độ tin cậy cao, dễ sử dụng, hỗ trợ đa dạng các nguồn dữ liệu, khả năng tích hợp cao, Kafka được sử dụng rộng rãi trong nhiều lĩnh vực khác nhau và là lựa chọn hàng đầu cho các doanh nghiệp muốn xây dựng hệ thống xử lý dữ liệu streaming hiệu quả và đáng tin cậy. Và đặc biệt kafka và spark đều là phiên bản Apache thường thấy nên độ tương thích tốt.
 ![image](https://github.com/ntp2003/streaming-pyspark/assets/95475230/3e4d8665-f919-419b-a51e-f91760072249)

Ở đây dữ liệu ở camera sẽ được đẩy vào trong kafka vào lưu trữ vào trong topic A. Sau đó spark sẽ tiến hành xử lý hình ảnh lấy được khi consuming topic A. Spark sẽ tiến hành phát hiện khuôn mặt có trong ảnh. Sau đó kết hợp với identifier để xác định khuôn mặt đó là ai và tiến hành khoanh vùng và gán nhãn trong ảnh. Nếu không xác định được sẽ gán nhãn là Unknown.
Cụ thể quá trình xử lý ở spark sẽ tiến hành như sau:
 
 ![image](https://github.com/ntp2003/streaming-pyspark/assets/95475230/a28daec8-a2a8-4ff4-bdc5-3081b9c866aa)

-	Đầu tiên dữ liệu đọc từ kafka dưới dạng structured streaming sẽ có dạng sau:
 ![image](https://github.com/ntp2003/streaming-pyspark/assets/95475230/cf586709-1b66-464a-a8a0-6e10f660e8fe)

-	Ở đây dữ liệu của hình ảnh sẽ được lưu trong cột value duới dạng binary nên chúng ta sẽ tiến hành chuyển nó về dạng mảng 3 chiều dưới định dạng RGB.
-	Sau đó tiến hành chúng ta sẽ tiến hành phát hiện khuôn mặt có trong ảnh và lưu lại thông tin vị trí khuôn mặt trong ảnh.
-	Tiếp theo, trước khi tiến hành nhận diện, chúng ta từ thông tin vị trí và mảng 3 chiều của ảnh tiến hành mã hóa các khuôn mặt đưa về dạng mảng (ở đây chúng ta chọn 128-dimensional encoding để mã hóa).
-	Kế đến, chúng ta sẽ kết hợp Identifier để tiến hành nhận diện và gán nhãn cho các khuôn mặt. Nếu không nhận diện được sẽ gán nhãn Unkown. Sau đó tiến hành đánh dấu vào mảng 3 chiều, sau đó chuyển nó về dạng binary.
Quá trình nhận dạng ở đây chúng ta sẽ thực hiện theo 2 cách đó là
-	Tạo 1 folder tập hợp các ảnh. Mỗi ảnh sẽ là ảnh chụp của 1 người. Sau đó spark sẽ đọc hình ảnh có trong folder này và gán nhãn mỗi bức ảnh ứng với tên file ảnh. Chúng ta cũng sẽ tiến hành mã hóa bằng 128-dimensional encoding cho mỗi bức ảnh. Sau đó tiến hành tính khoảng cách Euclid của chúng với hình ảnh đang xử lý trong spark. Khoảng cách càng nhỏ thì càng có khả năng hình ảnh đang xử lý là của label ứng với hình ảnh đang tính khoảng cách. Chúng ta sẽ dùng 1 ngưỡng để xác định, nếu thấp hơn ngưỡng này thì gán nhãn ứng với label của hình ảnh đó. Ngưỡng càng thấp độ tin cậy càng cao.
Thu thập dữ liệu hình ảnh khuôn mặt của những người cần nhận dạng. Mỗi người khoảng 100 - 200 tấm. Sau đó cũng sẽ tiến hành mã hóa bằng 128 - dimensional encoding và gán nhãn ứng với tên của từng người. Sau đó sử dụng những thuật toán có khả năng phân loại để tiến hành training tạo mô hình. Mô hình chúng ta chọn ở đây là hồi quy logistic. Sau đó đưa mô hình này vào nhận dạng hình ảnh đã được mã hóa đang được xử lý trong spark. Mô hình này đưa ra dự đoán khả năng của hỉnh ảnh này là của ai trong danh sách các nhãn được training. Chọn 1 ngưỡng khả năng để có thể khẳng định khuôn mặt trong ảnh là của ai. Ngưỡng càng cao thì nhận diện càng chính xác.


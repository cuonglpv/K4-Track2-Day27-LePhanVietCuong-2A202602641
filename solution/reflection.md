# Reflection

Kết quả cuối: practice 50.0 (bắt đủ 33/33 lỗi, không báo nhầm cái nào), public 46.9, private 32.41 với TPR 0.648, FPR 0 và không vượt ngân sách.

**Lỗi nào khó bắt nhất, và vì sao?**

Khó nhất là mấy lỗi "tinh vi" nằm sát dải dao động bình thường, nhất là feature skew và các batch dữ liệu lệch nhẹ. Ở practice và public mình gần như bắt hết, nhưng sang private thì TPR rơi xuống còn khoảng 0.65, nghĩa là cứ 3 lỗi thì sót 1. Report private chỉ trả về con số tổng nên mình không biết chính xác sót ở trụ cột nào, nhưng đoán là chủ yếu ở ai_infra và checks. Contracts và lineage thì gần như không có vùng xám: contract_diff trả violations rõ ràng, còn lineage chỉ cần so upstream thực tế với cặp raw.orders + raw.customers và xem downstream có bằng 0 không.

Với feature skew, mình đặt ngưỡng 0.65σ, nằm giữa giá trị sạch lớn nhất (~0.47) và giá trị lỗi nhỏ nhất (~0.97) quan sát được ở practice. Ngưỡng này an toàn về FPR, nhưng một lỗi skew 0.55σ chẳng hạn thì lọt qua. Hạ ngưỡng xuống thì lại đụng vào đuôi của dữ liệu sạch, mà mỗi lần báo nhầm bị phạt nặng hơn bỏ sót.

Với data_batch, mình thử công thức norm2 > 1.30 trong hướng dẫn trước, kết quả là 9 báo động giả trên practice. Lý do là hai thành phần null/null_max và stale/stale_max không trừ đi trung bình, nên batch bình thường đã có sẵn khoảng 0.5 đến 1.0 điểm từ hai chiều đó. Mình đổi sang tổng bình phương z-score của 5 chỉ số (thêm cả std_amount), với trung bình và độ lệch chuẩn học dần trong ctx.state từ các batch được coi là sạch, rồi so với ngưỡng chi-square 20.5 (p = 0.001). Practice về lại FPR = 0 và vẫn sạch khi hạ ngưỡng xuống 15.1, nên mình giữ 20.5 cho có biên an toàn.

Một chuyện ngoài lề nhưng tốn khá nhiều thời gian: in debug ra stderr cũng có thể làm treo harness. Parent không đọc stderr của child cho tới khi child chết, nên in quá khoảng 4KB là pipe đầy và cả hai cùng đứng. Lúc mình kill tiến trình thì file key với schedule của practice còn kẹt trong thư mục tạm, phải chuyển tay về. Sau đó mình bỏ hẳn ý định debug từng event và quay về so verdict với answer key.

**Cân bằng giữa chi phí gọi tool và TPR**

Mình chọn mỗi event gọi đúng một tool, không gọi lặp. Tool trả về giá trị thật, gọi lại cũng chẳng thêm thông tin gì. Payload của mỗi loại event lại giống hệt nhau theo template, nên cũng không có tín hiệu miễn phí nào để lọc bớt trước khi gọi.

Ở public, quét đủ tốn 240 credit trên ngân sách 220, bị phạt khoảng 1.8 điểm. Mình có tính thử: muốn vừa ngân sách thì phải bỏ qua 10 đến 20 event, và với tỉ lệ lỗi khoảng 25% thì sẽ sót chừng 3 lỗi, tức mất hơn 3 điểm TPR. Nên mình chấp nhận overage. Private có ngân sách 320 nên quét đủ vẫn nằm trong hạn mức.

Nếu làm lại, mình vẫn giữ quét đủ, nhưng sẽ dùng phần budget dư ở private (khoảng 20 credit) cho việc gì đó có ích hơn. Ví dụ dồn tín hiệu theo chuỗi thời gian thay vì chấm từng event độc lập.

**Nếu có thêm thời gian, mình sẽ cải tiến gì trong ctx.state?**

Thứ nhất, thay mean/std (Welford) bằng median và MAD cho phần học phân phối sạch. Một batch lỗi lọt vào tập "sạch" hiện tại có thể kéo lệch thống kê, còn median/MAD chịu được chuyện đó tốt hơn.

Thứ hai, áp dụng cách học online này cho cả feature drift và embedding, thay vì dùng ngưỡng cố định. Mỗi feature_view và mỗi corpus sẽ có phân phối riêng của mean_shift_sigma, centroid_shift và avg_doc_age_days. Từ đó có thể chấm theo z-score so với chính lịch sử của nó, và gộp shift với tuổi tài liệu thành một điểm chung.

Thứ ba, thêm EWMA hoặc CUSUM để bắt drift chậm. Một chuỗi batch cùng lệch nhẹ về một phía thì từng cái riêng lẻ không đáng báo, nhưng cộng dồn lại rất đáng ngờ. Đây có lẽ là chỗ có thể kéo TPR private lên mà không phải đánh đổi FPR.

# Reflection

Kết quả cuối (thang điểm tối đa 50): practice 49.66 (bắt đủ 33/33 lỗi, báo nhầm 1), public 47.93 (bắt đủ 39/39, báo nhầm 1), private 32.41 với TPR 0.648, FPR 0 và không vượt ngân sách.

**Lỗi nào khó bắt nhất, và vì sao?**

Khó nhất là mấy lỗi "tinh vi" nằm sát dải dao động bình thường, nhất là feature skew và các batch dữ liệu lệch nhẹ. Ở practice và public mình gần như bắt hết, nhưng sang private thì TPR rơi xuống còn khoảng 0.65, nghĩa là cứ 3 lỗi thì sót 1. Report private chỉ trả về con số tổng nên mình không biết chính xác sót ở trụ cột nào, nhưng đoán là chủ yếu ở ai_infra và checks. Contracts và lineage thì gần như không có vùng xám: contract_diff trả violations rõ ràng, còn lineage chỉ cần so upstream thực tế với cặp raw.orders + raw.customers và xem downstream có bằng 0 không.

Với feature skew, mình đặt ngưỡng 0.65σ, nằm giữa giá trị sạch lớn nhất (~0.47) và giá trị lỗi nhỏ nhất (~0.97) quan sát được ở practice. Ngưỡng này an toàn về FPR, nhưng một lỗi skew 0.55σ chẳng hạn thì lọt qua. Hạ ngưỡng xuống thì lại đụng vào đuôi của dữ liệu sạch, mà mỗi lần báo nhầm bị phạt nặng hơn bỏ sót.

Với data_batch, mình thử công thức norm2 > 1.30 trong hướng dẫn trước, kết quả là 9 báo động giả trên practice. Lý do là hai thành phần null/null_max và stale/stale_max không trừ đi trung bình, nên batch bình thường đã có sẵn khoảng 0.5 đến 1.0 điểm từ hai chiều đó. Mình đổi sang tổng bình phương z-score của 5 chỉ số (thêm cả std_amount), với tâm và độ phân tán học dần trong ctx.state từ các batch được coi là sạch, rồi so với ngưỡng chi-square 20.5 (p = 0.001). Chính kiểm tra này bắt được lỗi duy nhất mình từng sót ở public, đổi lại một lần báo nhầm.

Một chuyện ngoài lề nhưng tốn khá nhiều thời gian: in debug ra stderr cũng có thể làm treo harness. Parent không đọc stderr của child cho tới khi child chết, nên in quá khoảng 4KB là pipe đầy và cả hai cùng đứng. Lúc mình kill tiến trình thì file key với schedule của practice còn kẹt trong thư mục tạm, phải chuyển tay về. Sau đó mình bỏ hẳn ý định debug từng event và quay về so verdict với answer key.

**Cân bằng giữa chi phí gọi tool và TPR**

Mình chọn mỗi event gọi đúng một tool, không gọi lặp. Tool trả về giá trị thật, gọi lại cũng chẳng thêm thông tin gì. Payload của mỗi loại event lại giống hệt nhau theo template, nên cũng không có tín hiệu miễn phí nào để lọc bớt trước khi gọi.

Ở public, quét đủ tốn 240 credit trên ngân sách 220, bị phạt khoảng 1.8 điểm. Mình có tính thử: muốn vừa ngân sách thì phải bỏ qua 10 đến 20 event, và với tỉ lệ lỗi khoảng 25% thì sẽ sót chừng 3 lỗi, tức mất hơn 3 điểm TPR. Nên mình chấp nhận overage. Private có ngân sách 320 nên quét đủ vẫn nằm trong hạn mức.

Nếu làm lại, mình vẫn giữ quét đủ. Gọi thêm tool cũng không giúp được gì, vì cùng một event thì tool luôn trả về đúng một giá trị.

**Một lượt cải tiến, và vì sao nó không đổi được điểm private**

Sau lần nộp đầu, mình làm thêm một lượt. Mọi tín hiệu số (5 chỉ số của batch, độ trễ contract, thời gian chạy lineage, độ lệch feature có dấu, centroid shift và tuổi tài liệu) giờ đều có lịch sử sạch riêng trong ctx.state. Mỗi giá trị mới được chấm bằng robust z-score (median và MAD), và chỉ event không bị báo mới được ghi vào lịch sử, để lỗi không làm loãng phân phối tham chiếu. Ngưỡng cố định cũ vẫn giữ làm sàn.

Ngưỡng z mình chọn 4.5 chứ không phải 5.0, dù ở 5.0 practice sạch tuyệt đối. Lý do nằm ở công thức điểm: private có khoảng 54 lỗi và 146 event sạch, nên bắt thêm một lỗi được khoảng 0.93 điểm, còn báo nhầm một lần chỉ mất 0.21. Ép FPR về 0 bằng mọi giá là thiệt. Toàn bộ việc chọn ngưỡng mình chỉ làm trên practice và public, rồi chạy private đúng một lần.

Kết quả private ra y hệt lần đầu: 32.41, không thêm báo nhầm nào nhưng cũng không bắt thêm được lỗi nào. Mình nghĩ lý do là thế này: với feature skew, độ lệch sạch có σ khoảng 0.16, nên z = 4.5 tương đương khoảng 0.7σ, còn cao hơn cả ngưỡng cố định 0.65. Các lỗi tinh vi ở private chắc nằm dưới mức đó, cỡ 3 đến 4 σ của dữ liệu sạch. Mình cũng tính thử EWMA/CUSUM nhưng bỏ, vì lỗi trong practice xuất hiện rời rạc chứ không thành chuỗi trôi dần.

**Nếu có thêm thời gian, mình sẽ cải tiến gì trong ctx.state?**

Mình sẽ đặt ngưỡng z riêng cho từng tín hiệu thay vì một con số chung. Hai lần báo nhầm ở practice khi hạ ngưỡng đều đến từ độ trễ contract và batch, là những tín hiệu có đuôi dày. Còn feature và embedding trông gần chuẩn hơn, nên có thể hạ xuống quanh 3 đến 3.5 mà vẫn ít báo nhầm. Mình cũng muốn gộp centroid shift với tuổi tài liệu thành một điểm chung cho embedding, giống cách đã làm với batch. Cái khó là practice và public đều đã bắt đủ lỗi, nên không còn tín hiệu nào để chứng minh các thay đổi này giúp được gì. Thử chúng trên private nhiều lần thì lại thành overfit vào chính bộ chấm.

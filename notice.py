import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import ssl
import os


def send_email_report():
    EMAIL_SEND_COUNT = 0
    EMAIL_SEND_COUNT += 1
    print(f"[DEBUG] send_email_report TRIGGERED ({EMAIL_SEND_COUNT} times)")


    sender_email = "nguyenvanminh180299@gmail.com"
    receiver_emails = ["nguyenhaidang9xbk@gmail.com", "minhthuy06042021@gmail.com"]
    app_password = "omrf wgwg hngs cqam"  # mật khẩu ứng dụng


    # Kiểm tra file tồn tại và có nội dung
    if not os.path.exists("report.txt") or os.path.getsize("report.txt") == 0:
        print("⚠️ report.txt trống hoặc không tồn tại -> Không gửi email.")
        return

    # Đọc nội dung
    with open("report.txt", "r", encoding="utf-8") as f:
        report_content = f.read().strip()

    if not report_content:
        print("⚠️ report.txt không có nội dung -> Không gửi email.")
        return
    
    # Tạo email
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ", ".join(receiver_emails) 
    message["Subject"] = "Coin check"

    # Thêm vào thân email
    body = f"Hello,\n\nThis is the coin list we can long ASAP:\n\n{report_content}"
    message.attach(MIMEText(body, "plain", "utf-8"))

    try:
        # Sử dụng SSL port 465 cho gọn
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, app_password)
            server.sendmail(sender_email, receiver_emails, message.as_string())

        print(f"✅ Report sent successfully (inline content). Count={EMAIL_SEND_COUNT}")

    except Exception as e:
        print(f"❌ Error sending email: {e}")

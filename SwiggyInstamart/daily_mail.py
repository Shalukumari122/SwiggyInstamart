import smtplib
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import timedelta
import pandas as pd

today_date = str(date.today()).replace('-', '_')
yesterday_date = str(date.today() + timedelta(-1)).replace('-', '_')
# print(yesterday_date)

yesterday_folder_loc = f'C:/Palak/LiveProjects/SwiggyInstamart/data_files/{yesterday_date}/'

yesterday_file = pd.read_excel(yesterday_folder_loc + f'swiggy_instamart_{yesterday_date.replace('_', '')}.xlsx', sheet_name='data')

today_folder_loc = f'C:/Palak/LiveProjects/SwiggyInstamart/data_files/{today_date}/'

today_file = pd.read_excel(today_folder_loc + f'swiggy_instamart_{today_date.replace('_', '')}.xlsx', sheet_name='data')


# Email content
today_date = str(date.today().strftime('%d-%m-%Y'))
sender_email = "palakchauhan.actowiz@gmail.com"
receiver_email = "abhishekv.actowiz@gmail.com"
subject = f"Swiggy Instamart Daily Update - {today_date}"
body = f'''
Total field count in:

Yesterday's File: {len(yesterday_file.columns)}
Today's File: {len(today_file.columns)}'''

print('y:', len(yesterday_file.columns))
print('t:', len(today_file.columns))
# Create a multipart message
message = MIMEMultipart()
message["From"] = sender_email
message["To"] = receiver_email
message["Subject"] = subject

# Attach body to the email
message.attach(MIMEText(body, "plain"))
password = "kecv hqva tbhs leka"

agree = input('Send Mail:')

if agree == 'y':
    # Connect to the SMTP server
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()  # Secure the connection
        server.login(sender_email, password)
        text = message.as_string()
        server.sendmail(sender_email, receiver_email, text)

        print('Mail Sent')
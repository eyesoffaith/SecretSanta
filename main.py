# TODO: Encrypt outptut file so no one can view who got who

import re
import os
import sys
import glob
import random
import smtplib
from email.mime.text import MIMEText
from datetime import datetime

import polars as pl
import pprint as pp
from cryptography.fernet import Fernet

RETRIES = 100
TEST_RUN = True
TEST_EMAIL = "dmjunkjunk@gmail.com"
TEST_MODE = "test"
LIVE_MODE = "live"

with open("data/encryption.key", "rb") as key_file:
    ENCRYPTION_KEY = key_file.read()

with open("data/email_credentials") as file:
    content = file.read()
    GMAIL_USERNAME, GMAIL_APP_PASSWORD = content.split("\n")

df_contacts = pl.DataFrame()

def print_df(df: pl.DataFrame):
    print(''.join([f'{col:<60}' for col in df.columns]))
    for row in df.rows():
        line = ''.join([f'{"" if x is None else x:<60}' for x in row])
        print(line)
    print('')

def encrypt_dataframe_to_csv(df: pl.DataFrame, file_path: str):
    fernet = Fernet(ENCRYPTION_KEY)

    df.write_csv("data/output.csv")
    with open("data/output.csv", "rb") as file:
        data = file.read()
    os.remove("data/output.csv")

    encrypted_data = fernet.encrypt(data)
    with open(file_path, "wb") as file:
        file.write(encrypted_data)

def decrypt_csv_to_dataframe(file_path: str):
    fernet = Fernet(ENCRYPTION_KEY)
    with open(file_path, "rb") as file:
        encrypted_data = file.read()

    data = fernet.decrypt(encrypted_data)

    with open("data/output.csv", "wb") as file:
        file.write(data)
    df = pl.read_csv("data/output.csv")
    os.remove("data/output.csv")

    return df

def pair_contact_with_recipient(df_contacts: pl.DataFrame, df_exceptions: pl.DataFrame):
    contacts = sorted(df_contacts.to_dicts(), key=lambda contact: len([v for v in contact.values() if v is None]))
    contacts = {contact["email"]: contact for contact in contacts}
    emails = [k for k in contacts]
    
    listComplete = False
    retries = RETRIES

    while not listComplete:
        for email in contacts:
            contacts[email]["recipient_email"] = ""   
        email = emails[0]
        email_start = email
        recipients = emails.copy()
        while True:
            exceptions = df_exceptions.filter(pl.col("from_email") == email)["to_email"].to_list()
            exceptions += [contacts[email][k] for k in contacts[email] if re.match(r"previously_gave_to_\d", k)]
            exceptions += [email]
            exceptions = [email for email in exceptions if email is not None]

            _recipients = [recipient_email for recipient_email in recipients.copy() if recipient_email not in exceptions]
            if len(_recipients) == 0:
                listComplete = False
                break
            random.shuffle(_recipients)
            recipient_email = _recipients[0]
            contacts[email]["recipient_email"] = recipient_email
            recipients.remove(recipient_email)

            email = recipient_email

            if email == email_start:
                listComplete = len([k for k in contacts if contacts[k]["recipient_email"] == ""]) == 0
                break
        retries -= 1
        if retries == 0:
            break

    if retries == 0:
        print(f"Unable to find matching pairs after {RETRIES} attempts. Likely not possible")
        return pl.DataFrame()
    else:
        print(f"Success in {RETRIES - retries} attempts")
        df_recipients = pl.DataFrame({"sender_email": [email for email in contacts], "recipient_email": [contacts[email]["recipient_email"] for email in contacts]})
        return df_recipients.join(df_contacts.select(["email", "name"]), left_on="sender_email", right_on="email")\
            .join(df_contacts.select(["email", "name", "wishlist_link"]), left_on="recipient_email", right_on="email")\
            .rename({"name": "sender_name", "name_right": "recipient_name"})\
            .select(["sender_name", "sender_email", "recipient_name", "recipient_email", "wishlist_link"])

def send_emails(df_gift_pairs: pl.DataFrame):
    email_html = ""
    with open("data/email_template.html") as file:
        email_html = file.read()

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp_server:
        smtp_server.login(GMAIL_USERNAME, GMAIL_APP_PASSWORD)
        
        for row in df_gift_pairs.to_dicts():
            if TEST_RUN:
                recipient_email = "dmjunkjunk@gmail.com"
            else:
                recipient_email = row["sender_email"]
            
            msg_body = email_html
            msg_body = msg_body.replace("[[sender_name]]", row["sender_name"])
            msg_body = msg_body.replace("[[recipient_name]]", row["recipient_name"])
            msg_body = msg_body.replace("[[wishlist_link]]", row["wishlist_link"] or "https://www.amazon.com")

            msg = MIMEText(msg_body, "html")
            msg["Subject"] = "Secret Santa w/ Discord Friends!!!"
            msg["From"] = GMAIL_USERNAME
            msg["To"] = recipient_email

            smtp_server.sendmail(GMAIL_USERNAME, [recipient_email], msg.as_string())
            print(f"Message sent to {recipient_email}")

def main():
    contact_files = [[os.path.getmtime(file_path), file_path] for file_path in glob.glob("data/*_contacts.csv")]
    contact_file_path = max(contact_files)[1] # get filename of most recently updated file
    year = int(os.path.basename(contact_file_path).split("_")[0])
    
    df_contacts = pl.read_csv(contact_file_path).with_columns(pl.lit("").alias("recipient_email"))
    exception_file_path = f"data/{year}_exceptions.csv"
    
    df_exceptions = pl.DataFrame({"from_email":[], "to_email":[]})
    if os.path.exists(exception_file_path):
        df_exceptions = pl.read_csv(exception_file_path)

    df_gift_pairs = pair_contact_with_recipient(df_contacts, df_exceptions)
    if df_gift_pairs.is_empty():
        sys.exit()
    df_gift_pairs.write_csv("gift_pairs.csv")

    send_emails(df_gift_pairs)

    df_new_contacts = df_gift_pairs.join(df_contacts, left_on="sender_email", right_on="email")\
        .select(["name", "sender_email", "wishlist_link", "recipient_email", "previously_gave_to_1", "previously_gave_to_2"])\
        .rename({"sender_email": "email", "previously_gave_to_2": "previously_gave_to_3", "previously_gave_to_1": "previously_gave_to_2", "recipient_email": "previously_gave_to_1"})\
        
    df_new_contacts.write_csv(f"data/{year + 1}_contacts.csv")

if __name__ == "__main__":
    execution_mode = None
    if len(sys.argv) > 1:
        execution_mode = sys.argv[1]

    if execution_mode == TEST_MODE:
        TEST_RUN = True
    elif execution_mode == LIVE_MODE:
        TEST_RUN = False
    else:
        print(f"Unknown execution mode {execution_mode}. Valid modes are [{TEST_MODE},{LIVE_MODE}]")
        sys.exit()

    main()

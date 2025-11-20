# TODO: Accept a JSON/CSV file of additional exceptions that will be adhered to but won't be saved to next year's input file.
# TODO: Create a UI for easy use
# TODO: Encrypt outptut file so no one can view who got who

import re
import os
import sys
import glob
import random
import smtplib
from email.mime.text import MIMEText

import polars as pl
import pprint as pp

RETRIES = 100
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

def pair_contact_with_recipient(df_contacts: pl.DataFrame):
    contacts = sorted(df_contacts.to_dicts(), key=lambda contact: len([v for v in contact.values() if v is None]))
    contacts = {contact["email"]: contact for contact in contacts}
    emails = [k for k in contacts]
    
    listComplete = False
    retries = RETRIES
    while not listComplete:
        for email in contacts:
            contacts[email]["recipient_email"] = ""   
        email = emails[0]
        email_prev = None
        email_start = email
        recipients = emails.copy()
        while True:
            exemptions = [contacts[email][k] for k in contacts[email] if re.match(r"previously_gave_to_\d", k)] + [email, email_prev]
            exemptions = [email for email in exemptions if email is not None]

            _recipients = [recipient_email for recipient_email in recipients.copy() if recipient_email not in exemptions]
            if len(_recipients) == 0:
                listComplete = False
                break
            random.shuffle(_recipients)
            recipient_email = _recipients[0]
            contacts[email]["recipient_email"] = recipient_email
            recipients.remove(recipient_email)

            email_prev = email
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
            .join(df_contacts.select(["email", "name", "gift_list_link"]), left_on="recipient_email", right_on="email")\
            .rename({"name": "sender_name", "name_right": "recipient_name"})\
            .select(["sender_name", "sender_email", "recipient_name", "recipient_email", "gift_list_link"])

def send_emails(df_gift_pairs: pl.DataFrame):
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp_server:
        smtp_server.login(GMAIL_USERNAME, GMAIL_APP_PASSWORD)

        for row in df_gift_pairs.to_dicts():
            msg_body = f"""
Ho ho ho! And welcome {row["sender_name"]}!

You've gotten {row["recipient_name"]} as your special someone to shop for this year!

I've done some snooping and wrote down a little list of things that they'll like to give you a headstart!:
{row["gift_list_link"]}

Thanks for being santa's little helper and helping me out this year!
"""          
            # TODO: Switch to row["sender_email"] for actually live use
            recipient_email = "dmjunkjunk@gmail.com"
            # recipient_email = row["sender_email"]

            msg = MIMEText(msg_body)
            msg["Subject"] = "Test Subject"
            msg["From"] = GMAIL_USERNAME
            msg["To"] = recipient_email

            # smtp_server.sendmail(GMAIL_USERNAME, [recipient_email], msg.as_string())
            print(f"Message Sent to {recipient_email}")

def main():
    input_files = [[os.path.getmtime(file_path), file_path] for file_path in glob.glob("data/input_*.csv")]
    input_file_path = max(input_files)[1]

    df_contacts = pl.read_csv(input_file_path).with_columns(pl.lit("").alias("recipient_email"))
    df_gift_pairs = pair_contact_with_recipient(df_contacts)
    if df_gift_pairs.is_empty():
        sys.exit()
    df_gift_pairs.write_csv("gift_pairs.csv")

    send_emails(df_gift_pairs)

    counter = 1
    output_file_path = f"data/input_{counter}.csv"
    while os.path.exists(output_file_path):
        counter += 1
        output_file_path = f"data/input_{counter}.csv"
    
    df_new_contacts = df_gift_pairs.join(df_contacts, left_on="sender_email", right_on="email")\
        .select(["name", "sender_email", "gift_list_link", "recipient_email", "previously_gave_to_1", "previously_gave_to_2"])\
        .rename({"sender_email": "email", "previously_gave_to_2": "previously_gave_to_3", "previously_gave_to_1": "previously_gave_to_2", "recipient_email": "previously_gave_to_1"})\
        
    df_new_contacts.write_csv(output_file_path)
        
if __name__ == "__main__":
    main()

import re
import os
import sys
import glob
import random

import polars as pl
import pprint as pp

RETRIES = 1000

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
            contacts[email]["recipient"] = ""   
        email = emails[0]
        email_prev = None
        email_start = email
        recipients = emails.copy()
        while True:
            exemptions = [contacts[email][k] for k in contacts[email] if re.match(r"previously_gave_to_\d", k)] + [email, email_prev]
            exemptions = [email for email in exemptions if email is not None]

            _recipients = [recipient for recipient in recipients.copy() if recipient not in exemptions]
            if len(_recipients) == 0:
                listComplete = False
                break
            random.shuffle(_recipients)
            recipient = _recipients[0]
            contacts[email]["recipient"] = recipient
            recipients.remove(recipient)

            email_prev = email
            email = recipient

            if email == email_start:
                listComplete = len([k for k in contacts if contacts[k]["recipient"] == ""]) == 0
                break
        retries -= 1
        if retries == 0:
            break

    if retries == 0:
        print(f"Unable to find matching pairs after {RETRIES} attempts. Likely not possible")
        return pl.DataFrame()
    else:
        print(f"Success in {RETRIES - retries} tries")
        df_recipients = pl.DataFrame({"sender": [email for email in contacts], "recipient": [contacts[email]["recipient"] for email in contacts]})
        return df_recipients.join(df_contacts.select(["email", "name"]), left_on="sender", right_on="email")\
            .join(df_contacts.select(["email", "name", "gift_list_link"]), left_on="recipient", right_on="email")\
            .rename({"name": "sender_name", "name_right": "recipient_name"})\
            .select(["sender_name", "sender", "recipient_name", "recipient", "gift_list_link"])

def send_emails(df_gift_pairs: pl.DataFrame):
    pass

def main():
    input_files = [[os.path.getmtime(file_path), file_path] for file_path in glob.glob("data/input_*.csv")]
    input_file_path = max(input_files)[1]

    df_contacts = pl.read_csv(input_file_path).with_columns(pl.lit("").alias("recipient"))
    df_gift_pairs = pair_contact_with_recipient(df_contacts)
    if df_gift_pairs.is_empty():
        sys.exit()

    counter = 1
    output_file_path = f"data/input_{counter}.csv"
    while os.path.exists(output_file_path):
        counter += 1
        output_file_path = f"data/input_{counter}.csv"
    
    df_new_contacts = df_gift_pairs.join(df_contacts, left_on="sender", right_on="email")\
        .select(["name", "sender", "gift_list_link", "recipient", "previously_gave_to_1", "previously_gave_to_2"])\
        .rename({"sender": "email", "previously_gave_to_2": "previously_gave_to_3", "previously_gave_to_1": "previously_gave_to_2", "recipient": "previously_gave_to_1"})\
        
    df_new_contacts.write_csv(output_file_path)
        
if __name__ == "__main__":
    main()

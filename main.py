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

import tkinter
import tkinter.messagebox
import customtkinter

customtkinter.set_appearance_mode("Dark")  # Modes: "System" (standard), "Dark", "Light"
customtkinter.set_default_color_theme("blue")  # Themes: "blue" (standard), "green", "dark-blue"

RETRIES = 100
with open("email_credentials") as file:
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
    email_html = ""
    with open("email_template.html") as file:
        email_html = file.read()

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp_server:
        smtp_server.login(GMAIL_USERNAME, GMAIL_APP_PASSWORD)
        
        for row in df_gift_pairs.to_dicts():
            # TODO: Switch to row["sender_email"] for actually live use
            recipient_email = "dmjunkjunk@gmail.com"
            # recipient_email = row["sender_email"]

            msg_body = email_html
            msg_body = msg_body.replace("[[sender_name]]", row["sender_name"])
            msg_body = msg_body.replace("[[recipient_name]]", row["recipient_name"])
            msg_body = msg_body.replace("[[gift_list_link]]", row["gift_list_link"] or "https://www.amazon.com")

            msg = MIMEText(msg_body, "html")
            msg["Subject"] = "Secret Santa w/ Discord Friends!!!"
            msg["From"] = GMAIL_USERNAME
            msg["To"] = recipient_email

            smtp_server.sendmail(GMAIL_USERNAME, [recipient_email], msg.as_string())
            print(f"Message Sent to {recipient_email}")

            # TODO: Remove break for live use
            break

class App(customtkinter.CTk):
    def __init__(self):
        super().__init__()

        # configure window
        self.title("CustomTkinter complex_example.py")
        self.geometry(f"{1100}x{580}")

        # configure grid layout (4x4)
        self.grid_columnconfigure(1, weight=1)
        self.grid_columnconfigure((2, 3), weight=0)
        self.grid_rowconfigure((0, 1, 2), weight=1)

        # create sidebar frame with widgets
        self.sidebar_frame = customtkinter.CTkFrame(self, width=140, corner_radius=0)
        self.sidebar_frame.grid(row=0, column=0, rowspan=4, sticky="nsew")
        self.sidebar_frame.grid_rowconfigure(4, weight=1)

        # create main entry and button
        self.entry = customtkinter.CTkEntry(self, placeholder_text="CTkEntry")
        self.entry.grid(row=3, column=1, columnspan=2, padx=(20, 0), pady=(20, 20), sticky="nsew")

        self.main_button_1 = customtkinter.CTkButton(master=self, fg_color="transparent", border_width=2, text_color=("gray10", "#DCE4EE"))
        self.main_button_1.grid(row=3, column=3, padx=(20, 20), pady=(20, 20), sticky="nsew")

        # create textbox
        self.textbox = customtkinter.CTkTextbox(self, width=250)
        self.textbox.grid(row=0, column=1, padx=(20, 0), pady=(20, 0), sticky="nsew")

        # create tabview
        self.tabview = customtkinter.CTkTabview(self, width=250)
        self.tabview.grid(row=0, column=2, padx=(20, 0), pady=(20, 0), sticky="nsew")
        self.tabview.add("CTkTabview")
        self.tabview.add("Tab 2")
        self.tabview.add("Tab 3")
        self.tabview.tab("CTkTabview").grid_columnconfigure(0, weight=1)  # configure grid of individual tabs
        self.tabview.tab("Tab 2").grid_columnconfigure(0, weight=1)

        self.optionmenu_1 = customtkinter.CTkOptionMenu(self.tabview.tab("CTkTabview"), dynamic_resizing=False,
                                                        values=["Value 1", "Value 2", "Value Long Long Long"])
        self.optionmenu_1.grid(row=0, column=0, padx=20, pady=(20, 10))
        self.combobox_1 = customtkinter.CTkComboBox(self.tabview.tab("CTkTabview"),
                                                    values=["Value 1", "Value 2", "Value Long....."])
        self.combobox_1.grid(row=1, column=0, padx=20, pady=(10, 10))
        self.string_input_button = customtkinter.CTkButton(self.tabview.tab("CTkTabview"), text="Open CTkInputDialog",
                                                           command=self.open_input_dialog_event)
        self.string_input_button.grid(row=2, column=0, padx=20, pady=(10, 10))
        self.label_tab_2 = customtkinter.CTkLabel(self.tabview.tab("Tab 2"), text="CTkLabel on Tab 2")
        self.label_tab_2.grid(row=0, column=0, padx=20, pady=20)

    def open_input_dialog_event(self):
        dialog = customtkinter.CTkInputDialog(text="Type in a number:", title="CTkInputDialog")
        print("CTkInputDialog:", dialog.get_input())

    def change_appearance_mode_event(self, new_appearance_mode: str):
        customtkinter.set_appearance_mode(new_appearance_mode)

    def change_scaling_event(self, new_scaling: str):
        new_scaling_float = int(new_scaling.replace("%", "")) / 100
        customtkinter.set_widget_scaling(new_scaling_float)

    def sidebar_button_event(self):
        print("sidebar_button click")

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
    app = App()
    app.mainloop()
    main()

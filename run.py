import imaplib
import email
from email.header import decode_header
import configparser
import mysql.connector
import sys
import pandas as pd


def setup_db():
    config = configparser.ConfigParser()
    config.read("config.ini")
    mysql_config = {
        "host": config.get("MYSQL", "HOST"),
        "port": config.getint("MYSQL", "PORT"),
        "user": config.get("MYSQL", "USER"),
        "password": config.get("MYSQL", "PASSWORD"),
        "database": config.get("MYSQL", "DATABASE"),
    }
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()
    cursor.execute(
        """
                   CREATE TABLE IF NOT EXISTS email_data
                   (uid VARCHAR(255) PRIMARY KEY,
                   sender TEXT,
                   subject TEXT,
                   date TEXT)
                   """
    )
    conn.commit()
    return conn, cursor


def fetch_emails(mail, start_date, end_date):
    conn, cursor = setup_db()
    result, data = mail.search(None, f'(SINCE "{start_date}" BEFORE "{end_date}")')
    uids = data[0].split()
    for uid in uids:
        result, data = mail.fetch(uid, "(RFC822)")
        raw_email = data[0][1]
        msg = email.message_from_bytes(raw_email)
        sender = decode_header(msg["From"])[0][0]
        subject = msg["Subject"]
        if subject is not None:
            subject = decode_header(subject)[0][0]
        date = msg["Date"]
        cursor.execute(
            "INSERT IGNORE INTO email_data (uid, sender, subject, date) VALUES (%s, %s, %s, %s)",
            (uid, sender, subject, date),
        )
        conn.commit()
    conn.close()


def top_senders():
    conn, cursor = setup_db()
    cursor.execute(
        "SELECT sender, COUNT(*) FROM email_data GROUP BY sender ORDER BY COUNT(*) DESC LIMIT 100"
    )
    rows = cursor.fetchall()
    for row in rows:
        print(f"{row[0]}: {row[1]} emails")
    conn.close()


def top_senders_to_excel(filename):
    conn, cursor = setup_db()
    cursor.execute(
        "SELECT sender, COUNT(*) as count FROM email_data GROUP BY sender ORDER BY count DESC LIMIT 100"
    )
    rows = cursor.fetchall()
    conn.close()

    # Convert the rows to a pandas DataFrame
    df = pd.DataFrame(rows, columns=["Sender", "Email Count"])

    # Write the DataFrame to an Excel file
    df.to_excel(filename, index=False)
    print(f"Data exported to {filename}")


def main():
    config = configparser.ConfigParser()
    config.read("config.ini")
    user = config.get("DEFAULT", "GMAIL_USER")
    password = config.get("DEFAULT", "GMAIL_PASSWORD")

    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(user, password)
    mail.select("inbox")

    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "fetch":
            fetch_emails(mail, "01-Jan-2005", "31-Dec-2023")
        elif command == "top":
            top_senders()
        elif command == "to_excel":
            filename = sys.argv[2]
            top_senders_to_excel(filename)
    else:
        print("Usage: python script.py <command>")
        print("Commands: fetch, top, to_excel")


if __name__ == "__main__":
    main()

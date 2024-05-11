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
    num_emails_processed = 0
    total_ingested_data = 0

    for index, uid in enumerate(uids, start=1):
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
        num_emails_processed += 1
        total_ingested_data += len(raw_email)

        if num_emails_processed % 25 == 0:
            print(f"{num_emails_processed} emails processed")
            sys.stdout.flush()  # Flush the output buffer

    # Print final progress
    print(f"All emails processed! Count: {num_emails_processed}")
    sys.stdout.flush()  # Flush the output buffer

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


def frequent_daily_senders():
    conn, cursor = setup_db()

    # Fetch the senders who have emailed more than once in the same day.
    cursor.execute(
        """
        SELECT sender, DATE(date) as email_date
        FROM email_data 
        GROUP BY sender, DATE(date)
        HAVING COUNT(*) > 1
        """
    )
    daily_counts = cursor.fetchall()

    # Aggregate this data to get the number of "offenses" for each sender
    offense_counts = {}
    for sender, _ in daily_counts:
        offense_counts[sender] = offense_counts.get(sender, 0) + 1

    # Sort the senders by their number of "offenses"
    sorted_senders = sorted(offense_counts.items(), key=lambda x: x[1], reverse=True)

    for sender, offenses in sorted_senders:
        print(f"{sender}: {offenses} offenses (emailed more than once in a single day)")

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
            start = config.get("FETCH", "START")
            end = config.get("FETCH", "END")
            fetch_emails(mail, start, end)
        elif command == "top":
            top_senders()
        elif command == "to_excel":
            filename = sys.argv[2]
            top_senders_to_excel(filename)
        elif command == "more_than_once":
            frequent_daily_senders()
    else:
        print("Usage: python script.py <command>")
        print("Commands: fetch, top, to_excel, more_than_once")


if __name__ == "__main__":
    main()

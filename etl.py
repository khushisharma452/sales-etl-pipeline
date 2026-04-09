import pandas as pd
import mysql.connector

def extract():
    return pd.read_csv('data/sales_data.csv')

def transform(df):
    df = df.dropna()
    df['date'] = pd.to_datetime(df['date'])
    return df

def load(df):
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="your_password",
        database="sales_db"
    )

    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales (
        order_id INT,
        product VARCHAR(50),
        amount FLOAT,
        date DATE
    )
    """)

    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO sales VALUES (%s,%s,%s,%s)",
            tuple(row)
        )

    conn.commit()
    conn.close()

def main():
    df = extract()
    df = transform(df)
    load(df)
    print("Done!")

if __name__ == "__main__":
    main()
import yfinance as yf
import psycopg2


# 종목 코드 리스트
TICKERS = [{
    'code': 'TSLA',
    'labelKo': '테슬라',
    'labelEn': 'Tesla'
}, {
    'code': 'GOOG',
    'labelKo': '구글',
    'labelEn': 'Google'
    
}, {
    'code': 'AAPL',
    'labelKo': '애플',
    'labelEn': 'Apple'
}, {
    'code': 'MSFT',
    'labelKo': '마이크로소프트',
    'labelEn': 'Microsoft'
}]
COL_NAMES_HIST = ['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'user': 'choeseongpil',
    'password': '0525',
    'database': 'stock',
}
# 날짜 범위 설정
start_date = "2023-01-01"
end_date = "2024-01-01"

conn = psycopg2.connect(
    host=DB_CONFIG["host"],
    port=DB_CONFIG["port"],
    user=DB_CONFIG["user"],
    password=DB_CONFIG["password"],
    database=DB_CONFIG["database"],
)
cur = conn.cursor()
data = {}
for ticker in TICKERS:
    print(f"Processing {ticker}...")
    # hist = stock.history(period="max")
    stock = yf.Ticker(ticker['code'])
    df = stock.history(start=start_date, end=end_date)

    # 테이블 생성 (없는 경우)
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS stock_prices (
            date DATE,
            ticker VARCHAR(10),
            labelKo VARCHAR(20),
            labelEn VARCHAR(20),
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            CONSTRAINT unique_key PRIMARY KEY (date, ticker)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_stock_prices_date_ticker ON stock_prices (date, ticker)
        """
    )

    # 데이터 삽입
    for i in range(len(df)):
        cur.execute(
            """
            INSERT INTO stock_prices (date, ticker, labelKo, labelEn, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                df.index[i].strftime("%Y-%m-%d"),
                ticker['code'],
                ticker['labelKo'],
                ticker['labelEn'],
                df["Open"][i].astype(float),
                df["High"][i].astype(float),
                df["Low"][i].astype(float),
                df["Close"][i].astype(float),
                df["Volume"][i].astype(float),
            ),
        )

# 변경 사항 저장
conn.commit()
cur.close()
conn.close()


    
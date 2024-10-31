import mysql.connector
from mysql.connector import Error
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pandas as pd
import nltk
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
import yfinance as yf
import time
from typing import Dict, List, Optional, Tuple

# AAPL/AMZN/GOOGL/NFLX
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_analyzer.log'),
        logging.StreamHandler()
    ]
)

load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.connect()

    def connect(self):
        """Establish database connection with retry mechanism"""
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                self.connection = mysql.connector.connect(
                    host=os.getenv('DB_HOST', 'localhost'),
                    user=os.getenv('DB_USER', 'root'),
                    password=os.getenv('DB_PASS', 'Kingpin28256'),
                    database='StockAnalytics',
                    auth_plugin='mysql_native_password'
                )
                if self.connection.is_connected():
                    logging.info("Successfully connected to MySQL database")
                    return
            except Error as e:
                logging.error(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception("Failed to connect to database after maximum retries")

    def save_stock_details(self, df: pd.DataFrame):
        """Save stock details to StockDetails table"""
        if self.connection is None or not self.connection.is_connected():
            self.connect()

        try:
            cursor = self.connection.cursor()
            for _, row in df.iterrows():
                query = """
                    INSERT INTO StockDetails 
                    (ticker, date, open_price, close_price, volume, percentage_change, profit_loss)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                percentage_change = ((row['Close'] - row['Open']) / row['Open']) * 100
                profit_loss = 'profit' if row['Close'] > row['Open'] else 'loss'
                
                values = (
                    row['ticker'],
                    row['date'],
                    row['Open'],
                    row['Close'],
                    row['Volume'],
                    round(percentage_change, 2),
                    profit_loss
                )
                
                cursor.execute(query, values)
            
            self.connection.commit()
            logging.info(f"Successfully saved stock details for {row['ticker']}")
        except Error as e:
            logging.error(f"Error saving stock details: {str(e)}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def update_profit_statistics(self):
        """Update profit statistics for all stocks"""
        if self.connection is None or not self.connection.is_connected():
            self.connect()

        try:
            cursor = self.connection.cursor()
            
        
            query = """
                INSERT INTO ProfitStatistics 
                (ticker, total_days, profit_days, loss_days, profit_probability, last_calculated)
                SELECT ticker, 
                       COUNT(*) AS total_days,
                       SUM(CASE WHEN profit_loss = 'profit' THEN 1 ELSE 0 END) AS profit_days,
                       SUM(CASE WHEN profit_loss = 'loss' THEN 1 ELSE 0 END) AS loss_days,
                       (SUM(CASE WHEN profit_loss = 'profit' THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS profit_probability,
                       CURDATE() AS last_calculated
                FROM StockDetails
                GROUP BY ticker
                ON DUPLICATE KEY UPDATE 
                    total_days = VALUES(total_days),
                    profit_days = VALUES(profit_days),
                    loss_days = VALUES(loss_days),
                    profit_probability = VALUES(profit_probability),
                    last_calculated = VALUES(last_calculated)
            """
            
            cursor.execute(query)
            self.connection.commit()
            logging.info("Successfully updated profit statistics")
        except Error as e:
            logging.error(f"Error updating profit statistics: {str(e)}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()

class StockAnalyzer:
    def __init__(self):
        self.db_manager = DatabaseManager()
        
    def get_stock_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Fetch stock data using yfinance"""
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period="1d", interval="1m")
            if not df.empty:
                df['ticker'] = ticker
                df = df.reset_index()
                df['date'] = df['Datetime'].dt.date
                df['time'] = df['Datetime'].dt.time
                return df
            return None
        except Exception as e:
            logging.error(f"Error fetching data for {ticker}: {e}")
            return None

    def analyze_stocks(self, tickers: List[str]) -> Tuple[bool, str]:
        """Main analysis function"""
        try:
            for ticker in tickers:
                df = self.get_stock_data(ticker)
                if df is not None:
                    self.db_manager.save_stock_details(df)
            
            
            self.db_manager.update_profit_statistics()
            
            return True, "Analysis completed successfully"
        except Exception as e:
            error_msg = f"Analysis failed: {str(e)}"
            logging.error(error_msg)
            return False, error_msg

def display_stock_stats():
    """Display current stock statistics"""
    try:
        db_manager = DatabaseManager()
        cursor = db_manager.connection.cursor(dictionary=True)
        
       
        cursor.execute("""
            SELECT * FROM ProfitStatistics 
            ORDER BY profit_probability DESC
        """)
        
        stats = cursor.fetchall()
        print("\nStock Performance Statistics:")
        print("=" * 80)
        print(f"{'Ticker':<10} {'Total Days':<12} {'Profit Days':<12} {'Loss Days':<12} {'Profit Probability':<15}")
        print("-" * 80)
        
        for stat in stats:
            print(f"{stat['ticker']:<10} {stat['total_days']:<12} {stat['profit_days']:<12} "
                  f"{stat['loss_days']:<12} {stat['profit_probability']:.2f}%")
        
        cursor.close()
    except Error as e:
        logging.error(f"Error displaying statistics: {e}")

if __name__ == "__main__":
   
    db_manager = DatabaseManager()
    cursor = db_manager.connection.cursor()
    cursor.execute("SELECT DISTINCT ticker FROM StockDetails")
    available_stocks = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    print("Available stocks:", ", ".join(available_stocks))
    user_input = input("\nEnter stock tickers to analyze (comma-separated) or 'all' for all stocks: ")
    
    if user_input.lower().strip() == 'all':
        tickers = available_stocks
    else:
        tickers = [ticker.strip().upper() for ticker in user_input.split(',')]
    
    analyzer = StockAnalyzer()
    success, message = analyzer.analyze_stocks(tickers)
    print(message)
    
    if success:
        display_stock_stats()
import mysql.connector
from mysql.connector import Error

def create_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',  # or 'retail-mysql' if running inside another Docker container
            user='retail_user',
            password='retailpass',
            database='retail_dw',
            port=3306
        )
        if connection.is_connected():
            print("Connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error: {e}")
        return None

def close_connection(connection):
    if connection and connection.is_connected():
        connection.close()
        print("MySQL connection closed")

if __name__ == "__main__":
    conn = create_connection()
    if conn:
        close_connection(conn)
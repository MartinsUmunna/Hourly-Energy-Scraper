import pymysql

try:
    db_connection = pymysql.connect(
        host="148.251.246.72",
        port=3306,
        database="jksutauf_nesidb",
        user="jksutauf_martins",
        password="12345678"
    )
    print("Database connection successful!")
    db_connection.close()
except Exception as e:
    print(f"Database connection failed: {e}")

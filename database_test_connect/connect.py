import psycopg2
from config import load_config


def connect(config):
    """ Connect to the PostgreSQL database server and return the connection object. """
    conn = None
    try:
        # connecting to the PostgreSQL server
        conn = psycopg2.connect(**config)
        print('Connected to the PostgreSQL server.')
        # *** Trả về đối tượng kết nối đang MỞ ***
        return conn
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error connecting to the database: {error}")
        # Quan trọng: Nếu kết nối thất bại, 'conn' là None hoặc đã xảy ra lỗi

        # Trả về None nếu kết nối thất bại
        return None 


if __name__ == '__main__':
    # --- Ví dụ cách sử dụng (và quản lý đóng kết nối) ---
    config = load_config()
    db_connection = connect(config)
    
    
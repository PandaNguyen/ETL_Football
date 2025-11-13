"""Load processed dimension CSVs into Postgres tables.

Usage (PowerShell):
    python .\scr\load_dim_tables.py         # load without truncation
    python .\scr\load_dim_tables.py --truncate  # truncate target tables before loading

The script expects a `database.ini` file located in the `database` folder (same location as `database/config.py`).
"""
import os
import argparse
import psycopg2
from database.config import load_config


def copy_csv_to_table(conn, csv_path, table_name, columns=None, truncate_first=False):
    """Copy CSV file into Postgres table using COPY FROM STDIN.

    - conn: psycopg2 connection
    - csv_path: path to CSV file
    - table_name: target table name (optionally schema.table)
    - columns: optional list of column names to pass to COPY
    - truncate_first: if True, truncates the table before copying
    """
    cur = conn.cursor()
    try:
        if truncate_first:
            cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
            print(f"Truncated {table_name}")

        cols_sql = ''
        if columns:
            cols_sql = '(' + ','.join(columns) + ')'

        sql = f"COPY {table_name} {cols_sql} FROM STDIN WITH CSV HEADER DELIMITER ','"
        with open(csv_path, 'r', encoding='utf-8') as f:
            cur.copy_expert(sql, f)
        conn.commit()
        print(f"Loaded '{csv_path}' into {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"Error loading {csv_path} into {table_name}: {e}")
        raise
    finally:
        cur.close()


def main(truncate=False):
    # Determine paths relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(script_dir, '..'))

    dim_team_csv = os.path.join(repo_root, 'data_processed', 'dim_team.csv')
    dim_stadium_csv = os.path.join(repo_root, 'data_processed', 'dim_stadium.csv')

    for p in (dim_team_csv, dim_stadium_csv):
        if not os.path.exists(p):
            raise FileNotFoundError(f"Required CSV not found: {p}")

    # Load DB config from the `database` directory
    config = load_config(filename='database.ini', section='postgresql')

    conn = None
    try:
        conn = psycopg2.connect(**config)
        print("Connected to database")

        # Table names - adjust schema/table if your tables are different
        team_table = 'public.dim_team'
        stadium_table = 'public.dim_stadium'

        # If CSVs contain the same column names as the table, we can omit columns list.
        # Optionally, specify column lists if needed to map columns precisely.
        copy_csv_to_table(conn, dim_team_csv, team_table, columns=None, truncate_first=truncate)
        copy_csv_to_table(conn, dim_stadium_csv, stadium_table, columns=None, truncate_first=truncate)

        print('All done.')
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load dim CSVs into Postgres')
    parser.add_argument('--truncate', action='store_true', help='Truncate target tables before loading')
    args = parser.parse_args()
    main(truncate=args.truncate)

from flask import Flask, request, jsonify
import sqlite3
from datetime import datetime
import os
import shutil
import csv

app = Flask(__name__)

def backup_database():
    now = datetime.now()
    backup_dir = f"D:/Backup/{now.year}/{now.month}/{now.day}/{now.hour}"
    os.makedirs(backup_dir, exist_ok=True)
    backup_path = os.path.join(backup_dir, 'backup.db')
    shutil.copy('inventory_1.db', backup_path)


def log_query(sql_query, status):
    now = datetime.now()
    log_dir = f"D:/Backup/{now.year}/{now.month}/{now.day}"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, 'log.csv')

    cleaned_query = ' '.join(sql_query.splitlines())

    with open(log_path, mode='a', newline='') as log_file:
        fieldnames = ['timestamp', 'query', 'status']
        log_writer = csv.DictWriter(log_file, fieldnames=fieldnames)

        if os.stat(log_path).st_size == 0:
            log_writer.writeheader()

        log_writer.writerow({'timestamp': now.strftime("%Y-%m-%d-%H:%M:%S"), 'query': cleaned_query, 'status': status})

def get_db_connection():
    conn = sqlite3.connect('inventory_1.db')
    conn.row_factory = sqlite3.Row
    return conn


@app.route('/execute_sql', methods=['POST'])
def execute_sql():
    status = False
    sql_query = request.json.get('query')  # Nhận lệnh SQL từ yêu cầu POST
    if not sql_query:
        return jsonify({'error': 'No SQL query provided'}), 400

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(sql_query)
        conn.commit()
        if sql_query.strip().lower().startswith('select'):
            results = cur.fetchall()
            results_list = [dict(row) for row in results]
            status = True
            return jsonify(results_list)
        else:
            status = True
            return jsonify({'message': 'Query executed successfully'})
    except sqlite3.Error as e:
        status = False
        return jsonify({'error': str(e)}), 500
    finally:
        if not sql_query.strip().lower().startswith('select'):
            backup_database()
        log_query(sql_query, status)
        conn.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0")

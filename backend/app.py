from flask import Flask, jsonify
from flask_cors import CORS
import psycopg2

app = Flask(__name__)

# Enable CORS for all domains on all routes
CORS(app)

# Define a route to fetch average prices data
@app.route('/api/average-prices')
def get_average_prices():
    try:
        # Connect to your PostgreSQL database
        conn = psycopg2.connect(
            host='postgres',  # Docker service name
            database='airflow',
            user='airflow',
            password='airflow'
        )

        # Execute the SQL query
        with conn.cursor() as cur:
            cur.execute('SELECT date_pulled, avg(price) FROM airbnb_data GROUP BY date_pulled ORDER BY date_pulled;')
            result = cur.fetchall()

        # Close the database connection
        conn.close()

        # Convert the result to a JSON response
        response = [{'date_pulled': row[0], 'avg_price': row[1]} for row in result]
        return jsonify(response)

    except Exception as e:
        return str(e), 500
# Define a route to fetch average prices data
@app.route('/api/avg-price-ratings')
def get_average_price_ratings():
    try:
        # Connect to your PostgreSQL database
        conn = psycopg2.connect(
            host='postgres',  # Docker service name
            database='airflow',
            user='airflow',
            password='airflow'
        )

        # Execute the SQL query
        with conn.cursor() as cur:
            cur.execute("SELECT date_pulled, round(avg(price)) as avg_price, round(avg(CASE WHEN rating != 'Nan' THEN rating ELSE NULL END),2) as avg_rating FROM airbnb_data  GROUP BY date_pulled ORDER BY date_pulled;")
            result = cur.fetchall()

        # Close the database connection
        conn.close()

        # Convert the result to a JSON response
        response = [{'date_pulled': row[0], 'avg_price': row[1], 'avg_rating': row[2]} for row in result]
        return jsonify(response)

    except Exception as e:
        return str(e), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

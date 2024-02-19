import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import requests
from datetime import date
import logging
from airflow.models import Variable
rapidapi_key = Variable.get("rapidapi_key")


@dag(
    dag_id="process-airbnb",
    schedule_interval="0 0 * * *",  # Runs at 00:00 (midnight) every day
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessAirbnb():
    create_airbnb_table = PostgresOperator(
        task_id="create_airbnb_table",
        postgres_conn_id="new_pg_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS airbnb_data (
                "id" TEXT,
                "price" NUMERIC,
                "deeplink" TEXT,
                "rating" NUMERIC,
                "type" TEXT,
                "reviews_count" NUMERIC,
                "is_superhost" BOOLEAN,
                "rarefind" BOOLEAN,
                "city" TEXT,
                "first_image" TEXT,
                "date_pulled" DATE,
                PRIMARY KEY ("id", "date_pulled")
            );
        """,
    )

    @task
    def get_airbnb_data():
        def make_airbnb_api_request():
            url = "https://airbnb13.p.rapidapi.com/search-location"
            headers = {
                "X-RapidAPI-Key": rapidapi_key,
                "X-RapidAPI-Host": "airbnb13.p.rapidapi.com"
            }
            query_params = {
                "location": "Costa Rica",
                "checkin": "2024-09-01",
                "checkout": "2024-09-30",
                "adults": "2",
                "children": "0",
                "infants": "0",
                "pets": "0",
                "page": "1",
                "currency": "USD"
            }
            try:
                response = requests.get(url, headers=headers, params=query_params)
                response.raise_for_status()
                return response.json().get('results', [])
            except requests.exceptions.RequestException as e:
                logging.error(f"Error making Airbnb API request: {e}")
                return []

        def parse_airbnb_data(results):
            ids = []
            prices = []
            deeplinks = []
            ratings = []
            types = []
            reviews_counts = []
            is_superhosts = []
            rarefinds = []
            cities = []
            first_images = []
            date_pulled = [date.today()] * len(results)

            for result in results:
                ids.append(result.get('id', ''))
                prices.append(result.get('price', {}).get('rate', None))
                deeplinks.append(result.get('deeplink', ''))
                ratings.append(result.get('rating', None))
                types.append(result.get('type', ''))
                reviews_counts.append(result.get('reviewsCount', None))
                is_superhosts.append(result.get('isSuperhost', False))
                rarefinds.append(result.get('rareFind', False))
                cities.append(result.get('city', ''))
                first_images.append(result.get('images', [])[0] if result.get('images') else '')

            return pd.DataFrame({
                "id": ids,
                "price": prices,
                "deeplink": deeplinks,
                "rating": ratings,
                "type": types,
                "reviews_count": reviews_counts,
                "is_superhost": is_superhosts,
                "rarefind": rarefinds,
                "city": cities,
                "first_image": first_images,
                "date_pulled": date_pulled
            })

        def write_to_temp_table(df):
            try:
                postgres_hook = PostgresHook(postgres_conn_id="new_pg_conn")
                conn = postgres_hook.get_conn()
                cur = conn.cursor()
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS airbnb_temp (
                        "id" TEXT,
                        "price" NUMERIC,
                        "deeplink" TEXT,
                        "rating" NUMERIC,
                        "type" TEXT,
                        "reviews_count" NUMERIC,
                        "is_superhost" BOOLEAN,
                        "rarefind" BOOLEAN,
                        "city" TEXT,
                        "first_image" TEXT,
                        "date_pulled" DATE
                    );
                """)
                cur.execute("TRUNCATE TABLE airbnb_temp;")
                for _, row in df.iterrows():
                    cur.execute("""
                        INSERT INTO airbnb_temp 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, tuple(row))
                conn.commit()
                logging.info("Data written to temporary table.")
            except Exception as e:
                logging.error(f"Error writing data to temporary table: {e}")
                raise

        results = make_airbnb_api_request()
        if results:
            df = parse_airbnb_data(results)
            write_to_temp_table(df)
        else:
            logging.warning("No data retrieved from Airbnb API.")

    @task
    def merge_airbnb_data():
        query = """
            INSERT INTO airbnb_data (id, price, deeplink, rating, type, reviews_count, is_superhost, rarefind, city, first_image, date_pulled)
            SELECT id, price, deeplink, rating, type, reviews_count, is_superhost, rarefind, city, first_image, date_pulled
            FROM airbnb_temp
            ON CONFLICT (id, date_pulled) DO NOTHING;
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="new_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            logging.info("New data merged into airbnb_data table.")
        except Exception as e:
            logging.error(f"Error merging data into airbnb_data table: {e}")
            raise

    get_data_task = get_airbnb_data()
    merge_data_task = merge_airbnb_data()

    create_airbnb_table >> get_data_task >> merge_data_task

dag = ProcessAirbnb()

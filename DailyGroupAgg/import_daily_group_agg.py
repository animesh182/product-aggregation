import psycopg2.extras
from params import params
import uuid

def load_data(aggregated_data):
    conn_str = f"dbname={params['dbname']} user={params['user']} password={params['password']} host={params['host']} port={params['port']}"
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            insert_statement = """
                INSERT INTO public."DailyGroupAggregation" (id, gastronomic_day, company, restaurant, article_supergroup, article_group, total_net, qty)
                VALUES %s
                ON CONFLICT (gastronomic_day, company, restaurant, article_supergroup, article_group)
                DO UPDATE SET
                    total_net = EXCLUDED.total_net,
                    qty = EXCLUDED.qty
            """
            psycopg2.extras.execute_values(
                cur, 
                insert_statement, 
                [(str(uuid.uuid4()),) + row for row in aggregated_data], 
                template='(%s, %s, %s, %s, %s, %s, %s, %s)', 
                page_size=100
            )
            conn.commit()
import psycopg2
from params import params

def extract_data(start_date, end_date):
    conn_str = f"dbname={params['dbname']} user={params['user']} password={params['password']} host={params['host']} port={params['port']}"
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                WITH FilteredData AS (
                    SELECT
                        gastronomic_day,
                        company,
                        restaurant,
                        article_supergroup,
                        article_group,
                        total_net,
                        qty
                    FROM
                        public."SalesData"
                    WHERE
                        article_group IS NOT NULL AND
                        article_supergroup IS NOT NULL AND
                        gastronomic_day >= %s AND
                        gastronomic_day <= %s
                ),
                AggregatedData AS (
                    SELECT
                        gastronomic_day,
                        company,
                        restaurant,
                        article_supergroup,
                        article_group,
                        SUM(qty) AS qty,
                        SUM(total_net) AS total_net
                    FROM
                        FilteredData
                    GROUP BY
                        1,2,3,4,5
                )
                SELECT
                    gastronomic_day,
                    company,
                    restaurant,
                    article_supergroup,
                    article_group,
                    total_net,
                    qty
                FROM
                    AggregatedData;
            """, (start_date, end_date))
            return cur.fetchall()

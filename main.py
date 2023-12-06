from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_caching import Cache
import snowflake.connector
from snowflake.connector import DictCursor
import os

REDIS_LINK = os.environ['REDIS']
SNOWFLAKE_USER = os.environ['SNOWFLAKE_USER']
SNOWFLAKE_PASS = os.environ['SNOWFLAKE_PASS']
SNOWFLAKE_ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
SNOWFLAKE_WAREHOUSE = os.environ['SNOWFLAKE_WAREHOUSE']

config = {
  "CACHE_TYPE": "redis",
  "CACHE_DEFAULT_TIMEOUT": 3600,
  "CACHE_REDIS_URL": REDIS_LINK
}

app = Flask(__name__)
app.config.from_mapping(config)
cache = Cache(app)
CORS(app)


def make_cache_key(*args, **kwargs):
  path = request.path
  args = str(hash(frozenset(request.args.items())))
  return (path + args).encode('utf-8')


def execute_sql(sql_string, **kwargs):
  conn = snowflake.connector.connect(user=SNOWFLAKE_USER,
                                     password=SNOWFLAKE_PASS,
                                     account=SNOWFLAKE_ACCOUNT,
                                     warehouse=SNOWFLAKE_WAREHOUSE,
                                     database="BUNDLEBEAR",
                                     schema="ERC4337")

  sql = sql_string.format(**kwargs)
  res = conn.cursor(DictCursor).execute(sql)
  results = res.fetchall()
  conn.close()
  return results


@app.route('/users')
@cache.memoize(make_name=make_cache_key)
def users():
  filter = request.args.get('filter', 'all')
  timeframe = request.args.get('timeframe', 'week')

  if filter == 'all':
    actives_24h = execute_sql('''
    SELECT COUNT(DISTINCT FROM_ADDRESS) as active_wallets
    FROM SCROLL.RAW.TRANSACTIONS
    WHERE BLOCK_TIMESTAMP >= current_timestamp - interval '1 day' 
    ''')

    actives_growth_24h = execute_sql('''
    WITH active_wallet_counts AS (
        SELECT
            COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP >= current_timestamp() - interval '1 day' THEN FROM_ADDRESS END) as past_day_wallets,
            COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP < current_timestamp() - interval '1 day' AND BLOCK_TIMESTAMP >= current_timestamp() - interval '2 day' THEN FROM_ADDRESS END) as day_before_wallets
        FROM SCROLL.RAW.TRANSACTIONS
        WHERE BLOCK_TIMESTAMP >= current_timestamp() - interval '2 day'
    )
    SELECT
        ROUND((100 * (past_day_wallets / NULLIF(day_before_wallets, 0)) - 100), 1) AS daily_growth
    FROM active_wallet_counts;
    ''')

    actives_7d = execute_sql('''
    SELECT COUNT(DISTINCT FROM_ADDRESS) as active_wallets
    FROM SCROLL.RAW.TRANSACTIONS
    WHERE BLOCK_TIMESTAMP >= current_timestamp - interval '7 day'
    ''')

    actives_growth_7d = execute_sql('''
    WITH active_wallet_counts AS (
        SELECT
            COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP >= current_timestamp() - interval '7 day' THEN FROM_ADDRESS END) as past_week_wallets,
            COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP < current_timestamp() - interval '7 day' AND BLOCK_TIMESTAMP >= current_timestamp() - interval '14 day' THEN FROM_ADDRESS END) as week_before_wallets
        FROM SCROLL.RAW.TRANSACTIONS
        WHERE BLOCK_TIMESTAMP >= current_timestamp() - interval '14 day'
    )
    SELECT
        ROUND((100 * (past_week_wallets / NULLIF(week_before_wallets, 0)) - 100), 1) AS weekly_growth
    FROM active_wallet_counts;
    ''')

    actives_1m = execute_sql('''
    SELECT COUNT(DISTINCT FROM_ADDRESS) as active_wallets 
    FROM SCROLL.RAW.TRANSACTIONS
    WHERE BLOCK_TIMESTAMP >= current_timestamp - interval '1 month' 
    ''')

    actives_growth_1m = execute_sql('''
    WITH active_wallet_counts AS (
        SELECT
            COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP >= current_timestamp() - interval '1 month' THEN FROM_ADDRESS END) as past_month_wallets,
            COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP < current_timestamp() - interval '1 month' AND BLOCK_TIMESTAMP >= current_timestamp() - interval '2 months' THEN FROM_ADDRESS END) as month_before_wallets
        FROM SCROLL.RAW.TRANSACTIONS
        WHERE BLOCK_TIMESTAMP >= current_timestamp() - interval '2 months'
    )
    SELECT
        ROUND((100 * (past_month_wallets / NULLIF(month_before_wallets, 0)) - 100), 1) AS monthly_growth
    FROM active_wallet_counts;
    ''')

    active_accounts_chart = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIMESTAMP), 'YY-MM-DD') as DATE,
    COUNT(DISTINCT FROM_ADDRESS) as active_wallets
    FROM SCROLL.RAW.TRANSACTIONS
    WHERE BLOCK_TIMESTAMP < date_trunc('{time}', BLOCK_TIMESTAMP)
    GROUP BY 1
    ORDER BY 1
    ''',
                                        time=timeframe)

    transactions_chart = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIMESTAMP), 'YY-MM-DD') as DATE,
    COUNT(*) as transactions
    FROM SCROLL.RAW.TRANSACTIONS
    WHERE BLOCK_TIMESTAMP < date_trunc('{time}', BLOCK_TIMESTAMP)
    GROUP BY 1
    ORDER BY 1
    ''',
                                     time=timeframe)

    if timeframe == 'week':
      retention_scope = 12
    elif timeframe == 'month':
      retention_scope = 6
    elif timeframe == 'day':
      retention_scope = 14

    retention_chart = execute_sql('''
    WITH transactions AS (
      SELECT FROM_ADDRESS, BLOCK_TIMESTAMP AS created_at
      FROM SCROLL.RAW.TRANSACTIONS
    ),

    cohort AS (
      SELECT 
        FROM_ADDRESS,
        MIN(date_trunc('{time}', created_at)) AS cohort_{time}
      FROM transactions
      GROUP BY 1
    ),

    cohort_size AS (
      SELECT
        cohort_{time},
        COUNT(1) as num_users
      FROM cohort
      GROUP BY cohort_{time}
    ),

    user_activities AS (
      SELECT
        DISTINCT
          DATEDIFF({time}, cohort_{time}, created_at) AS {time}_number,
          A.FROM_ADDRESS
      FROM transactions AS A
      LEFT JOIN cohort AS C 
      ON A.FROM_ADDRESS = C.FROM_ADDRESS
    ),

    retention_table AS (
      SELECT
        cohort_{time},
        A.{time}_number,
        COUNT(1) AS num_users
      FROM user_activities A
      LEFT JOIN cohort AS C 
      ON A.FROM_ADDRESS = C.FROM_ADDRESS
      GROUP BY 1, 2  
    )

    SELECT
      TO_VARCHAR(date_trunc('{time}', A.cohort_{time}), 'YY-MM-DD') AS cohort,
      B.num_users AS total_users,
      A.{time}_number,
      ROUND((A.num_users * 100 / B.num_users), 2) as percentage
    FROM retention_table AS A
    LEFT JOIN cohort_size AS B
    ON A.cohort_{time} = B.cohort_{time}
    WHERE 
      A.cohort_{time} IS NOT NULL
      AND A.cohort_{time} >= date_trunc('{time}', (CURRENT_TIMESTAMP() - interval '{retention_scope} {time}'))  
      AND A.cohort_{time} < date_trunc('{time}', CURRENT_TIMESTAMP())
    ORDER BY 1,3
    ''',
                                  time=timeframe,
                                  retention_scope=retention_scope)

    response_data = {
      "actives_24h": actives_24h,
      "actives_growth_24h": actives_growth_24h,
      "actives_7d": actives_7d,
      "actives_growth_7d": actives_growth_7d,
      "actives_1m": actives_1m,
      "actives_growth_1m": actives_growth_1m,
      "active_accounts_chart": active_accounts_chart,
      "transactions_chart": transactions_chart,
      "retention_chart": retention_chart
    }

    return jsonify(response_data)

  else:

    return 'COMING SOON'


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=81)

# REQUIREMENTS:
# 1. TO GET SNOWFLAKE
# POETRY ADD snowflake-connector-python
# 2. TO GET SSL
# sed -i '/    ];/i\      pkgs.openssl.out' replit.nix

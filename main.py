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
                                     database="WORLDMETRICS",
                                     schema="DBT_KOFI")

  sql = sql_string.format(**kwargs)
  res = conn.cursor(DictCursor).execute(sql)
  results = res.fetchall()
  conn.close()
  return results


@app.route('/overview')
@cache.memoize(make_name=make_cache_key)
def overview():
  timeframe = request.args.get('timeframe', 'week')

  actives_24h = execute_sql('''
    SELECT COUNT(DISTINCT ACCOUNT) as active_wallets
    FROM WORLDMETRICS.DBT_KOFI.WORLDAPP_OPTIMISM_ACTIONS
    WHERE BLOCK_TIMESTAMP >= current_timestamp - interval '1 day' 
  ''')

  actives_growth_24h = execute_sql('''
    WITH active_wallet_counts AS (
        SELECT
            COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP >= current_timestamp() - interval '1 day' THEN ACCOUNT END) as past_day_wallets,
            COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP < current_timestamp() - interval '1 day' AND BLOCK_TIMESTAMP >= current_timestamp() - interval '2 day' THEN ACCOUNT END) as day_before_wallets
        FROM WORLDMETRICS.DBT_KOFI.WORLDAPP_OPTIMISM_ACTIONS
        WHERE BLOCK_TIMESTAMP >= current_timestamp() - interval '2 day'
    )
    SELECT
        ROUND((100 * (past_day_wallets / NULLIF(day_before_wallets, 0)) - 100), 1) AS daily_growth
    FROM active_wallet_counts;
  ''')

  actives_7d = execute_sql('''
    SELECT COUNT(DISTINCT ACCOUNT) as active_wallets
    FROM WORLDMETRICS.DBT_KOFI.WORLDAPP_OPTIMISM_ACTIONS
    WHERE BLOCK_TIMESTAMP >= current_timestamp - interval '7 day'
  ''')

  actives_growth_7d = execute_sql('''
    WITH active_wallet_counts AS (
        SELECT
            COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP >= current_timestamp() - interval '7 day' THEN ACCOUNT END) as past_week_wallets,
            COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP < current_timestamp() - interval '7 day' AND BLOCK_TIMESTAMP >= current_timestamp() - interval '14 day' THEN ACCOUNT END) as week_before_wallets
        FROM WORLDMETRICS.DBT_KOFI.WORLDAPP_OPTIMISM_ACTIONS
        WHERE BLOCK_TIMESTAMP >= current_timestamp() - interval '14 day'
    )
    SELECT
        ROUND((100 * (past_week_wallets / NULLIF(week_before_wallets, 0)) - 100), 1) AS weekly_growth
    FROM active_wallet_counts;
  ''')

  actives_1m = execute_sql('''
    SELECT COUNT(DISTINCT ACCOUNT) as active_wallets 
    FROM WORLDMETRICS.DBT_KOFI.WORLDAPP_OPTIMISM_ACTIONS
    WHERE BLOCK_TIMESTAMP >= current_timestamp - interval '1 month' 
  ''')

  actives_growth_1m = execute_sql('''
    WITH active_wallet_counts AS (
        SELECT
            COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP >= current_timestamp() - interval '1 month' THEN ACCOUNT END) as past_month_wallets,
            COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP < current_timestamp() - interval '1 month' AND BLOCK_TIMESTAMP >= current_timestamp() - interval '2 months' THEN ACCOUNT END) as month_before_wallets
        FROM WORLDMETRICS.DBT_KOFI.WORLDAPP_OPTIMISM_ACTIONS
        WHERE BLOCK_TIMESTAMP >= current_timestamp() - interval '2 months'
    )
    SELECT
        ROUND((100 * (past_month_wallets / NULLIF(month_before_wallets, 0)) - 100), 1) AS monthly_growth
    FROM active_wallet_counts;
  ''')

  active_accounts_chart = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIMESTAMP), 'YY-MM-DD') as DATE,
    COUNT(DISTINCT ACCOUNT) as active_wallets
    FROM WORLDMETRICS.DBT_KOFI.WORLDAPP_OPTIMISM_ACTIONS
    WHERE BLOCK_TIMESTAMP < date_trunc('{time}', CURRENT_TIMESTAMP())
    AND BLOCK_TIMESTAMP > date_trunc('{time}', CURRENT_TIMESTAMP()) - interval '5 months'
    GROUP BY 1
    ORDER BY 1
  ''',
                                      time=timeframe)

  transactions_chart = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIMESTAMP), 'YY-MM-DD') as date,
    COUNT(*) as transactions
    FROM WORLDMETRICS.DBT_KOFI.WORLDAPP_OPTIMISM_ACTIONS
    WHERE BLOCK_TIMESTAMP < date_trunc('{time}', CURRENT_TIMESTAMP())
    AND BLOCK_TIMESTAMP > date_trunc('{time}', CURRENT_TIMESTAMP()) - interval '5 months'
    GROUP BY 1
    ORDER BY 1
  ''',
                                   time=timeframe)

  transactions_type_chart = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIMESTAMP), 'YY-MM-DD') as date,
    TXN_TYPE,
    COUNT(*) as transactions
    FROM WORLDMETRICS.DBT_KOFI.WORLDAPP_OPTIMISM_ACTIONS
    WHERE BLOCK_TIMESTAMP < date_trunc('{time}', CURRENT_TIMESTAMP())
    AND BLOCK_TIMESTAMP > date_trunc('{time}', CURRENT_TIMESTAMP()) - interval '5 months'
    GROUP BY 1,2
    ORDER BY 1
  ''',
                                        time=timeframe)

  account_deployments_chart = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIMESTAMP), 'YY-MM-DD') as date,
    COUNT(*) as deployments
    FROM WORLDMETRICS.DBT_KOFI.WORLDAPP_OPTIMISM_ACCOUNT_DEPLOYMENTS
    WHERE BLOCK_TIMESTAMP < date_trunc('{time}', CURRENT_TIMESTAMP())
    AND BLOCK_TIMESTAMP > date_trunc('{time}', CURRENT_TIMESTAMP()) - interval '5 months'
    GROUP BY 1
    ORDER BY 1
  ''',
                                          time=timeframe)

  response_data = {
    "actives_24h": actives_24h,
    "actives_growth_24h": actives_growth_24h,
    "actives_7d": actives_7d,
    "actives_growth_7d": actives_growth_7d,
    "actives_1m": actives_1m,
    "actives_growth_1m": actives_growth_1m,
    "active_accounts_chart": active_accounts_chart,
    "transactions_chart": transactions_chart,
    "account_deployments_chart": account_deployments_chart
  }

  return jsonify(response_data)


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=81)

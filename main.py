from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_caching import Cache
import snowflake.connector
from snowflake.connector import DictCursor
from datetime import datetime
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
    WHERE BLOCK_TIMESTAMP < date_trunc('{time}', CURRENT_TIMESTAMP())
    GROUP BY 1
    ORDER BY 1
    ''',
                                        time=timeframe)

    transactions_chart = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIMESTAMP), 'YY-MM-DD') as DATE,
    COUNT(*) as transactions
    FROM SCROLL.RAW.TRANSACTIONS
    WHERE BLOCK_TIMESTAMP < date_trunc('{time}', CURRENT_TIMESTAMP())
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
      WHERE BLOCK_TIMESTAMP < date_trunc('{time}', CURRENT_TIMESTAMP())
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

    # contract_users_chart = execute_sql('''
    # WITH RankedProjects AS (
    #   SELECT
    #     DATE_TRUNC('{time}', u.BLOCK_TIMESTAMP) AS DATE,
    #     CASE
    #         WHEN c.ADDRESS IS NOT NULL THEN c.ADDRESS
    #         WHEN VALUE > 0 THEN 'ETH transfer'
    #         ELSE 'empty_call'
    #     END AS PROJECT,
    #     COUNT(DISTINCT u.FROM_ADDRESS) AS NUM_UNIQUE_WALLETS,
    #     ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('{time}', u.BLOCK_TIMESTAMP) ORDER BY COUNT(DISTINCT u.FROM_ADDRESS) DESC) AS RN
    #   FROM
    #     SCROLL.RAW.TRANSACTIONS u
    #     LEFT JOIN SCROLL.RAW.CONTRACTS c ON u.TO_ADDRESS = c.ADDRESS

    #   WHERE (c.ADDRESS IS NOT NULL OR VALUE > 0)
    #   GROUP BY
    #     1, 2
    # ),
    # GroupedProjects AS (
    #   SELECT
    #     DATE,
    #     CASE WHEN RN <= 10 THEN PROJECT ELSE 'Other' END AS PROJECT,
    #     SUM(NUM_UNIQUE_WALLETS) AS NUM_UNIQUE_WALLETS
    #   FROM
    #     RankedProjects
    #   WHERE NUM_UNIQUE_WALLETS > 10
    #   GROUP BY
    #     1, 2
    # )
    # SELECT
    #   TO_VARCHAR(g.DATE, 'YYYY-MM-DD') AS DATE,
    #   COALESCE(l.NAME, g.PROJECT) AS PROJECT,
    #   g.NUM_UNIQUE_WALLETS
    # FROM
    #   GroupedProjects g
    # LEFT JOIN SCROLLSTATS.DBT_SCROLLSTATS.SCROLLSTATS_LABELS_APPS l ON g.PROJECT = l.ADDRESS
    # ORDER BY
    #   g.DATE, g.NUM_UNIQUE_WALLETS DESC;
    # ''',
    #                                    time=timeframe)

    contract_transactions_chart = execute_sql('''
    WITH RankedProjects AS (
      SELECT
        DATE_TRUNC('{time}', u.BLOCK_TIMESTAMP) AS DATE,
        CASE
            WHEN c.ADDRESS IS NOT NULL THEN c.ADDRESS
            WHEN VALUE > 0 THEN 'ETH transfer'
            ELSE 'empty_call'
        END AS PROJECT,
        COUNT(DISTINCT u.HASH) AS NUM_TRANSACTIONS,
        COUNT(DISTINCT u.FROM_ADDRESS) AS NUM_UNIQUE_WALLETS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('{time}', u.BLOCK_TIMESTAMP) ORDER BY COUNT(DISTINCT u.FROM_ADDRESS) DESC) AS RN
      FROM
        SCROLL.RAW.TRANSACTIONS u
        LEFT JOIN SCROLL.RAW.CONTRACTS c ON u.TO_ADDRESS = c.ADDRESS

      WHERE (c.ADDRESS IS NOT NULL OR VALUE > 0)
      GROUP BY
        1, 2
    ),
    GroupedProjects AS (
      SELECT
        DATE,
        CASE WHEN RN <= 10 THEN PROJECT ELSE 'Other' END AS PROJECT,
        SUM(NUM_TRANSACTIONS) AS NUM_TRANSACTIONS
      FROM
        RankedProjects
      WHERE NUM_UNIQUE_WALLETS > 10
      GROUP BY
        1, 2
    )
    SELECT
      TO_VARCHAR(g.DATE, 'YYYY-MM-DD') AS DATE,
      COALESCE(l.NAME, g.PROJECT) AS PROJECT,
      g.NUM_TRANSACTIONS
    FROM
      GroupedProjects g
    LEFT JOIN SCROLLSTATS.DBT_SCROLLSTATS.SCROLLSTATS_LABELS_APPS l ON g.PROJECT = l.ADDRESS
    ORDER BY
      g.DATE, g.NUM_TRANSACTIONS DESC;
    ''',
                                              time=timeframe)

    contract_gas_chart = execute_sql('''
    WITH RankedProjects AS (
      SELECT 
        DATE_TRUNC('{time}', u.BLOCK_TIMESTAMP) AS DATE,
        CASE 
            WHEN c.ADDRESS IS NOT NULL THEN c.ADDRESS
            WHEN VALUE > 0 THEN 'ETH transfer'
            ELSE 'empty_call'
        END AS PROJECT,
        SUM((RECEIPT_L1_FEE + GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS ETH_FEES,
        COUNT(DISTINCT u.FROM_ADDRESS) AS NUM_UNIQUE_WALLETS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('{time}', u.BLOCK_TIMESTAMP) ORDER BY SUM((RECEIPT_L1_FEE + GAS_PRICE * RECEIPT_GAS_USED)/1e18) DESC) AS RN
      FROM 
        SCROLL.RAW.TRANSACTIONS u
        LEFT JOIN SCROLL.RAW.CONTRACTS c ON u.TO_ADDRESS = c.ADDRESS

      WHERE (c.ADDRESS IS NOT NULL OR VALUE > 0)
      GROUP BY 
        1, 2
    ),
    GroupedProjects AS (
      SELECT 
        DATE, 
        CASE WHEN RN <= 10 THEN PROJECT ELSE 'Other' END AS PROJECT,
        SUM(ETH_FEES) AS ETH_FEES
      FROM 
        RankedProjects
      WHERE NUM_UNIQUE_WALLETS > 10
      GROUP BY 
        1, 2
    )
    SELECT 
      TO_VARCHAR(g.DATE, 'YYYY-MM-DD') AS DATE, 
      COALESCE(l.NAME, g.PROJECT) AS PROJECT, 
      g.ETH_FEES
    FROM 
      GroupedProjects g
    LEFT JOIN SCROLLSTATS.DBT_SCROLLSTATS.SCROLLSTATS_LABELS_APPS l ON g.PROJECT = l.ADDRESS
    ORDER BY 
      g.DATE, g.ETH_FEES DESC;
    ''',
                                     time=timeframe)

    trending_contracts = execute_sql('''
    WITH time_settings AS (
    SELECT 
        CURRENT_TIMESTAMP() - INTERVAL '1 {time}' AS one_{time}_ago,
        CURRENT_TIMESTAMP() - INTERVAL '2 {time}' AS two_{time}s_ago
    ),
    aggregated_data AS (
        SELECT 
            t.TO_ADDRESS AS contract,  
            COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP >= ts.one_{time}_ago THEN t.HASH END) AS txns_current,
            COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP < ts.one_{time}_ago AND t.BLOCK_TIMESTAMP >= ts.two_{time}s_ago THEN t.HASH END) AS txns_previous,
            COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP >= ts.one_{time}_ago THEN t.FROM_ADDRESS END) AS active_accounts_current,
            COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP < ts.one_{time}_ago AND t.BLOCK_TIMESTAMP >= ts.two_{time}s_ago THEN t.FROM_ADDRESS END) AS active_accounts_previous,
            SUM(CASE WHEN t.BLOCK_TIMESTAMP >= ts.one_{time}_ago THEN (t.GAS_PRICE * t.RECEIPT_GAS_USED)/1e18 END) AS gas_spend_current,
            SUM(CASE WHEN t.BLOCK_TIMESTAMP < ts.one_{time}_ago AND t.BLOCK_TIMESTAMP >= ts.two_{time}s_ago THEN (t.GAS_PRICE * t.RECEIPT_GAS_USED)/1e18 END) AS gas_spend_previous
        FROM 
            SCROLL.RAW.TRANSACTIONS t  
        INNER JOIN 
            SCROLL.RAW.CONTRACTS c ON t.TO_ADDRESS = c.ADDRESS
        CROSS JOIN 
            time_settings ts
        WHERE 
            t.BLOCK_TIMESTAMP >= ts.two_{time}s_ago
        GROUP BY 
            t.TO_ADDRESS
    )
    
    SELECT
        ad.contract,
        COALESCE(l.NAME, 'Unknown') AS project,
        ad.gas_spend_current,
        CASE 
            WHEN ad.gas_spend_previous > 0 THEN (100 * (ad.gas_spend_current - ad.gas_spend_previous) / ad.gas_spend_previous) 
            ELSE NULL 
        END as gas_growth,
        ad.txns_current,
        CASE 
            WHEN ad.txns_previous > 0 THEN (100 * (ad.txns_current - ad.txns_previous) / ad.txns_previous) 
            ELSE NULL 
        END as txn_growth,
        ad.active_accounts_current,
        CASE 
            WHEN ad.active_accounts_previous > 0 THEN (100 * (ad.active_accounts_current - ad.active_accounts_previous) / ad.active_accounts_previous) 
            ELSE NULL 
        END as accounts_growth
    FROM 
        aggregated_data ad
    LEFT JOIN SCROLLSTATS.DBT_SCROLLSTATS.SCROLLSTATS_LABELS_APPS l ON ad.contract = l.ADDRESS    
    WHERE 
        ad.active_accounts_previous > 10
        AND (ad.active_accounts_current - ad.active_accounts_previous) > 0
    ORDER BY 
        ad.txns_current DESC
    LIMIT 20
    ''',
                                     time=timeframe)

    current_time = datetime.now().strftime('%d/%m/%y %H:%M')

    response_data = {
      "time": current_time,
      "actives_24h": actives_24h,
      "actives_growth_24h": actives_growth_24h,
      "actives_7d": actives_7d,
      "actives_growth_7d": actives_growth_7d,
      "actives_1m": actives_1m,
      "actives_growth_1m": actives_growth_1m,
      "active_accounts_chart": active_accounts_chart,
      "transactions_chart": transactions_chart,
      "retention_chart": retention_chart,
      # "contract_users_chart": contract_users_chart,
      "contract_transactions_chart": contract_transactions_chart,
      "contract_gas_chart": contract_gas_chart,
      "trending_contracts": trending_contracts
    }

    return jsonify(response_data)

  else:

    return 'COMING SOON'


@app.route('/bd')
@cache.memoize(make_name=make_cache_key)
def bd():
  timeframe = request.args.get('timeframe', 'month')

  leaderboard = execute_sql('''
  WITH time_settings AS (
      SELECT 
          CURRENT_TIMESTAMP() - INTERVAL '1 {time}' AS one_{time}_ago,
          CURRENT_TIMESTAMP() - INTERVAL '2 {time}' AS two_{time}s_ago
  ),
  aggregated_data AS (
      SELECT 
          t.TO_ADDRESS AS contract,  
          COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP >= ts.one_{time}_ago THEN t.HASH END) AS txns_current,
          COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP < ts.one_{time}_ago AND t.BLOCK_TIMESTAMP >= ts.two_{time}s_ago THEN t.HASH END) AS txns_previous,
          COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP >= ts.one_{time}_ago THEN t.FROM_ADDRESS END) AS active_accounts_current,
          COUNT(DISTINCT CASE WHEN t.BLOCK_TIMESTAMP < ts.one_{time}_ago AND t.BLOCK_TIMESTAMP >= ts.two_{time}s_ago THEN t.FROM_ADDRESS END) AS active_accounts_previous,
          SUM(CASE WHEN t.BLOCK_TIMESTAMP >= ts.one_{time}_ago THEN ((t.RECEIPT_L1_FEE + t.GAS_PRICE * t.RECEIPT_GAS_USED)/1e18) END) AS gas_spend_current,
          SUM(CASE WHEN t.BLOCK_TIMESTAMP < ts.one_{time}_ago AND t.BLOCK_TIMESTAMP >= ts.two_{time}s_ago THEN ((t.RECEIPT_L1_FEE + t.GAS_PRICE * t.RECEIPT_GAS_USED)/1e18) END) AS gas_spend_previous
      FROM 
          SCROLL.RAW.TRANSACTIONS t  
      INNER JOIN 
          SCROLL.RAW.CONTRACTS c ON t.TO_ADDRESS = c.ADDRESS
      CROSS JOIN 
          time_settings ts
      WHERE 
          t.BLOCK_TIMESTAMP >= ts.two_{time}s_ago
      GROUP BY 
          t.TO_ADDRESS
  )
  
  SELECT
      l.NAME AS project,
      ad.gas_spend_current as ETH_FEES,
      CASE 
          WHEN ad.gas_spend_previous > 0 THEN (100 * (ad.gas_spend_current - ad.gas_spend_previous) / ad.gas_spend_previous) 
          ELSE 0 
      END as ETH_FEES_GROWTH,
      ad.txns_current as TRANSACTIONS,
      CASE 
          WHEN ad.txns_previous > 0 THEN (100 * (ad.txns_current - ad.txns_previous) / ad.txns_previous) 
          ELSE 0 
      END as TRANSACTIONS_GROWTH,
      ad.active_accounts_current as WALLETS,
      CASE 
          WHEN ad.active_accounts_previous > 0 THEN (100 * (ad.active_accounts_current - ad.active_accounts_previous) / ad.active_accounts_previous) 
          ELSE 0 
      END as WALLETS_GROWTH
  FROM 
      aggregated_data ad
  INNER JOIN SCROLLSTATS.DBT_SCROLLSTATS.SCROLLSTATS_LABELS_APPS l ON ad.contract = l.ADDRESS    
  ORDER BY 
      ad.gas_spend_current DESC
    ''',
                            time=timeframe)

  response_data = {
    "leaderboard": leaderboard,
  }

  return jsonify(response_data)


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=81)

# REQUIREMENTS:
# 1. TO GET SNOWFLAKE
# POETRY ADD snowflake-connector-python
# 2. TO GET SSL
# sed -i '/    ];/i\      pkgs.openssl.out' replit.nix

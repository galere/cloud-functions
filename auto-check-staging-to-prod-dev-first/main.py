import sqlalchemy
import os


def init_connection_engine(database):
    db_config = {
        # [START cloud_sql_postgres_sqlalchemy_limit]
        # Pool size is the maximum number of permanent connections to keep.
        "pool_size": 5,
        # Temporarily exceeds the set pool_size if no connections are available.
        "max_overflow": 2,
        # The total number of concurrent connections for your application will be
        # a total of pool_size and max_overflow.
        # [END cloud_sql_postgres_sqlalchemy_limit]

        # [START cloud_sql_postgres_sqlalchemy_backoff]
        # SQLAlchemy automatically uses delays between failed connection attempts,
        # but provides no arguments for configuration.
        # [END cloud_sql_postgres_sqlalchemy_backoff]

        # [START cloud_sql_postgres_sqlalchemy_timeout]
        # 'pool_timeout' is the maximum number of seconds to wait when retrieving a
        # new connection from the pool. After the specified amount of time, an
        # exception will be thrown.
        "pool_timeout": 30,  # 30 seconds
        # [END cloud_sql_postgres_sqlalchemy_timeout]

        # [START cloud_sql_postgres_sqlalchemy_lifetime]
        # 'pool_recycle' is the maximum number of seconds a connection can persist.
        # Connections that live longer than the specified amount of time will be
        # reestablished
        "pool_recycle": 1800,  # 30 minutes
        # [END cloud_sql_postgres_sqlalchemy_lifetime]
    }

    return init_unix_connection_engine(db_config, database)

def init_unix_connection_engine(db_config, database):
    db_user = os.environ.get("DB_USER")
    db_pass = os.environ.get("DB_PASS")
    db_name = os.environ.get("DB_PROD_NAME") if database == 'prod' else os.environ.get("DB_STG_NAME")
    db_socket_dir = os.environ.get("DB_SOCKET_DIR", "/cloudsql")
    cloud_sql_connection_name = os.environ["CLOUD_SQL_CONNECTION_NAME"]

    pool = sqlalchemy.create_engine(

        # Equivalent URL:
        # postgresql+pg8000://<db_user>:<db_pass>@/<db_name>
        #                         ?unix_sock=<socket_path>/<cloud_sql_instance_name>/.s.PGSQL.5432
        sqlalchemy.engine.url.URL.create(
            drivername="postgresql+pg8000",
            username=db_user,  # e.g. "my-database-user"
            password=db_pass,  # e.g. "my-database-password"
            database=db_name,  # e.g. "my-database-name"
            query={
                "unix_sock": "{}/{}/.s.PGSQL.5432".format(
                    db_socket_dir,  # e.g. "/cloudsql"
                    cloud_sql_connection_name)  # i.e "<PROJECT-NAME>:<INSTANCE-REGION>:<INSTANCE-NAME>"
            }
        ),
        **db_config
    )

    pool.dialect.description_encoding = None
    return pool

prod_db = None
stg_db = None


def invoke(event, callback):

    global prod_db
    global stg_db

    # Connect to production database
    prod_db = init_connection_engine('prod')
    prod_conn = prod_db.connect()

    # Connect to staging database
    stg_db = init_connection_engine('stg')
    stg_conn = stg_db.connect()



    cursor = prod_conn.execute("select count(*) as count_target from published.public.ft_d_market")
    prod_ft_d_market_count = cursor.fetchall()
    prod_ft_d_market_count = prod_ft_d_market_count[0][0]
    print('prod_ft_d_market_count is equals to: {}'.format(prod_ft_d_market_count))

    cursor = prod_conn.execute("select count(*) as count_target from published.public.ft_d_hfb")
    prod_ft_d_hfb_count = cursor.fetchall()
    prod_ft_d_hfb_count = prod_ft_d_hfb_count[0][0]
    print('prod_ft_d_hfb_count is equals to: {}'.format(prod_ft_d_hfb_count))

    cursor = prod_conn.execute("select count(*) as count_target from published.public.ft_d_version")
    prod_ft_d_version_count = cursor.fetchall()
    prod_ft_d_version_count = prod_ft_d_version_count[0][0]
    print('prod_ft_d_version_count is equals to: {}'.format(prod_ft_d_version_count))

    cursor = prod_conn.execute("select count(*) as count_target from published.public.ft_d_view")
    prod_ft_d_view_count = cursor.fetchall()
    prod_ft_d_view_count = prod_ft_d_view_count[0][0]
    print('prod_ft_d_view_count is equals to: {}'.format(prod_ft_d_view_count))

    cursor = prod_conn.execute("select count(*) as source_count from ( select fct.view,object,fct.version,market,measures::text from public.ft_f_report_market fct inner join public.ft_d_version ver on fct.view = ver.view and fct.version = ver.version order by fct.view ) count_table")
    prod_ft_f_report_market_count = cursor.fetchall()
    prod_ft_f_report_market_count = prod_ft_f_report_market_count[0][0]
    print('prod_ft_f_report_market_count is equals to: {}'.format(prod_ft_f_report_market_count))


    # Get count from staging database
    stg_cursor = stg_conn.execute("select count(*) as count_target from postgres.public.ft_d_market")
    stg_ft_d_market_count = stg_cursor.fetchall()
    stg_ft_d_market_count = stg_ft_d_market_count[0][0]
    print('stg_ft_d_market_count is equals to: {}'.format(stg_ft_d_market_count))

    stg_cursor = stg_conn.execute("select count(*) as count_target from postgres.public.ft_d_hfb")
    stg_ft_d_hfb_count = stg_cursor.fetchall()
    stg_ft_d_hfb_count = stg_ft_d_hfb_count[0][0]
    print('stg_ft_d_hfb_count is equals to: {}'.format(stg_ft_d_hfb_count))

    stg_cursor = stg_conn.execute("select count(*) as count_target from postgres.public.ft_d_version")
    stg_ft_d_version_count = stg_cursor.fetchall()
    stg_ft_d_version_count = stg_ft_d_version_count[0][0]
    print('stg_ft_d_version_count is equals to: {}'.format(stg_ft_d_version_count))

    stg_cursor = stg_conn.execute("select count(*) as count_target from postgres.public.ft_d_view")
    stg_ft_d_view_count = stg_cursor.fetchall()
    stg_ft_d_view_count = stg_ft_d_view_count[0][0]
    print('stg_ft_d_view_count is equals to: {}'.format(stg_ft_d_view_count))

    stg_cursor = stg_conn.execute("select count(*) as source_count from ( select fct.view,object,fct.version,market,measures::text from postgres.public.ft_f_report_market fct inner join postgres.public.ft_d_version ver on fct.view = ver.view and fct.version = ver.version order by fct.view ) count_table")
    stg_ft_f_report_market_count = stg_cursor.fetchall()
    stg_ft_f_report_market_count = stg_ft_f_report_market_count[0][0]
    print('stg_ft_f_report_market_count is equals to: {}'.format(stg_ft_f_report_market_count))


    if (stg_ft_d_market_count - prod_ft_d_market_count) == 0 or (stg_ft_d_hfb_count - prod_ft_d_hfb_count) == 0 or (stg_ft_d_version_count - prod_ft_d_version_count) == 0\
           or (stg_ft_d_view_count - prod_ft_d_view_count) == 0 or (stg_ft_f_report_market_count - prod_ft_f_report_market_count) == 0:
        condition = "True"
    else:
        condition = "False"

    prod_conn.close()
    stg_conn.close()

    return condition

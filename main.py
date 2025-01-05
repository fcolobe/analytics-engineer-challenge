import duckdb


################
# DATA LOADING #
################
def load_data(conn) -> None:
    """Load parquet files into raw schema"""
    #! This method shouldn't be modified by the candidate
    sql = """
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE OR REPLACE VIEW raw.products AS
    SELECT * FROM read_parquet('data/products.parquet');

    CREATE OR REPLACE VIEW raw.product_territories AS
    SELECT * FROM read_parquet('data/product_territories.parquet');

    CREATE OR REPLACE VIEW raw.users AS
    SELECT * FROM read_parquet('data/users.parquet');

    CREATE OR REPLACE VIEW raw.history_search AS
    SELECT * FROM read_parquet('data/history_search.parquet');

    CREATE OR REPLACE VIEW raw.dispatch_bookings AS
    SELECT * FROM read_parquet('data/dispatch_bookings.parquet');
    """
    conn.sql(sql)


####################
# DATA PREPARATION #
####################
def prepare_datasets(conn) -> None:
    """Create prepared analytical datasets"""
    # TODO: Here, create VIEWS and/or TABLE to build a queryable data model
    sql = """
    CREATE SCHEMA IF NOT EXISTS prepared;
    """
    conn.sql(sql)


def create_reports(conn) -> None:
    """Create reporting views"""
    # TODO: Here, create VIEWS and/or TABLE to build the core data models
    sql = """
    CREATE SCHEMA IF NOT EXISTS reporting;
    """
    conn.sql(sql)


################
# DATA ANALYZE #
################
# TODO: In each of the function, complete the sql query to return the results
def trips_per_country(conn) -> None:
    """Question 1.1: How many completed trips do we have per country?"""
    sql = "SELECT 42 as trips_count"
    conn.sql(sql).show()


def user_averages(conn) -> None:
    """Question 1.2:
    What's the average number of searches, bookings, and completed trips per user?"""
    sql = "SELECT 42 as avg_searches, 42 as avg_bookings, 42 as avg_completed_trips"
    conn.sql(sql).show()


if __name__ == "__main__":
    conn = duckdb.connect(
        "padam_data.duckdb", config={"threads": 4, "memory_limit": "2GB"}
    )

    try:
        load_data(conn)
        print("✓ Raw views created")

        prepare_datasets(conn)
        print("✓ Prepared datasets created")

        create_reports(conn)
        print("✓ Reporting views created")

        print("\nAnalysis Results:")
        print("\nCompleted trips per country:")
        trips_per_country(conn)

        print("\nUser activity averages:")
        user_averages(conn)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

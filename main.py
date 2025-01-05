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
    sql = """
    CREATE SCHEMA IF NOT EXISTS prepared;

    -- Users preparation with their product information
    CREATE OR REPLACE VIEW prepared.users AS
    SELECT 
        u.id as user_id,
        u.product_slug,
        u.registered_at,
        p.name as product_name,
        p.country_code as country
    FROM raw.users u
    LEFT JOIN raw.products p ON u.product_slug = p.slug;

    -- Search history with context
    CREATE OR REPLACE VIEW prepared.searches AS
    SELECT 
        hs.id as search_id,
        hs.user_id,
        hs.territory_id,
        hs.product_slug,
        hs.passengers_number,
        hs.nb_propositions,
        hs.created_at as search_datetime,
        pt.name as territory_name,
        p.country_code as country
    FROM raw.history_search hs
    LEFT JOIN raw.product_territories pt ON hs.territory_id = pt.id
    LEFT JOIN raw.products p ON hs.product_slug = p.slug;

    -- Bookings preparation with completion status
    CREATE OR REPLACE VIEW prepared.bookings AS
    SELECT 
        db.id as booking_id,
        hs.user_id,
        hs.territory_id,
        db.product_slug,
        db.status,
        db.passenger_status,
        db.created_at as booking_datetime,
        db.pickup_time as pickup_datetime,
        CASE 
            WHEN db.passenger_status = 'DROPOFF' AND db.status = 'VALIDATED' 
            THEN TRUE 
            ELSE FALSE 
        END as is_completed_trip,
        p.country_code as country
    FROM raw.dispatch_bookings db
    LEFT JOIN raw.history_search hs ON db.search_id = hs.id
    LEFT JOIN raw.products p ON db.product_slug = p.slug;
    """
    conn.sql(sql)


def create_reports(conn) -> None:
    """Create reporting views"""
    sql = """
    CREATE SCHEMA IF NOT EXISTS reporting;

    -- 1. Number of completed trips by country
    CREATE OR REPLACE VIEW reporting.completed_trips_by_country AS
    SELECT 
        b.country,
        COUNT(DISTINCT b.booking_id) as completed_trips_count
    FROM prepared.bookings b
    WHERE b.is_completed_trip = TRUE
    GROUP BY b.country
    ORDER BY completed_trips_count DESC;

    -- 2. Average metrics per user
    CREATE OR REPLACE VIEW reporting.user_engagement_metrics AS
    SELECT 
        u.country,
        COUNT(DISTINCT CONCAT(u.user_id, '_', u.product_slug)) as total_users,
        ROUND(COUNT(DISTINCT s.search_id)::FLOAT / COUNT(DISTINCT CONCAT(u.user_id, '_', u.product_slug)), 2) as avg_searches_per_user,
        ROUND(COUNT(DISTINCT b.booking_id)::FLOAT / COUNT(DISTINCT CONCAT(u.user_id, '_', u.product_slug)), 2) as avg_bookings_per_user,
        ROUND(COUNT(DISTINCT CASE WHEN b.is_completed_trip THEN b.booking_id END)::FLOAT / 
              COUNT(DISTINCT CONCAT(u.user_id, '_', u.product_slug)), 2) as avg_completed_trips_per_user
    FROM prepared.users u
    LEFT JOIN prepared.searches s ON u.user_id = s.user_id AND u.product_slug = s.product_slug
    LEFT JOIN prepared.bookings b ON u.user_id = b.user_id AND u.product_slug = b.product_slug
    GROUP BY u.country
    ORDER BY total_users DESC;

    -- 3. Territory conversion rates
    CREATE OR REPLACE VIEW reporting.territory_conversion_rates AS
    SELECT 
        pt.name as territory_name,
        p.country_code as country,
        COUNT(DISTINCT s.search_id) as total_searches,
        COUNT(DISTINCT b.booking_id) as total_bookings,
        ROUND(COUNT(DISTINCT b.booking_id)::FLOAT / NULLIF(COUNT(DISTINCT s.search_id), 0) * 100, 2) as search_to_booking_rate,
        COUNT(DISTINCT CASE WHEN b.is_completed_trip THEN b.booking_id END) as completed_trips,
        ROUND(COUNT(DISTINCT CASE WHEN b.is_completed_trip THEN b.booking_id END)::FLOAT / 
              NULLIF(COUNT(DISTINCT s.search_id), 0) * 100, 2) as search_to_completion_rate
    FROM raw.product_territories pt
    JOIN raw.products p ON pt.product_slug = p.slug
    LEFT JOIN prepared.searches s ON pt.id = s.territory_id
    LEFT JOIN prepared.bookings b ON pt.id = b.territory_id
    GROUP BY pt.name, p.country_code
    HAVING COUNT(DISTINCT s.search_id) > 0
    ORDER BY total_searches DESC;
    """
    conn.sql(sql)


################
# DATA ANALYZE #
################
# TODO: In each of the function, complete the sql query to return the results
def trips_per_country(conn) -> None:
    """Question 1.1: How many completed trips do we have per country?"""
    sql = """
    SELECT 
        country,
        completed_trips_count
    FROM reporting.completed_trips_by_country;
    """
    conn.sql(sql).show()


def user_averages(conn) -> None:
    """Question 1.2:
    What's the average number of searches, bookings, and completed trips per user?"""
    sql = """
    SELECT 
        country,
        total_users,
        avg_searches_per_user,
        avg_bookings_per_user,
        avg_completed_trips_per_user
    FROM reporting.user_engagement_metrics;
    """
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

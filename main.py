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
        'All Countries' as scope,
        COUNT(DISTINCT CONCAT(u.user_id, '_', u.product_slug)) as total_users,
        ROUND(COUNT(DISTINCT s.search_id)::FLOAT / COUNT(DISTINCT CONCAT(u.user_id, '_', u.product_slug)), 2) as avg_searches_per_user,
        ROUND(COUNT(DISTINCT b.booking_id)::FLOAT / COUNT(DISTINCT CONCAT(u.user_id, '_', u.product_slug)), 2) as avg_bookings_per_user,
        ROUND(COUNT(DISTINCT CASE WHEN b.is_completed_trip THEN b.booking_id END)::FLOAT / 
            COUNT(DISTINCT CONCAT(u.user_id, '_', u.product_slug)), 2) as avg_completed_trips_per_user
    FROM prepared.users u
    LEFT JOIN prepared.searches s ON u.user_id = s.user_id AND u.product_slug = s.product_slug
    LEFT JOIN prepared.bookings b ON u.user_id = b.user_id AND u.product_slug = b.product_slug;

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

    -- 4. Booking channel distribution for completed trips
    CREATE OR REPLACE VIEW reporting.booking_channels_analysis AS
    SELECT 
        db.booked_from,
        COUNT(DISTINCT b.booking_id) as total_bookings,
        ROUND(COUNT(DISTINCT b.booking_id)::FLOAT / 
            SUM(COUNT(DISTINCT b.booking_id)) OVER () * 100, 2) as percentage
    FROM prepared.bookings b
    JOIN raw.dispatch_bookings db ON b.booking_id = db.id
    WHERE b.is_completed_trip = TRUE
    GROUP BY db.booked_from
    ORDER BY total_bookings DESC;

    -- 5. Cancellation rates by territory
    CREATE OR REPLACE VIEW reporting.cancellation_analysis AS
    SELECT 
        pt.name as territory_name,
        p.country_code as country,
        COUNT(DISTINCT b.booking_id) as total_bookings,
        COUNT(DISTINCT CASE WHEN b.status LIKE 'CANCELED%' THEN b.booking_id END) as cancelled_bookings,
        ROUND(COUNT(DISTINCT CASE WHEN b.status LIKE 'CANCELED%' THEN b.booking_id END)::FLOAT / 
            NULLIF(COUNT(DISTINCT b.booking_id), 0) * 100, 2) as cancellation_rate
    FROM raw.product_territories pt
    JOIN raw.products p ON pt.product_slug = p.slug
    LEFT JOIN prepared.bookings b ON pt.id = b.territory_id
    GROUP BY pt.name, p.country_code
    HAVING COUNT(DISTINCT b.booking_id) > 0
    ORDER BY cancellation_rate DESC;

    -- 6. Peak hours analysis by country
    CREATE OR REPLACE VIEW reporting.peak_hours_analysis AS
    SELECT 
        country,
        EXTRACT(HOUR FROM booking_datetime) as hour_of_day,
        COUNT(DISTINCT booking_id) as total_bookings,
        ROUND(COUNT(DISTINCT booking_id)::FLOAT / 
            SUM(COUNT(DISTINCT booking_id)) OVER (PARTITION BY country) * 100, 2) as percentage_of_daily_bookings
    FROM prepared.bookings
    GROUP BY country, hour_of_day
    ORDER BY country, total_bookings DESC;

    -- Add conversion rates analysis (optimized)
    CREATE OR REPLACE VIEW reporting.search_to_completion_rates AS
    WITH search_stats AS (
        SELECT 
            territory_id,
            COUNT(DISTINCT search_id) as total_searches
        FROM prepared.searches
        GROUP BY territory_id
    ),
    booking_stats AS (
        SELECT 
            territory_id,
            COUNT(DISTINCT booking_id) as total_bookings,
            COUNT(DISTINCT CASE WHEN is_completed_trip THEN booking_id END) as completed_trips
        FROM prepared.bookings
        GROUP BY territory_id
    )
    SELECT 
        pt.name as territory_name,
        p.country_code as country,
        COALESCE(s.total_searches, 0) as total_searches,
        COALESCE(b.total_bookings, 0) as total_bookings,
        COALESCE(b.completed_trips, 0) as completed_trips,
        ROUND(COALESCE(b.completed_trips, 0)::FLOAT / NULLIF(s.total_searches, 0) * 100, 2) as search_to_completion_rate
    FROM raw.product_territories pt
    JOIN raw.products p ON pt.product_slug = p.slug
    LEFT JOIN search_stats s ON pt.id = s.territory_id
    LEFT JOIN booking_stats b ON pt.id = b.territory_id
    WHERE s.total_searches > 0
    ORDER BY search_to_completion_rate DESC
    LIMIT 10;
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
        avg_searches_per_user,
        avg_bookings_per_user,
        avg_completed_trips_per_user
    FROM reporting.user_engagement_metrics;
    """
    conn.sql(sql).show()


def analyze_conversion_rates(conn) -> None:
    """Question 2.1: What's the conversion rate from searches to completed trips per territory?"""
    sql = """
    SELECT * FROM reporting.search_to_completion_rates;
    """
    conn.sql(sql).show()


def analyze_booking_channels(conn) -> None:
    """Question 2.2: What's the distribution of booking channels for completed trips?"""
    sql = """
    SELECT * FROM reporting.booking_channels_analysis;
    """
    conn.sql(sql).show()


def analyze_cancellations(conn) -> None:
    """Question 2.3: What are the cancellation patterns across territories?"""
    sql = """
    SELECT * FROM reporting.cancellation_analysis;
    """
    conn.sql(sql).show()


def analyze_peak_hours(conn) -> None:
    """Question 2.4: What are the peak booking hours per country?"""
    sql = """
    SELECT * FROM reporting.peak_hours_analysis;
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

        print("\nOptional Analysis Results:")

        print("\nSearch to Completion Conversion Rates:")
        analyze_conversion_rates(conn)

        print("\nBooking Channels Distribution:")
        analyze_booking_channels(conn)

        # print("\nCancellation Analysis:")
        # analyze_cancellations(conn)

        # print("\nPeak Hours Analysis:")
        # analyze_peak_hours(conn)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

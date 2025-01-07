# Analytics Engineering Technical Test Report

**Author:** Fonty COLO BE

## Data Modeling Choices

1. **Prepared Layer**
   - Created normalized views for users, searches, and bookings
   - Added proper joins with product information for country context
   - Implemented unique user identification across products using composite keys
   - Added `is_completed_trip` flag based on booking status

2. **Reporting Layer**
   - Created specific views for each analysis requirement
   - Implemented proper aggregations with consideration for unique identifiers
   - Maintained data consistency across different metrics

## Answers to Questions

### 1. Required Stats on Operations

#### 1.1 Completed Trips per Country
| Country | Completed Trips |
|---------|----------------|
| FR      | 6,126         |
| EN      | 5,598         |
| DE      | 2,051         |

##### Interpretation:
- France leads with 6,126 completed trips, showing the strongest market presence
- England follows with 5,598 completed trips
- Germany shows lower adoption with 2,051 completed trips

#### 1.2 Average User Activity
| Avg Searches | Avg Bookings | Avg Completed Trips |
|--------------|--------------|---------------------|
| 3.75         | 1.19        | 0.75               |

##### Interpretation:
- Users perform an average of 3.75 searches before making a booking
- Each user makes approximately 1.19 bookings on average
- The average of 0.75 completed trips per user suggests that not all bookings result in completed trips
- The conversion funnel shows expected drop-offs at each stage

### 2. Optional Operational Insights

#### 2.1 Search to Completion Conversion Rates by Territory
| Territory | Country | Total Searches | Total Bookings | Completed Trips | Conversion Rate |
|-----------|---------|----------------|----------------|-----------------|----------------|
| Tewkesbury District | EN | 3,004 | 1,385 | 1,127 | 37.52% |
| VitaBus Soir | FR | 3,004 | 1,385 | 1,127 | 37.52% |
| wil | DE | 7,363 | 2,913 | 2,051 | 27.86% |
| pays_de_langres | FR | 7,363 | 2,913 | 2,051 | 27.86% |
| gcc_se_forest_of_dean | EN | 31,988 | 11,632 | 7,334 | 22.93% |
| bourges_pmr | FR | 31,988 | 11,632 | 7,334 | 22.93% |
| Berkeley Vale | EN | 8,681 | 2,198 | 1,392 | 16.04% |
| South Cotswolds | EN | 8,803 | 1,931 | 1,078 | 12.25% |
| gcc_north_costwolds | EN | 8,049 | 1,497 | 793 | 9.85% |
| bourges_tad | FR | 8,049 | 1,497 | 793 | 9.85% |

##### Interpretation:
- Highest conversion rates are seen in Tewkesbury District (EN) and VitaBus Soir (FR) at 37.52%
- Lowest conversion rates are in gcc_north_costwolds (EN) and bourges_tad (FR) at 9.85%
- Several territories show identical metrics, suggesting potential data quality issues or shared operational characteristics
- Conversion rates vary significantly from 9.85% to 37.52%, indicating large performance differences between territories

**Data Quality Note**: Multiple territories show identical metrics across all columns, which warrants further investigation.

#### 2.2 Booking Channels Distribution
| Channel | Total Bookings | Percentage |
|---------|----------------|------------|
| BOOKED_FROM_APP | 6,320 | 45.88% |
| BOOKED_FROM_WEBSITE | 4,187 | 30.40% |
| BOOKED_FROM_CALL_CENTER | 2,878 | 20.89% |
| BOOKED_FROM_DRIVER_APP | 390 | 2.83% |

##### Interpretation:
- Mobile app is the preferred booking channel (45.88%)
- Digital channels (app + website) account for 76.28% of bookings
- Call center remains significant with 20.89%
- Driver app shows minimal usage at 2.83%

## Assumptions Made

1. Trip completion criteria:
   - Based on `passenger_status` and `status` fields
   - Only fully completed and validated trips are counted

2. User identification:
   - Users are unique within each product
   - Composite keys used for cross-product analysis

3. Data integrity:
   - Consistent timezone usage
   - No duplicate records in source data

## Technical Implementation Details

1. **Data Pipeline Structure**
   - Raw Layer: Direct views on parquet files
   - Prepared Layer: Cleaned and joined data models
   - Reporting Layer: Aggregated metrics and KPIs

2. **Performance Considerations**
   - Optimized view structure
   - Efficient join patterns
   - Proper indexing through DuckDB

## Future Recommendations

1. **Data Quality**
   - Investigate identical metrics across territories
   - Add data validation checks
   - Implement monitoring for key metrics

2. **Analysis Extensions**
   - Add time-based analysis
   - Implement user segmentation
   - Add geographic performance analysis 
# Analytics Engineering Technical Test Report

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

### 1. Stats on Operations

#### 1.1 Completed Trips per Country
| Country | Completed Trips |
|---------|----------------|
| France  | 6,126         |
| England | 5,598         |
| Germany | 2,051         |

#### 1.2 Average User Activity
| Country | Total Users | Avg Searches | Avg Bookings | Avg Completed Trips |
|---------|-------------|--------------|--------------|-------------------|
| France  | 3,567      | 5.89         | 2.70         | 1.61             |
| England | 8,748      | 4.53         | 1.04         | 0.64             |
| Germany | 5,510      | 1.13         | 0.43         | 0.37             |

## Additional Insights

1. **Conversion Patterns**
   - France shows the highest engagement and conversion rates
   - England has the largest user base but moderate conversion
   - Germany shows lower engagement across all metrics

2. **User Behavior**
   - French users are more likely to complete their bookings
   - English users show good initial interest but lower conversion
   - German market might need investigation for low engagement

## Assumptions Made

1. A trip is considered completed when:
   - `passenger_status = 'DROPOFF'`
   - `status = 'VALIDATED'`

2. User uniqueness:
   - Users are unique per product
   - Used combination of user_id and product_slug for unique identification

3. Data quality:
   - All timestamps are in the same timezone
   - No duplicate entries in raw data 

## Technical Implementation Details

1. **Data Pipeline Structure**
   - Raw Layer: Direct views on parquet files
   - Prepared Layer: Cleaned and joined data models
   - Reporting Layer: Aggregated metrics and KPIs

2. **Performance Considerations**
   - Used appropriate indexes through DuckDB
   - Minimized redundant joins
   - Implemented efficient aggregations

## Potential Improvements

With more time, I would:
1. Add data quality checks
2. Implement time-based analyses (daily/weekly trends)
3. Add booking channel analysis
4. Create territory-level conversion funnels
5. Add documentation directly in SQL using comments

## Data Quality Notes

1. **Completeness**
   - All countries have data representation
   - No missing values in key identifier fields

2. **Consistency**
   - Booking statuses are standardized
   - Territory mapping is consistent across products

## Future Recommendations

1. **Monitoring Suggestions**
   - Track conversion rates over time
   - Monitor booking completion rates by territory
   - Set up alerts for unusual drops in engagement

2. **Analysis Extensions**
   - Add peak hours analysis
   - Implement cohort analysis
   - Add seasonal trend analysis 
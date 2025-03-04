# Pursuit Data Integration Pipeline


### Prerequisites
- Apache Spark 3.x
- PostgreSQL 12+
- Python 3.7+ with PySpark
- PostgreSQL JDBC driver

### Setup and Execution
See the included setup instructions for detailed installation steps and execution guidelines.

## Example Queries

The implemented solution supports complex search patterns, including:
1. Finding entities where contacts have specific title keywords
2. Searching contacts with multiple criteria across related entities
3. Retrieving customer-specific entity mappings with associated data

## Future Considerations

The design includes clear paths for scaling as data volumes grow:
- Read/write separation strategies
- Potential for advanced search capabilities
- Table partitioning for larger datasets
- Cache optimization for frequently accessed data

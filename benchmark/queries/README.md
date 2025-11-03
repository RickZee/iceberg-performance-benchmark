# TPC-DS Queries for Performance Testing

This directory contains all 99 TPC-DS queries adapted for four different Snowflake table formats, designed for comprehensive performance testing and analysis.

As described in the [TPC Benchmark™ DS (TPC-DS) specificati](http://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DS_v2.5.0.pdf):
“In order to address the enormous range of query types and user behaviors encountered by a decision support system, TPC-DS utilizes a generalized query model. This model allows the benchmark to capture important aspects of the interactive, iterative nature of on-line analytical processing (OLAP) queries, the longer-running complex queries of data mining and knowledge discovery, and the more planned behavior of well known report queries.”

## 🗂️ Formats Supported

1. **native** - Standard Snowflake managed tables
2. **iceberg_sf** - Iceberg Snowflake-managed tables  
3. **iceberg_glue** - Iceberg AWS Glue-managed tables
4. **external** - External tables reading from S3

## 📊 Query Categories

The TPC-DS benchmark includes 99 queries categorized as follows:

### Reporting Queries (1-20)

- **Purpose**: Standard business reporting and dashboard queries
- **Characteristics**: Simple aggregations, groupings, basic filters
- **Performance Focus**: Fast execution for real-time dashboards
- **Examples**: Sales summaries, customer counts, product performance

### Ad-hoc Queries (21-40)

- **Purpose**: Business analysis and exploration queries
- **Characteristics**: Complex filtering, multiple joins, analytical functions
- **Performance Focus**: Balanced performance for analytical workloads
- **Examples**: Customer segmentation, product analysis, trend analysis

### Iterative Queries (41-60)

- **Purpose**: Multi-step analytical processes
- **Characteristics**: Complex business logic, subqueries, window functions
- **Performance Focus**: Optimized for analytical processing
- **Examples**: Cohort analysis, customer journey, complex reporting

### Data Mining Queries (61-99)

- **Purpose**: Advanced analytics and statistical analysis
- **Characteristics**: Complex joins, statistical functions, data mining operations
- **Performance Focus**: Optimized for large-scale analytical processing
- **Examples**: Predictive analytics, clustering, advanced statistics

## 🔧 Performance Considerations

### Query Complexity Levels

#### Low Complexity (1-25)

- Simple SELECT statements
- Basic WHERE clauses
- Single table operations
- Simple aggregations

#### Medium Complexity (26-50)

- Multiple table joins
- Complex WHERE clauses
- Subqueries
- Window functions

#### High Complexity (51-75)

- Multiple subqueries
- Complex analytical functions
- Advanced window functions
- Statistical operations

#### Very High Complexity (76-99)

- Complex business logic
- Advanced analytical functions
- Data mining operations
- Machine learning features

## 📋 Query Index

| Query | Description | Category | Complexity | Key Tables | Purpose |
|-------|-------------|----------|------------|------------|---------|
| q01 | Reporting query with simple aggregation | Reporting | Low | store_sales, store, date_dim | Sales analysis by store and state |
| q02 | Customer analysis with demographic filtering | Reporting | Low | store_sales, customer, customer_demographics | Customer segmentation analysis |
| q03 | Product performance analysis by category | Reporting | Low | store_sales, item, date_dim | Product category performance |
| q04 | Store performance comparison | Reporting | Low | store_sales, store, date_dim | Store performance metrics |
| q05 | Time-based sales trend analysis | Reporting | Low | store_sales, date_dim | Quarterly sales trends |
| q06 | Customer segmentation by purchase behavior | Reporting | Medium | store_sales, customer | Customer value segmentation |
| q07 | Product return analysis | Reporting | Medium | store_sales, store_returns, item | Return rate analysis |
| q08 | Geographic sales distribution | Reporting | Low | store_sales, store | Geographic sales analysis |
| q09 | Promotional campaign effectiveness | Reporting | Medium | store_sales, promotion | Campaign performance |
| q10 | Inventory turnover analysis | Reporting | Medium | store_sales, inventory, item | Inventory management |
| q11 | Customer lifetime value analysis | Ad-hoc | Medium | store_sales, customer, date_dim | Customer value analysis |
| q12 | Product cross-selling analysis | Ad-hoc | Medium | store_sales, item | Cross-selling opportunities |
| q13 | Seasonal sales pattern analysis | Ad-hoc | Medium | store_sales, date_dim | Seasonal trends |
| q14 | Customer acquisition analysis | Ad-hoc | Medium | store_sales, customer, date_dim | New customer analysis |
| q15 | Product profitability analysis | Ad-hoc | Medium | store_sales, item | Product profit margins |
| q16 | Store efficiency analysis | Ad-hoc | Medium | store_sales, store | Store performance metrics |
| q17 | Customer retention analysis | Ad-hoc | High | store_sales, customer, date_dim | Customer retention rates |
| q18 | Product lifecycle analysis | Ad-hoc | High | store_sales, item, date_dim | Product lifecycle trends |
| q19 | Market basket analysis | Ad-hoc | High | store_sales, item | Product association analysis |
| q20 | Customer journey analysis | Ad-hoc | High | store_sales, customer, date_dim | Customer behavior patterns |
| q21 | Advanced customer segmentation | Iterative | High | store_sales, customer, customer_demographics | Complex customer analysis |
| q22 | Product recommendation analysis | Iterative | High | store_sales, item, customer | Recommendation engine |
| q23 | Multi-dimensional sales analysis | Iterative | High | store_sales, store, item, date_dim | Complex sales analytics |
| q24 | Customer cohort analysis | Iterative | High | store_sales, customer, date_dim | Customer cohort trends |
| q25 | Product performance forecasting | Iterative | High | store_sales, item, date_dim | Predictive analytics |
| q26 | Store performance optimization | Iterative | High | store_sales, store, item | Store optimization |
| q27 | Customer churn prediction | Iterative | High | store_sales, customer, date_dim | Churn analysis |
| q28 | Product demand forecasting | Iterative | High | store_sales, item, date_dim | Demand prediction |
| q29 | Market share analysis | Iterative | High | store_sales, store, item | Market analysis |
| q30 | Customer satisfaction analysis | Iterative | High | store_sales, customer, store_returns | Satisfaction metrics |
| q31 | Advanced inventory optimization | Data Mining | Very High | store_sales, inventory, item | Inventory optimization |
| q32 | Customer behavior modeling | Data Mining | Very High | store_sales, customer, customer_demographics | Behavior modeling |
| q33 | Product clustering analysis | Data Mining | Very High | store_sales, item | Product clustering |
| q34 | Sales pattern recognition | Data Mining | Very High | store_sales, date_dim | Pattern analysis |
| q35 | Customer lifetime value prediction | Data Mining | Very High | store_sales, customer, date_dim | CLV prediction |
| q36 | Product recommendation engine | Data Mining | Very High | store_sales, item, customer | Recommendation system |
| q37 | Market basket optimization | Data Mining | Very High | store_sales, item | Basket optimization |
| q38 | Customer segmentation clustering | Data Mining | Very High | store_sales, customer, customer_demographics | Customer clustering |
| q39 | Product performance prediction | Data Mining | Very High | store_sales, item, date_dim | Performance prediction |
| q40 | Store performance optimization | Data Mining | Very High | store_sales, store, item | Store optimization |
| q41-q99 | Additional complex analytical queries | Various | High-Very High | Multiple tables | Advanced analytics |

### Format-Specific Optimizations

#### Native Snowflake Tables

- **Clustering**: Optimized for clustering keys
- **Search Optimization**: Enabled for better query performance
- **Materialized Views**: Can be used for complex aggregations

#### Iceberg Snowflake-Managed

- **Partition Pruning**: Optimized for date-based partitioning
- **File Format**: Parquet with Snappy compression
- **Metadata**: Snowflake-managed metadata for better performance

#### Iceberg AWS Glue-Managed

- **External Catalog**: AWS Glue catalog integration
- **Cross-Engine**: Compatible with multiple query engines
- **S3 Optimization**: Optimized for S3 storage patterns

#### External Tables

- **Read-Only**: Optimized for analytical workloads
- **Parquet Format**: Efficient columnar storage
- **S3 Integration**: Direct S3 access patterns

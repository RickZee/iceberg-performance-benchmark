# TPC-DS Performance Testing Application

A comprehensive performance testing suite for TPC-DS queries across all Snowflake table formats (Native, Iceberg Snowflake-managed, Iceberg AWS Glue-managed, and External tables).

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [TPC-DS Query Categories](#tpc-ds-query-categories)
- [Directory Structure](#directory-structure)
- [Quick Start](#quick-start)
- [Configuration Reference](#configuration-reference)
- [Usage Examples](#usage-examples)
- [Report Types](#report-types)
- [Performance Metrics](#performance-metrics)
- [Analytics Features](#analytics-features)
- [Command Line Interface](#command-line-interface)
- [Troubleshooting](#troubleshooting)

## Overview

This application provides automated performance testing, comprehensive metrics collection, advanced analytics, and detailed reporting for TPC-DS benchmark queries across different Snowflake table formats. It's designed to help evaluate and compare the performance characteristics of various data storage formats.

## Features

### Comprehensive Testing
- Automated execution of all 99 TPC-DS queries
- Support for all 4 Snowflake table formats
- Configurable test modes (quick, standard, comprehensive, stress)
- Query validation and safety checks

### Advanced Metrics Collection
- Real-time performance monitoring
- System resource usage tracking
- SQLite-based metrics storage
- Historical data analysis
- Export capabilities (JSON, CSV)

### Analytics & Insights
- Statistical analysis (ANOVA, t-tests, correlation analysis)
- Performance trend analysis
- Anomaly detection
- Format comparison and ranking
- Benchmark establishment

### Comprehensive Reporting
- HTML reports with interactive charts
- JSON reports for programmatic access
- CSV exports for data analysis
- PDF reports (optional)
- Summaries and recommendations

### Configuration Management
- YAML-based configuration
- Multiple test modes
- Performance thresholds
- Customizable reporting options

## TPC-DS Query Categories

The benchmark includes 99 TPC-DS queries organized into 4 categories:

- **Reporting Queries (1-20)**: Standard business reporting and dashboard queries with simple aggregations, groupings, and basic filters. Focus on fast execution for real-time dashboards.
- **Ad-hoc Queries (21-40)**: Business analysis and exploration queries with complex filtering, multiple joins, and analytical functions. Balanced performance for analytical workloads.
- **Iterative Queries (41-60)**: Multi-step analytical processes with complex business logic, subqueries, and window functions. Optimized for analytical processing.
- **Data Mining Queries (61-99)**: Advanced analytics and statistical analysis with complex joins, statistical functions, and data mining operations. Optimized for large-scale analytical processing.

Query files are located in the `queries/` directory, organized by format (native, iceberg_sf, iceberg_glue, external). Each format has 99 query files (q01.sql through q99.sql).

## Directory Structure

```
benchmark/
├── src/                    # Source code
│   ├── main.py            # Main application entry point
│   ├── metrics_collector.py # Performance metrics collection
│   ├── report_generator.py # Report generation
│   ├── analytics_engine.py # Advanced analytics
│   ├── cost_calculator.py # Cost calculation and analysis
│   ├── aws_cost_tracker.py # AWS cost tracking
│   └── ...                # Other modules
├── config/                # Configuration files
│   ├── perf_test_config.yaml
│   └── ...                # Other config files
├── queries/               # TPC-DS query files (99 per format)
│   ├── native/
│   ├── iceberg_sf/
│   ├── iceberg_glue/
│   └── external/
└── logs/                  # Application logs
```

## Quick Start

### 1. Prerequisites

Ensure you have the required dependencies installed:

```bash
pip install pandas matplotlib seaborn scipy numpy pyyaml psutil
```

### 2. Configuration

Copy and modify the configuration file:

```bash
cp benchmark/config/perf_test_config.yaml benchmark/config/my_config.yaml
```

### 3. Run Tests

#### Quick Test (5 queries per format)
```bash
python benchmark/src/main.py --test-mode
```

#### Standard Test (20 queries per format)
```bash
python benchmark/src/main.py --formats native iceberg_sf
```

#### Comprehensive Test (all 99 queries)
```bash
python benchmark/src/main.py
```

#### Custom Query Range (e.g., first 5 queries)
```bash
python benchmark/src/main.py --query-range 1 5
```


## Usage Examples

### Basic Performance Test

```python
from benchmark.src.main import TPCDSPerformanceTester

# Initialize tester
tester = TPCDSPerformanceTester("benchmark/config/perf_test_config.yaml")

# Run comprehensive test
results = tester.run_performance_test()

# Results contain detailed performance data for all formats
print(f"Tested {len(results)} formats")
```

### Custom Format Testing

```python
# Test only specific formats
results = tester.run_performance_test(formats=['native', 'iceberg_sf'])

# Test specific query range
results = tester.run_performance_test(query_range=(1, 20))
```

### Advanced Analytics

```python
from benchmark.src.analytics_engine import AnalyticsEngine

# Generate comprehensive analytics
analytics_engine = AnalyticsEngine(config, metrics_collector)
analytics = analytics_engine.generate_comprehensive_analytics(results)

# Access specific analyses
format_comparison = analytics['format_comparison']
statistical_analysis = analytics['statistical_analysis']
recommendations = analytics['recommendations']
```

### Metrics Collection

```python
from benchmark.src.metrics_collector import PerformanceMetricsCollector

# Initialize metrics collector
metrics_collector = PerformanceMetricsCollector(config, results_dir)

# Start system monitoring
metrics_collector.start_system_monitoring()

# Collect query metrics
metrics_collector.collect_query_metrics(query_result, session_id, test_run_id)

# Get performance summary
summary = metrics_collector.get_metrics_summary(format_name='native')
```

## Report Types

### HTML Reports
- Interactive performance charts
- Format comparison tables
- Query performance rankings
- Success rate analysis
- Summary

### JSON Reports
- Complete test results
- Performance analytics
- Statistical analysis
- Recommendations
- Configuration details

### CSV Reports
- Raw performance data
- Cost metrics (compute_credits, compute_cost_usd, storage_cost_usd, s3_storage_cost_usd, s3_request_cost_usd, glue_cost_usd, data_transfer_cost_usd, total_cost_usd)
- Warehouse size information
- Bytes scanned
- Suitable for external analysis
- Import into Excel/Tableau
- Time-series data

## Performance Metrics

### Execution Metrics
- Average execution time
- Minimum/maximum execution time
- Standard deviation
- Median execution time
- 95th/99th percentiles

### Success Metrics
- Success rate per format
- Error count and types
- Retry statistics
- Failure analysis

### System Metrics
- CPU usage
- Memory consumption
- Disk I/O
- Network I/O
- Resource utilization trends

### Cost Metrics
- **Compute Credits**: Snowflake compute credits consumed per query
- **Compute Cost**: Cost in USD based on warehouse size and execution time
- **Storage Cost**: Snowflake storage costs (native format)
- **S3 Storage Cost**: AWS S3 storage costs (external formats)
- **S3 Request Cost**: GET, PUT, LIST request costs
- **Glue Catalog Cost**: AWS Glue metadata storage and API call costs
- **Data Transfer Cost**: Cross-region data transfer costs
- **Total Cost**: Sum of all cost components
- **Cost per Query**: Average cost per query execution
- **Cost per Second**: Cost efficiency metric
- **Warehouse Size**: Warehouse size used for cost calculation

## Analytics Features

### Statistical Analysis
- ANOVA tests for format differences
- T-tests for pairwise comparisons
- Correlation analysis
- Distribution analysis
- Effect size calculations

### Performance Analysis
- Format performance ranking
- Query performance categorization
- Anomaly detection
- Trend analysis
- Benchmark establishment

### Insights Generation
- Key performance insights
- Improvement recommendations
- Optimization opportunities
- Best practice suggestions

## Command Line Interface

```bash
# Show help
python benchmark/src/main.py --help

# Test specific formats
python benchmark/src/main.py --formats native iceberg_sf

# Test query range (e.g., queries 1-5)
python benchmark/src/main.py --query-range 1 5

# Use custom config
python benchmark/src/main.py --config benchmark/config/my_config.yaml

# Quick test mode (5 queries per format)
python benchmark/src/main.py --test-mode
```

**Note**: The correct path is `benchmark/src/main.py` (not `tpcds_perf_test/src/main.py`).

## Configuration Reference

Edit `benchmark/config/perf_test_config.yaml` to customize test behavior.

### Test Modes

- **Quick Test**: 5 queries per format, 1 test run, no warmup runs
- **Standard Test**: 20 queries per format, 2 test runs, 1 warmup run
- **Comprehensive Test**: 99 queries per format, 3 test runs, 1 warmup run
- **Stress Test**: 99 queries per format, 5 test runs, 2 warmup runs, maximum load

### Test Settings

- `max_queries_per_format`: Maximum queries to test per format
- `max_execution_time`: Maximum execution time per query (seconds, default: 300)
- `retry_attempts`: Number of retry attempts for failed queries
- `warmup_runs`: Number of warmup runs before timing
- `test_runs`: Number of actual test runs for averaging

### Performance Thresholds

- `max_execution_time`: Maximum acceptable execution time (300 seconds)
- `memory_limit_mb`: Memory limit per query (1024 MB)
- `error_rate_threshold`: Maximum acceptable error rate (5%)
- `success_rate_threshold`: Minimum acceptable success rate (95%)

### Reporting Options

- `generate_html`: Generate HTML reports with interactive charts
- `generate_pdf`: Generate PDF reports (optional)
- `generate_csv`: Generate CSV data files
- `generate_json`: Generate JSON reports for programmatic access
- `include_charts`: Include performance charts in reports

### Query Complexity Levels

Queries are also categorized by technical complexity:

- **Low Complexity (1-25)**: Simple SELECT statements, basic WHERE clauses, single table operations, simple aggregations
- **Medium Complexity (26-50)**: Multiple table joins, complex WHERE clauses, subqueries, window functions
- **High Complexity (51-75)**: Multiple subqueries, complex analytical functions, advanced window functions, statistical operations
- **Very High Complexity (76-99)**: Complex business logic, advanced analytical functions, data mining operations, machine learning features

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


## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Verify Snowflake credentials
   - Check network connectivity
   - Ensure proper permissions

2. **Query Execution Failures**
   - Review query syntax
   - Check table existence
   - Verify schema permissions

3. **Memory Issues**
   - Adjust memory limits in config
   - Reduce concurrent queries
   - Monitor system resources

4. **Report Generation Errors**
   - Check disk space
   - Verify write permissions
   - Review log files

### Log Files

Application logs are stored in `benchmark/logs/`:
- `tpcds_perf_test.log`: Main application log
- Query execution details are logged to the main log file
- Metrics collection details are included in the main log

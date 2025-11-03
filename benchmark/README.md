# TPC-DS Performance Testing Application

A comprehensive performance testing suite for TPC-DS queries across all Snowflake table formats (Native, Iceberg Snowflake-managed, Iceberg AWS Glue-managed, and External tables).

## Overview

This application provides automated performance testing, comprehensive metrics collection, advanced analytics, and detailed reporting for TPC-DS benchmark queries across different Snowflake table formats. It's designed to help evaluate and compare the performance characteristics of various data storage formats.

## Features

### 🚀 **Comprehensive Testing**
- Automated execution of all 99 TPC-DS queries
- Support for all 4 Snowflake table formats
- Configurable test modes (quick, standard, comprehensive, stress)
- Query validation and safety checks

### 📊 **Advanced Metrics Collection**
- Real-time performance monitoring
- System resource usage tracking
- SQLite-based metrics storage
- Historical data analysis
- Export capabilities (JSON, CSV)

### 📈 **Analytics & Insights**
- Statistical analysis (ANOVA, t-tests, correlation analysis)
- Performance trend analysis
- Anomaly detection
- Format comparison and ranking
- Benchmark establishment

### 📋 **Comprehensive Reporting**
- HTML reports with interactive charts
- JSON reports for programmatic access
- CSV exports for data analysis
- PDF reports (optional)
- Executive summaries and recommendations

### ⚙️ **Configuration Management**
- YAML-based configuration
- Multiple test modes
- Performance thresholds
- Customizable reporting options

## Directory Structure

```
tpcds_perf_test/
├── src/                    # Source code
│   ├── main.py            # Main application entry point
│   ├── query_engine.py    # Query execution engine
│   ├── metrics_collector.py # Performance metrics collection
│   ├── report_generator.py # Report generation
│   └── analytics_engine.py # Advanced analytics
├── config/                # Configuration files
│   └── perf_test_config.yaml
├── results/               # Test results and data
├── reports/               # Generated reports
├── logs/                  # Application logs
└── examples/              # Usage examples
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
cp tpcds_perf_test/config/perf_test_config.yaml tpcds_perf_test/config/my_config.yaml
```

### 3. Run Tests

#### Quick Test (5 queries per format)
```bash
python tpcds_perf_test/src/main.py --test-mode
```

#### Standard Test (20 queries per format)
```bash
python tpcds_perf_test/src/main.py --formats native iceberg_sf
```

#### Comprehensive Test (all 99 queries)
```bash
python tpcds_perf_test/src/main.py
```

#### Custom Query Range
```bash
python tpcds_perf_test/src/main.py --query-range 1 10
```

## Configuration Options

### Test Settings
- `max_queries_per_format`: Maximum queries to test per format
- `max_execution_time`: Maximum execution time per query (seconds)
- `retry_attempts`: Number of retry attempts for failed queries
- `warmup_runs`: Number of warmup runs before timing
- `test_runs`: Number of actual test runs for averaging

### Performance Thresholds
- `max_execution_time`: Maximum acceptable execution time
- `memory_limit_mb`: Memory limit per query
- `error_rate_threshold`: Maximum acceptable error rate
- `success_rate_threshold`: Minimum acceptable success rate

### Reporting Options
- `generate_html`: Generate HTML reports
- `generate_pdf`: Generate PDF reports
- `generate_csv`: Generate CSV data files
- `generate_json`: Generate JSON reports
- `include_charts`: Include performance charts

## Usage Examples

### Basic Performance Test

```python
from tpcds_perf_test.src.main import TPCDSPerformanceTester

# Initialize tester
tester = TPCDSPerformanceTester("tpcds_perf_test/config/perf_test_config.yaml")

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
from tpcds_perf_test.src.analytics_engine import AnalyticsEngine

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
from tpcds_perf_test.src.metrics_collector import PerformanceMetricsCollector

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
- Executive summary

### JSON Reports
- Complete test results
- Performance analytics
- Statistical analysis
- Recommendations
- Configuration details

### CSV Reports
- Raw performance data
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
python tpcds_perf_test/src/main.py --help

# Test specific formats
python tpcds_perf_test/src/main.py --formats native iceberg_sf

# Test query range
python tpcds_perf_test/src/main.py --query-range 1 20

# Use custom config
python tpcds_perf_test/src/main.py --config my_config.yaml

# Quick test mode
python tpcds_perf_test/src/main.py --test-mode
```

## Configuration Reference

### Test Modes

#### Quick Test
- 5 queries per format
- 1 test run
- No warmup runs
- Fast execution

#### Standard Test
- 20 queries per format
- 2 test runs
- 1 warmup run
- Balanced testing

#### Comprehensive Test
- 99 queries per format
- 3 test runs
- 1 warmup run
- Complete analysis

#### Stress Test
- 99 queries per format
- 5 test runs
- 2 warmup runs
- No delays
- Maximum load

### Performance Thresholds

- **Max Execution Time**: 300 seconds
- **Memory Limit**: 1024 MB
- **Error Rate Threshold**: 5%
- **Success Rate Threshold**: 95%

### Query Categories

The application categorizes TPC-DS queries into groups for analysis:

- **Reporting Queries** (1-20): Medium complexity
- **Ad-hoc Queries** (21-40): High complexity
- **Iterative Queries** (41-60): Medium complexity
- **Data Analysis** (61-80): High complexity
- **Complex Analytics** (81-99): Very high complexity

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

Application logs are stored in `tpcds_perf_test/logs/`:
- `tpcds_perf_test.log`: Main application log
- `query_execution.log`: Query execution details
- `metrics_collection.log`: Metrics collection log

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is part of the event-based-processing/iceberg repository.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review log files
3. Create an issue in the repository
4. Contact the development team

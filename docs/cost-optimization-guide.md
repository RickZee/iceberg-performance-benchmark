# Cost Optimization Guide

## Overview

This guide provides comprehensive information on cost measurement, analysis, and optimization strategies for the Snowflake Iceberg Performance Benchmark across all 4 table formats.

## Table of Contents

1. [Cost Components](#cost-components)
2. [Cost Measurement Methodology](#cost-measurement-methodology)
3. [Cost Analysis](#cost-analysis)
4. [Optimization Strategies](#optimization-strategies)
5. [Best Practices](#best-practices)
6. [ROI Calculations](#roi-calculations)

## Cost Components

### 1. Snowflake Compute Costs

**Components:**

- Warehouse compute credits (based on warehouse size and execution time)
- Warehouse auto-suspend/resume overhead
- Query compilation and planning costs

**Calculation:**

```
Credits = (Warehouse Size Multiplier × Execution Time in Seconds) / 3600
Cost = Credits × Credit Price (default: $3/credit)
```

**Warehouse Size Multipliers:**

- X-Small: 1x
- Small: 2x
- Medium: 4x
- Large: 8x
- X-Large: 16x
- 2X-Large: 32x
- 3X-Large: 64x
- 4X-Large: 128x

**Optimization Opportunities:**

- Right-size warehouse based on query patterns
- Use auto-suspend to minimize idle costs
- Optimize query execution time to reduce credits consumed
- Consider result caching for repeated queries

### 2. Snowflake Storage Costs

**Components:**

- Native tables: Snowflake internal storage ($23-40/TB/month)
- Iceberg SF: External volume storage (S3 costs, but Snowflake may charge for metadata)
- Iceberg Glue: No Snowflake storage (external)
- External: No Snowflake storage (external)

**Calculation:**

```
Storage Cost = (Storage Size in TB) × Storage Price per TB × Months
```

**Optimization Opportunities:**

- Use external formats for large datasets to avoid Snowflake storage costs
- Implement data retention policies
- Archive old data to cheaper storage tiers

### 3. AWS S3 Storage Costs

**Components:**

- Standard storage: $0.023/GB/month
- Intelligent-Tiering: Variable pricing
- Lifecycle transitions: IA ($0.0125/GB/month), Glacier ($0.004/GB/month)
- S3 requests: GET ($0.0004/1000), PUT ($0.005/1000), LIST ($0.0005/1000)

**Calculation:**

```
Storage Cost = Storage (GB) × Storage Class Price × Months
Request Cost = (GET Requests / 1000) × $0.0004 + (PUT Requests / 1000) × $0.005 + (LIST Requests / 1000) × $0.0005
```

**Optimization Opportunities:**

- Use S3 lifecycle policies to transition old data to cheaper storage classes
- Optimize file sizes to reduce request counts
- Use Intelligent-Tiering for variable access patterns
- Minimize LIST operations by optimizing query patterns

### 4. AWS Glue Catalog Costs

**Components:**

- Storage: $1/TB/month for metadata
- API requests: GET operations ($0.0004/1000), CREATE/UPDATE ($0.001/1000)

**Calculation:**

```
Glue Cost = (Metadata Storage TB × $1) + (API Calls / 1000 × API Price)
```

**Optimization Opportunities:**

- Cache table metadata to minimize API calls
- Batch operations when possible
- Use table metadata caching strategies

### 5. Data Transfer Costs

**Components:**

- S3 to Snowflake data transfer (within same region: free, cross-region: $0.02/GB)
- S3 egress costs

**Optimization Opportunities:**

- Ensure Snowflake and S3 are in the same AWS region
- Minimize data transfer by optimizing query patterns
- Use partition pruning to reduce data scanned

## Cost Measurement Methodology

### Automated Cost Tracking

The benchmark framework automatically tracks costs through:

1. **Metrics Collector** (`benchmark/src/metrics_collector.py`):
   - Tracks warehouse size and execution time
   - Calculates compute credits consumed
   - Stores cost metrics in SQLite database

2. **Cost Calculator** (`benchmark/src/cost_calculator.py`):
   - Calculates costs for all components
   - Supports configurable pricing
   - Provides detailed cost breakdowns

3. **AWS Cost Tracker** (`benchmark/src/aws_cost_tracker.py`):
   - Tracks S3 usage via CloudWatch
   - Estimates S3 request counts
   - Monitors Glue API calls

### Cost Metrics Database

Cost metrics are stored in SQLite database with the following tables:

- **cost_metrics**: Per-query cost breakdowns
- **warehouse_metrics**: Warehouse utilization and credit consumption
- **storage_metrics**: Storage costs by format

### Cost Reports

Cost analysis is included in:

- JSON reports: Detailed cost breakdowns
- CSV reports: Cost data for analysis
- HTML reports: Visual cost comparisons and charts

## Cost Analysis

### Cost per Format

The framework calculates total costs for each format:

- Total cost per format
- Average cost per query
- Cost breakdown by component (compute, storage, S3, Glue)

### Cost Comparison

Compare costs across formats to identify:

- Cheapest format for specific workloads
- Most expensive format
- Potential cost savings

### Cost-Performance Ratio

Analyze cost-to-performance trade-offs:

- Cost per second of execution
- Cost per query
- Identify formats with best cost-performance balance

### Cost Optimization Opportunities

The framework identifies:

- High-cost queries that may benefit from optimization
- Warehouse size optimization opportunities
- Storage optimization recommendations
- Query optimization suggestions

## Optimization Strategies

### Format-Specific Optimizations

#### Native Format

- **Clustering Keys**: Use clustering keys on frequently queried columns
- **Result Caching**: Enable result caching for repeated queries
- **Warehouse Size**: Right-size warehouse based on workload
- **Materialized Views**: Consider materialized views for complex aggregations

#### Iceberg Snowflake-Managed

- **Partitioning**: Optimize Iceberg partitioning strategy
- **File Sizes**: Maintain optimal file sizes (128MB target)
- **Compaction**: Run periodic table compaction
- **Result Caching**: Use result caching where appropriate

#### Iceberg Glue-Managed

- **Glue API Calls**: Minimize Glue catalog API calls by caching metadata
- **S3 Requests**: Optimize S3 request patterns
- **Partitioning**: Use proper Iceberg partitioning
- **File Organization**: Optimize S3 file organization

#### External Format

- **S3 Requests**: Optimize S3 request patterns
- **Lifecycle Policies**: Use S3 lifecycle policies for cost optimization
- **Partitioning**: Use partition columns in WHERE clauses
- **Parquet Compression**: Optimize Parquet file compression

### Warehouse Optimization

1. **Right-Size Warehouses**:
   - Test different warehouse sizes
   - Balance query performance vs. cost
   - Use smaller warehouses for simple queries
   - Use larger warehouses only when needed

2. **Auto-Suspend Configuration**:
   - Set appropriate auto-suspend times
   - Minimize idle warehouse costs
   - Balance suspend/resume overhead

3. **Multi-Cluster**:
   - Enable for high concurrency workloads
   - Consider for production environments

### Storage Optimization

1. **S3 Lifecycle Policies**:
   - Transition old data to cheaper storage classes
   - Archive infrequently accessed data
   - Delete temporary data automatically

2. **Compression**:
   - Use appropriate Parquet compression (Snappy for balanced performance)
   - Optimize file sizes for better query performance

3. **Data Retention**:
   - Implement data retention policies
   - Archive historical data
   - Delete unnecessary data

### Query Optimization

1. **Reduce Data Scanning**:
   - Use partition columns in WHERE clauses
   - Avoid SELECT *
   - Add appropriate filters

2. **Result Caching**:
   - Enable result caching for repeated queries
   - Cache frequently accessed data

3. **Query Rewriting**:
   - Optimize join orders
   - Use CTEs for complex queries
   - Optimize window functions

## Best Practices

### Cost Monitoring

1. **Regular Cost Reviews**:
   - Review cost reports regularly
   - Monitor cost trends
   - Set up cost alerts

2. **Cost Thresholds**:
   - Configure cost thresholds in `perf_test_config.yaml`
   - Set alerts for high-cost queries
   - Monitor format-specific costs

3. **Cost Attribution**:
   - Track costs by format, query, and user
   - Use cost tags for better attribution
   - Analyze cost patterns

### Performance vs. Cost Trade-offs

1. **Evaluate Trade-offs**:
   - Consider performance requirements
   - Balance cost and performance
   - Test different configurations

2. **Cost-Performance Ratio**:
   - Analyze cost per query execution
   - Compare formats on cost-performance basis
   - Optimize for specific use cases

3. **ROI Analysis**:
   - Calculate ROI for optimizations
   - Prioritize high-impact optimizations
   - Measure improvement over time

## ROI Calculations

### Cost Savings Calculation

**Example: Warehouse Size Optimization**

```
Current: Medium warehouse (4x multiplier)
Optimized: Small warehouse (2x multiplier)
Execution Time: 100 seconds

Current Cost = (4 × 100) / 3600 × $3 = $0.333
Optimized Cost = (2 × 100) / 3600 × $3 = $0.167
Savings = $0.166 per query

For 1000 queries: $166 savings
```

### ROI for Optimizations

**ROI Formula:**

```
ROI = (Cost Savings - Optimization Cost) / Optimization Cost × 100%
```

**Optimization Costs:**

- Development time
- Testing time
- Infrastructure changes
- Maintenance overhead

### Break-Even Analysis

Calculate when optimizations pay for themselves:

```
Break-Even Point = Optimization Cost / Cost Savings per Query
```

## Configuration

### Cost Pricing Configuration

Configure pricing in `benchmark/config/perf_test_config.yaml`:

```yaml
cost_pricing:
  snowflake_credit_price_usd: 3.0
  snowflake_storage_price_per_tb_usd: 31.5
  aws_s3:
    storage_standard_per_gb_usd: 0.023
    request_get_per_1k_usd: 0.0004
  aws_glue:
    metadata_storage_per_tb_usd: 1.0
```

### Cost Optimization Settings

Enable optimization features:

```yaml
cost_optimization:
  enable_cost_tracking: true
  cost_threshold_per_query: 0.1
  warehouse_optimization:
    enabled: true
    test_multiple_sizes: false
```

## Cost Reports

### JSON Cost Reports

Cost data is included in JSON reports with:

- Cost per format
- Cost per query
- Cost breakdown by component
- Cost comparison across formats
- Optimization opportunities

### CSV Cost Reports

Cost data exported to CSV for analysis:

- Query-level cost data
- Format-level cost summaries
- Time-series cost data

### HTML Cost Reports

Interactive HTML reports include:

- Cost comparison charts
- Cost breakdown visualizations
- Cost trend analysis
- Optimization recommendations

## Implementation Examples

### Example 1: Warehouse Size Optimization

```python
from benchmark.src.warehouse_optimizer import WarehouseOptimizer

optimizer = WarehouseOptimizer(config)
results = optimizer.test_warehouse_sizes(
    query_execution_times=[0.5, 1.0, 2.0, 0.3],
    current_size='Medium'
)

print(f"Optimal size: {results['optimal_size']}")
print(f"Potential savings: ${results['potential_savings_usd']:.2f}")
```

### Example 2: Cost Analysis

```python
from benchmark.src.analytics_engine import AnalyticsEngine

analytics = AnalyticsEngine(config)
cost_analysis = analytics.analyze_costs(test_results)

print(f"Total cost: ${cost_analysis['total_costs']['total_cost_usd']:.2f}")
print(f"Cheapest format: {cost_analysis['cost_comparison']['cheapest_format']}")
```

### Example 3: Query Optimization

```python
from benchmark.src.query_optimizer import QueryOptimizer

optimizer = QueryOptimizer(config)
analysis = optimizer.analyze_query(
    query_content=query_sql,
    format_name='native',
    execution_time=5.0,
    bytes_scanned=1000000000
)

for recommendation in analysis['recommendations']:
    print(f"- {recommendation}")
```

## Benchmarking Results

### Cost Benchmarking

The framework enables cost benchmarking across:

- Different warehouse sizes
- Different table formats
- Different query patterns
- Different data sizes

### Cost Trends

Track cost trends over time:

- Monitor cost per query trends
- Identify cost increases
- Measure optimization effectiveness

## Troubleshooting

### High Costs

If costs are unexpectedly high:

1. Check warehouse size - may be too large
2. Review query execution times - may be slow
3. Analyze data scanning - may be scanning too much data
4. Check S3 request patterns - may have excessive requests

### Cost Tracking Issues

If cost tracking is not working:

1. Verify cost calculator is initialized
2. Check warehouse size is being captured
3. Verify AWS credentials for S3 tracking
4. Review logs for cost calculation errors

## Additional Resources

- [Snowflake Pricing](https://www.snowflake.com/pricing/)
- [AWS S3 Pricing](https://aws.amazon.com/s3/pricing/)
- [AWS Glue Pricing](https://aws.amazon.com/glue/pricing/)
- [Cost Optimization Best Practices](https://docs.snowflake.com/en/user-guide/cost-understanding.html)

## Summary

The cost optimization framework provides comprehensive cost tracking, analysis, and optimization recommendations. By following the strategies outlined in this guide, you can:

1. **Measure** costs accurately across all formats
2. **Analyze** cost patterns and identify opportunities
3. **Optimize** configurations to reduce costs
4. **Monitor** costs over time
5. **Calculate** ROI for optimizations

Regular cost reviews and optimization implementation can lead to significant cost savings while maintaining or improving performance.

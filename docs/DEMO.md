# Snowflake Iceberg Performance Benchmark - Demo Presentation Script

## Introduction

Welcome to the **Snowflake Iceberg Performance Testing Suite** demonstration. This comprehensive framework enables you to compare the performance, cost, and characteristics of four different Snowflake table formats using the industry-standard TPC-DS benchmark.

---

## Background

### What is This Project?

This project is a comprehensive performance testing framework designed to evaluate and compare four different Snowflake table storage formats:

1. **Native Snowflake Tables** - Standard Snowflake managed tables with full Snowflake optimization
2. **Iceberg Snowflake-Managed** - Apache Iceberg format with Snowflake's native catalog integration
3. **Iceberg AWS Glue-Managed** - Apache Iceberg format with external AWS Glue catalog for cross-platform compatibility
4. **External Tables** - External tables pointing directly to Parquet files in S3

### Why Does This Matter?

As organizations modernize their data architectures, they face critical decisions about data storage formats:

- **Performance**: Which format delivers the best query performance for your workload?
- **Cost**: What are the total cost implications including compute, storage, and cloud services?
- **Compatibility**: Do you need cross-platform compatibility or vendor lock-in?
- **Features**: Which format supports the features you need (ACID transactions, time travel, versioning)?

This benchmark provides **data-driven insights** to make informed architectural decisions.

### TPC-DS Benchmark Context

TPC-DS (Transaction Processing Performance Council Decision Support) is the industry-standard benchmark for decision support systems. It includes:

- **99 standardized queries** covering real-world analytical workloads
- **24 tables** (17 dimension tables, 7 fact tables) representing a retail data warehouse
- **4 query categories**:
  - **Reporting Queries (1-20)**: Standard business reporting and dashboards
  - **Ad-hoc Queries (21-40)**: Business analysis and exploration
  - **Iterative Queries (41-60)**: Multi-step analytical processes
  - **Data Mining Queries (61-99)**: Advanced analytics and statistical analysis

The benchmark provides a realistic, comprehensive workload that mirrors production analytical environments.

### Use Cases and Target Audience

**Primary Use Cases:**
- **Architecture Evaluation**: Compare table formats before making architectural decisions
- **Performance Optimization**: Identify performance bottlenecks and optimization opportunities
- **Cost Analysis**: Understand total cost of ownership across different formats
- **Migration Planning**: Evaluate migration from one format to another
- **Capacity Planning**: Understand resource requirements for different formats

**Target Audience:**
- Data architects and engineers
- Performance engineers
- Cost optimization teams
- Technical decision makers
- Snowflake administrators

### Why This Comparison Matters

Each table format has distinct characteristics:

| Format | Strengths | Use Cases |
|--------|-----------|-----------|
| **Native** | Best performance, full Snowflake features | Snowflake-only environments, maximum performance |
| **Iceberg SF** | ACID transactions, time travel, versioning | Data lakes with Snowflake, version control needs |
| **Iceberg Glue** | Cross-platform compatibility, AWS ecosystem | Multi-engine environments, AWS-native stacks |
| **External** | Cost-effective, direct S3 access | Read-heavy workloads, cost optimization |

This benchmark helps you understand the **real-world trade-offs** between these formats.

---

## Prerequisites

### Required Software

1. **Python 3.8 or higher**
   ```bash
   python --version  # Should show 3.8+
   ```

2. **Python Dependencies**
   ```bash
   pip install -r requirements.txt
   ```
   
   Key dependencies include:
   - `snowflake-connector-python` - Snowflake connectivity
   - `pandas`, `numpy` - Data processing
   - `matplotlib`, `seaborn` - Visualization
   - `boto3` - AWS integration
   - `pyyaml` - Configuration management

3. **Terraform** (for AWS infrastructure setup)
   ```bash
   terraform --version
   ```

4. **Spark** (optional, for Glue table creation if using Iceberg Glue format)

### Required Accounts and Access

1. **Snowflake Account**
   - ACCOUNTADMIN role or equivalent permissions
   - Warehouse created and running
   - Permissions for:
     - External volume creation
     - Catalog integration creation
     - Database and schema creation

2. **AWS Account** (for Iceberg Glue and External formats)
   - S3 bucket access
   - AWS Glue catalog access (for Iceberg Glue format)
   - IAM roles configured for Snowflake integration
   - Appropriate region (recommended: same region as Snowflake)

### Environment Configuration

1. **Environment Variables**
   ```bash
   cp env.example .env
   # Edit .env with your credentials
   ```

   Required variables:
   - `SNOWFLAKE_USER` - Your Snowflake username
   - `SNOWFLAKE_ACCOUNT` - Your Snowflake account identifier
   - `SNOWFLAKE_WAREHOUSE` - Warehouse name
   - `SNOWFLAKE_DATABASE` - Database name (default: `tpcds_performance_test`)
   - `SNOWFLAKE_ROLE` - Role (typically `ACCOUNTADMIN`)
   - `SNOWFLAKE_PRIVATE_KEY_FILE` - Path to RSA private key
   - `AWS_REGION` - AWS region
   - `AWS_S3_BUCKET` - S3 bucket name
   - `AWS_GLUE_DATABASE` - Glue database name (if using Iceberg Glue)

2. **Load Environment Variables**
   ```bash
   set -a && source .env && set +a
   ```

### Data Requirements

The benchmark requires TPC-DS test data. You have two options:

1. **Use Existing Data**: If data is already loaded in Snowflake
2. **Generate and Load Data**: Run the data loading pipeline
   ```bash
   cd setup/data/
   python main.py --action full --scale-factor 0.1  # For quick demo (100MB)
   ```

   Scale factors:
   - `0.01` - ~10MB (quick testing)
   - `0.1` - ~100MB (development/demo)
   - `1.0` - ~1GB (standard testing)
   - `10.0` - ~10GB (performance testing)

### Infrastructure Setup

Before running benchmarks, ensure infrastructure is set up:

1. **AWS Infrastructure** (if using Iceberg/External formats)
   ```bash
   cd setup/infrastructure/
   terraform init
   terraform apply
   ```

2. **Snowflake Schemas**
   - Execute SQL scripts in `setup/schemas/`
   - Or use the master script: `@setup/schemas/create_tpcds_objects.sql`

3. **Glue Tables** (only for Iceberg Glue format)
   ```bash
   cd setup/glue/
   python scripts/create/create_glue_tables.py
   ```

4. **Data Loading**
   ```bash
   cd setup/data/
   python main.py --action full --scale-factor 0.1
   ```

**Note**: For a quick demo, you can skip full infrastructure setup and use test mode with existing data.

---

## How to Run

### Quick Start - Test Mode

For a demonstration or quick validation, use test mode which runs a limited set of queries:

```bash
# Load environment variables
set -a && source .env && set +a

# Run quick test (5 queries per format)
python benchmark/src/main.py --test-mode
```

**Expected Execution Time**: 5-15 minutes (depending on warehouse size and data volume)

**What Test Mode Does**:
- Runs 5 representative queries per format
- Executes 1 warmup run + 1 test run per query
- Generates all standard reports
- Provides quick performance comparison

### Standard Test - All Queries

For comprehensive benchmarking:

```bash
# Run all 99 queries across all 4 formats
python benchmark/src/main.py
```

**Expected Execution Time**: 2-6 hours (depending on warehouse size, data volume, and query complexity)

**What Standard Test Does**:
- Runs all 99 TPC-DS queries per format
- Executes 1 warmup run + 3 test runs per query (for statistical validity)
- Generates comprehensive reports
- Provides detailed performance analysis

### Custom Test Options

#### Test Specific Formats

```bash
# Test only Native and Iceberg Snowflake formats
python benchmark/src/main.py --formats native iceberg_sf
```

Available formats: `native`, `iceberg_sf`, `iceberg_glue`, `external`

#### Test Query Range

```bash
# Test queries 1-20 (Reporting queries)
python benchmark/src/main.py --query-range 1 20
```

#### Use Custom Configuration

```bash
# Use custom configuration file
python benchmark/src/main.py --config benchmark/config/my_config.yaml
```

### Command-Line Options Summary

```bash
python benchmark/src/main.py [OPTIONS]

Options:
  --test-mode              Run in test mode (limited queries, faster)
  --formats FORMATS        Test specific formats (space-separated)
  --query-range START END  Test specific query range
  --config PATH            Use custom configuration file
  --help                   Show help message
```

### Configuration Customization

Edit `benchmark/config/perf_test_config.yaml` to customize:

- **Test Modes**: Quick, standard, comprehensive, stress
- **Performance Thresholds**: Max execution time, memory limits
- **Reporting Options**: HTML, JSON, CSV, PDF generation
- **Query Settings**: Timeouts, caching, safety limits
- **Cost Pricing**: Snowflake credits, AWS pricing

### Execution Flow

When you run the benchmark, here's what happens:

1. **Initialization**
   - Loads configuration
   - Connects to Snowflake
   - Validates environment

2. **Query Execution** (per format)
   - For each query:
     - Warmup run(s) to prime caches
     - Test run(s) with metrics collection
     - Error handling and retries
     - Metrics storage

3. **Metrics Collection**
   - Execution time
   - Data scanned (bytes)
   - Rows produced
   - Cache hit ratio
   - Credits used
   - Memory usage
   - Cost calculations

4. **Analysis and Reporting**
   - Statistical analysis
   - Format comparison
   - Cost analysis
   - Report generation

### Monitoring Progress

The benchmark provides real-time progress updates:

```
[INFO] Starting performance test...
[INFO] Testing format: native
[INFO] Executing query 1/99: q01.sql
[INFO] Query 1 completed in 2.3s
[INFO] Executing query 2/99: q02.sql
...
```

Logs are saved to `benchmark/logs/tpcds_perf_test.log` for detailed tracking.

---

## Understanding Results

### Output Locations

All results are saved to the `results/` directory:

```
results/
├── performance_data/          # Raw performance metrics
│   └── tpcds_performance_data_TIMESTAMP.csv
├── reports/                   # Generated reports
│   └── tpcds_performance_report_TIMESTAMP.html
├── tpcds_performance_report_TIMESTAMP.json
└── tpcds_analytics_TIMESTAMP.json
```

### Report Types

#### 1. HTML Reports (Interactive)

**Location**: `results/reports/tpcds_performance_report_TIMESTAMP.html`

**Features**:
- Interactive performance charts
- Format comparison tables
- Query performance rankings
- Success rate analysis
- Cost breakdowns
- Visual format comparisons

**How to View**:
```bash
# Open in browser
open results/reports/tpcds_performance_report_*.html
```

**Key Sections**:
- **Executive Summary**: High-level performance comparison
- **Format Comparison**: Side-by-side performance metrics
- **Query Performance**: Individual query results
- **Cost Analysis**: Cost breakdown by format
- **Recommendations**: Optimization suggestions

#### 2. JSON Reports (Machine-Readable)

**Location**: `results/tpcds_performance_report_TIMESTAMP.json`

**Contents**:
- Complete test results
- Performance analytics
- Statistical analysis
- Configuration details
- Raw metrics data

**Use Cases**:
- Programmatic analysis
- Integration with other tools
- Custom reporting
- Data pipeline integration

#### 3. CSV Reports (Data Analysis)

**Location**: `results/tpcds_performance_data_TIMESTAMP.csv`

**Columns Include**:
- Query number and format
- Execution time
- Data scanned (bytes)
- Rows produced
- Credits used
- Cost breakdown (compute, storage, S3, Glue, total)
- Warehouse size
- Success status

**Use Cases**:
- Excel/Tableau analysis
- Custom visualizations
- Statistical analysis
- Trend analysis

### Key Metrics Explained

#### Performance Metrics

1. **Execution Time** (seconds)
   - Time to complete query execution
   - Lower is better
   - Includes query compilation and execution

2. **Data Scanned** (bytes)
   - Amount of data read from storage
   - Indicates query efficiency
   - Lower is better (with same results)

3. **Rows Produced**
   - Number of result rows
   - Validates query correctness
   - Should match across formats

4. **Cache Hit Ratio** (%)
   - Percentage of data served from cache
   - Higher is better
   - Indicates query pattern efficiency

#### Cost Metrics

1. **Compute Credits**
   - Snowflake compute credits consumed
   - Based on warehouse size × execution time
   - Formula: `(Warehouse Multiplier × Seconds) / 3600`

2. **Compute Cost** (USD)
   - Cost based on credits × credit price
   - Default: $3 per credit

3. **Storage Cost** (USD)
   - Snowflake storage costs (Native format)
   - S3 storage costs (External formats)
   - Based on data volume and storage class

4. **Total Cost** (USD)
   - Sum of all cost components
   - Includes compute, storage, S3 requests, Glue API calls, data transfer

#### Warehouse Metrics

- **Warehouse Size**: X-Small (1x) through 4X-Large (128x)
- **Warehouse Multiplier**: Credit consumption multiplier
- **Auto-suspend Time**: Idle time before suspension

### Format Comparison Insights

The reports provide several comparison views:

1. **Overall Performance Ranking**
   - Which format is fastest overall
   - Performance by query category
   - Consistency across query types

2. **Cost Comparison**
   - Total cost per format
   - Cost per query
   - Cost-performance ratio

3. **Query-Specific Analysis**
   - Which format performs best for specific queries
   - Query patterns that favor certain formats
   - Optimization opportunities

4. **Statistical Analysis**
   - ANOVA tests for format differences
   - Confidence intervals
   - Effect sizes

### Interpreting Results

#### Performance Winners

Look for:
- **Fastest Average Execution Time**: Best overall performance
- **Most Consistent Performance**: Lowest standard deviation
- **Best Cache Utilization**: Highest cache hit ratios
- **Best for Specific Query Types**: Format-specific strengths

#### Cost Winners

Look for:
- **Lowest Total Cost**: Most cost-effective format
- **Best Cost-Performance Ratio**: Best value
- **Lowest Compute Costs**: Most efficient warehouse usage
- **Lowest Storage Costs**: Most efficient storage utilization

#### Trade-offs

Consider:
- **Performance vs. Cost**: Faster formats may cost more
- **Features vs. Performance**: More features may impact performance
- **Compatibility vs. Optimization**: Cross-platform may sacrifice optimization

### Example Results Interpretation

**Scenario**: Native format is 20% faster but 30% more expensive than External format.

**Insights**:
- Native provides better performance for time-sensitive workloads
- External provides better cost efficiency for batch workloads
- Decision depends on performance requirements vs. budget constraints

**Recommendation**: Use Native for real-time dashboards, External for batch reporting.

---

## Significance

### Business Value Proposition

This benchmark provides **quantifiable, data-driven insights** for critical architectural decisions:

1. **Risk Mitigation**
   - Make informed decisions before committing to a format
   - Avoid costly migrations due to poor initial choices
   - Understand trade-offs before implementation

2. **Cost Optimization**
   - Identify cost savings opportunities
   - Optimize warehouse sizing
   - Reduce total cost of ownership

3. **Performance Optimization**
   - Identify performance bottlenecks
   - Optimize query patterns
   - Right-size infrastructure

4. **Strategic Planning**
   - Plan migrations with confidence
   - Evaluate new technologies
   - Support business case development

### Technical Insights Provided

The benchmark delivers comprehensive technical analysis:

1. **Performance Characteristics**
   - Query execution patterns
   - Format-specific optimizations
   - Cache behavior
   - Resource utilization

2. **Scalability Analysis**
   - Performance at different data volumes
   - Warehouse size optimization
   - Concurrent query handling

3. **Cost Breakdown**
   - Component-level cost analysis
   - Cost drivers identification
   - Optimization opportunities

4. **Statistical Validity**
   - Multiple test runs for reliability
   - Confidence intervals
   - Statistical significance testing

### Cost Optimization Opportunities

The benchmark identifies specific optimization opportunities:

1. **Warehouse Sizing**
   - Right-size warehouses based on workload
   - Balance performance vs. cost
   - Optimize auto-suspend settings

2. **Query Optimization**
   - Identify slow queries
   - Optimize data scanning
   - Improve cache utilization

3. **Format Selection**
   - Choose format based on workload
   - Optimize for cost vs. performance
   - Consider feature requirements

4. **Storage Optimization**
   - Optimize S3 storage classes
   - Implement lifecycle policies
   - Reduce storage costs

### Decision-Making Support

The benchmark provides actionable insights for:

1. **Architecture Decisions**
   - Which format to use for new projects
   - Migration planning
   - Hybrid architecture design

2. **Performance Tuning**
   - Query optimization priorities
   - Infrastructure sizing
   - Caching strategies

3. **Cost Management**
   - Budget planning
   - Cost allocation
   - ROI calculations

4. **Capacity Planning**
   - Resource requirements
   - Scaling strategies
   - Performance projections

### Real-World Applications

#### Use Case 1: New Data Lake Architecture

**Scenario**: Building a new data lake and deciding between formats.

**Value**: Benchmark provides performance and cost data to make informed decision.

**Outcome**: Choose format that balances performance, cost, and feature requirements.

#### Use Case 2: Migration Planning

**Scenario**: Migrating from Native to Iceberg format.

**Value**: Understand performance impact and cost implications before migration.

**Outcome**: Plan migration with confidence, optimize for target format.

#### Use Case 3: Cost Optimization

**Scenario**: Reducing Snowflake costs.

**Value**: Identify cost drivers and optimization opportunities.

**Outcome**: Reduce costs by 20-40% through format optimization and warehouse sizing.

#### Use Case 4: Performance Troubleshooting

**Scenario**: Queries running slower than expected.

**Value**: Compare performance across formats to identify bottlenecks.

**Outcome**: Optimize queries and infrastructure based on benchmark insights.

### ROI Considerations

**Investment**:
- Setup time: 2-4 hours (one-time)
- Test execution: 2-6 hours (per test run)
- Analysis time: 1-2 hours

**Returns**:
- **Cost Savings**: 20-40% reduction in cloud costs
- **Performance Gains**: 10-30% improvement in query performance
- **Risk Reduction**: Avoid costly architectural mistakes
- **Time Savings**: Faster decision-making with data-driven insights

**Break-Even**: Typically achieved within first month of implementation.

### Competitive Advantages

Organizations using this benchmark gain:

1. **Data-Driven Decisions**: Make decisions based on data, not assumptions
2. **Cost Efficiency**: Optimize costs while maintaining performance
3. **Performance Excellence**: Achieve best-in-class query performance
4. **Strategic Flexibility**: Understand trade-offs for future planning

---

## Next Steps

### Immediate Actions

1. **Run Your First Benchmark**
   ```bash
   # Quick test to validate setup
   python benchmark/src/main.py --test-mode
   ```

2. **Review Results**
   - Open HTML report in browser
   - Review format comparisons
   - Identify key insights

3. **Analyze Your Workload**
   - Map your queries to TPC-DS query categories
   - Identify which formats perform best for your patterns
   - Consider cost implications

### Going Beyond the Demo

1. **Full Benchmark Suite**
   - Run comprehensive test with all 99 queries
   - Test multiple scale factors
   - Compare different warehouse sizes

2. **Custom Analysis**
   - Export CSV data for custom analysis
   - Create custom visualizations
   - Integrate with your analytics tools

3. **Continuous Monitoring**
   - Run benchmarks regularly
   - Track performance trends
   - Monitor cost changes

4. **Optimization Implementation**
   - Implement format recommendations
   - Optimize warehouse sizing
   - Tune queries based on insights

### Advanced Usage

1. **Custom Query Testing**
   - Add your own queries to the benchmark
   - Test format-specific optimizations
   - Validate performance improvements

2. **Multi-Scale Factor Analysis**
   - Test at different data volumes
   - Understand scalability characteristics
   - Plan for growth

3. **Cost Modeling**
   - Model costs for different scenarios
   - Plan budgets based on benchmark data
   - Calculate ROI for optimizations

### Resources and Documentation

- **Main README**: `README.md` - Project overview
- **Setup Guide**: `setup/README.md` - Detailed setup instructions
- **Benchmark Guide**: `benchmark/README.md` - Testing framework documentation
- **Cost Optimization**: `docs/cost-optimization-guide.md` - Cost analysis guide
- **Troubleshooting**: `docs/troubleshooting-journey.md` - Common issues and solutions

### Getting Help

- Review troubleshooting guides in `docs/`
- Check logs in `benchmark/logs/` and `results/logs/`
- Verify environment configuration
- Test Snowflake connectivity separately

### Contributing

If you find improvements or have suggestions:
- Report issues
- Contribute optimizations
- Share benchmark results (anonymized)
- Improve documentation

---

## Summary

The Snowflake Iceberg Performance Benchmark provides a comprehensive, data-driven approach to evaluating table storage formats. By running this benchmark, you gain:

- **Quantifiable Performance Data**: Real metrics on execution time, throughput, and efficiency
- **Cost Insights**: Detailed cost breakdowns and optimization opportunities
- **Decision Support**: Data-driven recommendations for architectural choices
- **Risk Mitigation**: Understand trade-offs before committing to a format

Whether you're evaluating formats for a new project, planning a migration, or optimizing costs, this benchmark provides the insights you need to make informed decisions.

**Start your benchmark today and make data-driven architectural decisions!**


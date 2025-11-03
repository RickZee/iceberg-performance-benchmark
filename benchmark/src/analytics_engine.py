#!/usr/bin/env python3
"""
Analytics Engine for TPC-DS Performance Testing
Advanced analytics and insights generation
"""

import logging
import statistics
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from datetime import datetime, timedelta
import json
import pandas as pd
from scipy import stats
from collections import defaultdict

logger = logging.getLogger(__name__)

class AnalyticsEngine:
    """Advanced analytics engine for performance data"""
    
    def __init__(self, config: Dict[str, Any], metrics_collector=None):
        """Initialize analytics engine"""
        self.config = config
        self.metrics_collector = metrics_collector
        self.analytics_cache = {}
        
    def generate_comprehensive_analytics(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive analytics"""
        
        analytics = {
            'executive_summary': self._generate_executive_summary(test_results),
            'format_comparison': self._analyze_format_performance(test_results),
            'query_analysis': self._analyze_query_performance(test_results),
            'query_category_breakdown': self._analyze_query_categories(test_results),
            'detailed_query_comparison': self._generate_detailed_query_comparison(test_results),
            'statistical_analysis': self._perform_statistical_analysis(test_results),
            'performance_trends': self._analyze_performance_trends(test_results),
            'anomaly_detection': self._detect_anomalies(test_results),
            'correlation_analysis': self._analyze_correlations(test_results),
            'recommendations': self._generate_analytics_recommendations(test_results),
            'benchmarking': self._generate_benchmark_analysis(test_results)
        }
        
        return analytics
    
    def _generate_executive_summary(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate executive summary"""
        
        total_queries = sum(len(format_results) for format_results in test_results.values())
        total_successful = sum(
            sum(1 for result in format_results if result['status'] == 'success')
            for format_results in test_results.values()
        )
        
        all_times = []
        format_performance = {}
        
        for format_name, format_results in test_results.items():
            successful_results = [r for r in format_results if r['status'] == 'success']
            
            if successful_results:
                format_times = [r['average_time'] for r in successful_results]
                all_times.extend(format_times)
                
                format_performance[format_name] = {
                    'queries_tested': len(format_results),
                    'successful_queries': len(successful_results),
                    'success_rate': len(successful_results) / len(format_results),
                    'avg_execution_time': statistics.mean(format_times),
                    'median_execution_time': statistics.median(format_times),
                    'p95_execution_time': np.percentile(format_times, 95),
                    'p99_execution_time': np.percentile(format_times, 99),
                    'std_execution_time': statistics.stdev(format_times) if len(format_times) > 1 else 0
                }
        
        # Calculate overall statistics
        overall_stats = {
            'total_queries': total_queries,
            'successful_queries': total_successful,
            'overall_success_rate': total_successful / total_queries if total_queries > 0 else 0,
            'avg_execution_time': statistics.mean(all_times) if all_times else 0,
            'median_execution_time': statistics.median(all_times) if all_times else 0,
            'p95_execution_time': np.percentile(all_times, 95) if all_times else 0,
            'p99_execution_time': np.percentile(all_times, 99) if all_times else 0,
            'std_execution_time': statistics.stdev(all_times) if len(all_times) > 1 else 0
        }
        
        # Identify best and worst performing formats
        if format_performance:
            best_format = min(format_performance.items(), key=lambda x: x[1]['avg_execution_time'])
            worst_format = max(format_performance.items(), key=lambda x: x[1]['avg_execution_time'])
            
            performance_gap = worst_format[1]['avg_execution_time'] / best_format[1]['avg_execution_time']
        else:
            best_format = worst_format = None
            performance_gap = 0
        
        return {
            'overall_statistics': overall_stats,
            'format_performance': format_performance,
            'best_performing_format': best_format,
            'worst_performing_format': worst_format,
            'performance_gap_ratio': performance_gap,
            'key_insights': self._extract_key_insights(test_results, format_performance)
        }
    
    def _analyze_format_performance(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze performance differences between formats"""
        
        format_analysis = {}
        
        # Collect data for each format
        format_data = {}
        for format_name, format_results in test_results.items():
            successful_results = [r for r in format_results if r['status'] == 'success']
            
            if successful_results:
                times = [r['average_time'] for r in successful_results]
                rows = [r.get('average_rows', 0) for r in successful_results]
                
                format_data[format_name] = {
                    'execution_times': times,
                    'result_rows': rows,
                    'success_rate': len(successful_results) / len(format_results),
                    'total_queries': len(format_results)
                }
        
        # Statistical comparison
        if len(format_data) >= 2:
            format_names = list(format_data.keys())
            
            # Pairwise comparisons
            pairwise_comparisons = {}
            for i, format1 in enumerate(format_names):
                for format2 in format_names[i+1:]:
                    times1 = format_data[format1]['execution_times']
                    times2 = format_data[format2]['execution_times']
                    
                    if len(times1) > 1 and len(times2) > 1:
                        # Perform t-test
                        t_stat, p_value = stats.ttest_ind(times1, times2)
                        
                        # Calculate effect size (Cohen's d)
                        pooled_std = np.sqrt(((len(times1) - 1) * np.var(times1, ddof=1) + 
                                            (len(times2) - 1) * np.var(times2, ddof=1)) / 
                                           (len(times1) + len(times2) - 2))
                        cohens_d = (np.mean(times1) - np.mean(times2)) / pooled_std if pooled_std > 0 else 0
                        
                        pairwise_comparisons[f"{format1}_vs_{format2}"] = {
                            't_statistic': t_stat,
                            'p_value': p_value,
                            'cohens_d': cohens_d,
                            'mean_difference': np.mean(times1) - np.mean(times2),
                            'significant': p_value < 0.05,
                            'effect_size': 'large' if abs(cohens_d) > 0.8 else 'medium' if abs(cohens_d) > 0.5 else 'small'
                        }
        
        # Performance ranking
        performance_ranking = sorted(format_data.items(), 
                                   key=lambda x: x[1]['success_rate'] * (1 / (np.mean(x[1]['execution_times']) + 1)))
        
        format_analysis = {
            'format_data': format_data,
            'pairwise_comparisons': pairwise_comparisons,
            'performance_ranking': performance_ranking,
            'consistency_analysis': self._analyze_performance_consistency(format_data)
        }
        
        return format_analysis
    
    def _analyze_query_performance(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze individual query performance"""
        
        query_analysis = {}
        
        # Collect query data across formats
        query_data = defaultdict(list)
        
        for format_name, format_results in test_results.items():
            for result in format_results:
                query_num = result['query_number']
                if result['status'] == 'success':
                    query_data[query_num].append({
                        'format': format_name,
                        'execution_time': result['average_time'],
                        'result_rows': result.get('average_rows', 0),
                        'std_time': result.get('std_time', 0)
                    })
        
        # Analyze each query
        for query_num, results in query_data.items():
            if len(results) > 1:  # Need at least 2 formats for comparison
                times = [r['execution_time'] for r in results]
                formats = [r['format'] for r in results]
                
                # Find best and worst performing formats for this query
                best_idx = np.argmin(times)
                worst_idx = np.argmax(times)
                
                query_analysis[f"query_{query_num}"] = {
                    'best_format': formats[best_idx],
                    'worst_format': formats[worst_idx],
                    'best_time': times[best_idx],
                    'worst_time': times[worst_idx],
                    'performance_variance': np.var(times),
                    'performance_range': max(times) - min(times),
                    'coefficient_of_variation': np.std(times) / np.mean(times) if np.mean(times) > 0 else 0,
                    'format_times': dict(zip(formats, times))
                }
        
        # Identify problematic queries
        problematic_queries = []
        for query_num, analysis in query_analysis.items():
            if analysis['coefficient_of_variation'] > 0.5:  # High variance
                problematic_queries.append({
                    'query_number': query_num,
                    'issue': 'high_variance',
                    'cv': analysis['coefficient_of_variation'],
                    'range': analysis['performance_range']
                })
            
            if analysis['performance_range'] > 10:  # Large time difference
                problematic_queries.append({
                    'query_number': query_num,
                    'issue': 'large_time_difference',
                    'range': analysis['performance_range'],
                    'best_format': analysis['best_format'],
                    'worst_format': analysis['worst_format']
                })
        
        return {
            'query_analysis': query_analysis,
            'problematic_queries': problematic_queries,
            'query_categorization': self._categorize_queries_by_performance(query_analysis)
        }
    
    def _perform_statistical_analysis(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Perform advanced statistical analysis"""
        
        # Collect all execution times
        all_times = []
        format_times = defaultdict(list)
        
        for format_name, format_results in test_results.items():
            successful_results = [r for r in format_results if r['status'] == 'success']
            times = [r['average_time'] for r in successful_results]
            format_times[format_name] = times
            all_times.extend(times)
        
        if not all_times:
            return {'error': 'No successful executions found'}
        
        # Basic statistics
        basic_stats = {
            'mean': np.mean(all_times),
            'median': np.median(all_times),
            'std': np.std(all_times),
            'variance': np.var(all_times),
            'skewness': stats.skew(all_times),
            'kurtosis': stats.kurtosis(all_times),
            'min': np.min(all_times),
            'max': np.max(all_times),
            'range': np.max(all_times) - np.min(all_times),
            'iqr': np.percentile(all_times, 75) - np.percentile(all_times, 25)
        }
        
        # Distribution analysis
        distribution_analysis = {
            'is_normal': stats.normaltest(all_times)[1] > 0.05,
            'shapiro_p_value': stats.shapiro(all_times)[1] if len(all_times) <= 5000 else None,
            'percentiles': {
                'p10': np.percentile(all_times, 10),
                'p25': np.percentile(all_times, 25),
                'p50': np.percentile(all_times, 50),
                'p75': np.percentile(all_times, 75),
                'p90': np.percentile(all_times, 90),
                'p95': np.percentile(all_times, 95),
                'p99': np.percentile(all_times, 99)
            }
        }
        
        # ANOVA test for format differences
        anova_results = {}
        if len(format_times) > 1:
            format_time_lists = [times for times in format_times.values() if len(times) > 1]
            if len(format_time_lists) > 1:
                f_stat, p_value = stats.f_oneway(*format_time_lists)
                anova_results = {
                    'f_statistic': f_stat,
                    'p_value': p_value,
                    'significant': p_value < 0.05,
                    'conclusion': 'Significant difference between formats' if p_value < 0.05 else 'No significant difference between formats'
                }
        
        return {
            'basic_statistics': basic_stats,
            'distribution_analysis': distribution_analysis,
            'anova_results': anova_results,
            'format_statistics': {fmt: {
                'mean': np.mean(times),
                'std': np.std(times),
                'count': len(times)
            } for fmt, times in format_times.items()}
        }
    
    def _analyze_performance_trends(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze performance trends"""
        
        trends = {}
        
        # Analyze trends by query number (if queries are ordered)
        for format_name, format_results in test_results.items():
            successful_results = [r for r in format_results if r['status'] == 'success']
            
            if len(successful_results) > 5:  # Need enough data points
                # Sort by query number
                sorted_results = sorted(successful_results, key=lambda x: x['query_number'])
                query_nums = [r['query_number'] for r in sorted_results]
                times = [r['average_time'] for r in sorted_results]
                
                # Calculate trend
                slope, intercept, r_value, p_value, std_err = stats.linregress(query_nums, times)
                
                trends[format_name] = {
                    'slope': slope,
                    'intercept': intercept,
                    'r_squared': r_value ** 2,
                    'p_value': p_value,
                    'trend_direction': 'increasing' if slope > 0 else 'decreasing' if slope < 0 else 'stable',
                    'trend_strength': abs(r_value),
                    'significant_trend': p_value < 0.05
                }
        
        return trends
    
    def _detect_anomalies(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Detect performance anomalies"""
        
        anomalies = {
            'outliers': [],
            'slow_queries': [],
            'failed_queries': [],
            'inconsistent_performance': []
        }
        
        for format_name, format_results in test_results.items():
            successful_results = [r for r in format_results if r['status'] == 'success']
            
            if len(successful_results) > 3:  # Need enough data for outlier detection
                times = [r['average_time'] for r in successful_results]
                
                # Detect outliers using IQR method
                Q1 = np.percentile(times, 25)
                Q3 = np.percentile(times, 75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                for result in successful_results:
                    if result['average_time'] < lower_bound or result['average_time'] > upper_bound:
                        anomalies['outliers'].append({
                            'format': format_name,
                            'query_number': result['query_number'],
                            'execution_time': result['average_time'],
                            'type': 'low' if result['average_time'] < lower_bound else 'high'
                        })
                
                # Detect slow queries (top 10% slowest)
                slow_threshold = np.percentile(times, 90)
                for result in successful_results:
                    if result['average_time'] > slow_threshold:
                        anomalies['slow_queries'].append({
                            'format': format_name,
                            'query_number': result['query_number'],
                            'execution_time': result['average_time'],
                            'threshold': slow_threshold
                        })
            
            # Detect failed queries
            failed_results = [r for r in format_results if r['status'] != 'success']
            for result in failed_results:
                anomalies['failed_queries'].append({
                    'format': format_name,
                    'query_number': result['query_number'],
                    'status': result['status'],
                    'error': result.get('error', 'Unknown error')
                })
        
        return anomalies
    
    def _analyze_correlations(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze correlations between different metrics"""
        
        correlations = {}
        
        # Collect data for correlation analysis
        data_points = []
        
        for format_name, format_results in test_results.items():
            for result in format_results:
                if result['status'] == 'success':
                    data_points.append({
                        'query_number': result['query_number'],
                        'format': format_name,
                        'execution_time': result['average_time'],
                        'result_rows': result.get('average_rows', 0),
                        'std_time': result.get('std_time', 0)
                    })
        
        if len(data_points) > 10:  # Need enough data points
            df = pd.DataFrame(data_points)
            
            # Correlation between execution time and result rows
            time_row_corr = df['execution_time'].corr(df['result_rows'])
            
            # Correlation between execution time and query number
            time_query_corr = df['execution_time'].corr(df['query_number'])
            
            # Correlation between execution time and standard deviation
            time_std_corr = df['execution_time'].corr(df['std_time'])
            
            correlations = {
                'execution_time_vs_result_rows': time_row_corr,
                'execution_time_vs_query_number': time_query_corr,
                'execution_time_vs_std_deviation': time_std_corr,
                'correlation_interpretation': self._interpret_correlations({
                    'time_vs_rows': time_row_corr,
                    'time_vs_query': time_query_corr,
                    'time_vs_std': time_std_corr
                })
            }
        
        return correlations
    
    def _generate_analytics_recommendations(self, test_results: Dict[str, Any]) -> List[str]:
        """Generate analytics-based recommendations"""
        
        recommendations = []
        
        # Analyze format performance
        format_analysis = self._analyze_format_performance(test_results)
        
        if 'pairwise_comparisons' in format_analysis:
            for comparison, data in format_analysis['pairwise_comparisons'].items():
                if data['significant'] and data['effect_size'] == 'large':
                    recommendations.append(
                        f"Large performance difference detected between {comparison.replace('_vs_', ' and ')} "
                        f"(effect size: {data['cohens_d']:.2f}). Consider investigating the slower format."
                    )
        
        # Analyze anomalies
        anomalies = self._detect_anomalies(test_results)
        
        if anomalies['outliers']:
            high_outliers = [o for o in anomalies['outliers'] if o['type'] == 'high']
            if high_outliers:
                recommendations.append(
                    f"Detected {len(high_outliers)} high-performance outliers. "
                    "These queries may indicate optimization opportunities."
                )
        
        if anomalies['slow_queries']:
            recommendations.append(
                f"Identified {len(anomalies['slow_queries'])} slow queries. "
                "Consider optimizing these queries or investigating format-specific issues."
            )
        
        if anomalies['failed_queries']:
            recommendations.append(
                f"Found {len(anomalies['failed_queries'])} failed queries. "
                "Review error logs and consider query modifications."
            )
        
        # Analyze trends
        trends = self._analyze_performance_trends(test_results)
        
        for format_name, trend_data in trends.items():
            if trend_data['significant_trend'] and trend_data['trend_direction'] == 'increasing':
                recommendations.append(
                    f"Detected increasing performance trend in {format_name} format. "
                    "This may indicate scalability issues or resource constraints."
                )
        
        return recommendations
    
    def _generate_benchmark_analysis(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate benchmark analysis"""
        
        benchmark = {
            'performance_baseline': self._establish_performance_baseline(test_results),
            'format_ranking': self._rank_formats(test_results),
            'performance_targets': self._define_performance_targets(test_results),
            'improvement_opportunities': self._identify_improvement_opportunities(test_results)
        }
        
        return benchmark
    
    def _establish_performance_baseline(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Establish performance baseline"""
        
        all_times = []
        for format_results in test_results.values():
            successful_results = [r for r in format_results if r['status'] == 'success']
            all_times.extend([r['average_time'] for r in successful_results])
        
        if not all_times:
            return {}
        
        return {
            'overall_baseline': {
                'mean': np.mean(all_times),
                'median': np.median(all_times),
                'p95': np.percentile(all_times, 95),
                'p99': np.percentile(all_times, 99)
            },
            'format_baselines': {
                format_name: {
                    'mean': np.mean([r['average_time'] for r in format_results if r['status'] == 'success']),
                    'median': np.median([r['average_time'] for r in format_results if r['status'] == 'success'])
                }
                for format_name, format_results in test_results.items()
                if any(r['status'] == 'success' for r in format_results)
            }
        }
    
    def _rank_formats(self, test_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Rank formats by performance"""
        
        format_scores = {}
        
        for format_name, format_results in test_results.items():
            successful_results = [r for r in format_results if r['status'] == 'success']
            
            if successful_results:
                success_rate = len(successful_results) / len(format_results)
                avg_time = np.mean([r['average_time'] for r in successful_results])
                
                # Composite score: success rate * (1 / avg_time)
                # Higher is better
                score = success_rate * (1 / (avg_time + 1))  # +1 to avoid division by zero
                
                format_scores[format_name] = {
                    'score': score,
                    'success_rate': success_rate,
                    'avg_time': avg_time,
                    'total_queries': len(format_results)
                }
        
        # Sort by score (descending)
        ranking = sorted(format_scores.items(), key=lambda x: x[1]['score'], reverse=True)
        
        return [{'format': fmt, **data} for fmt, data in ranking]
    
    def _define_performance_targets(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Define performance targets"""
        
        all_times = []
        for format_results in test_results.values():
            successful_results = [r for r in format_results if r['status'] == 'success']
            all_times.extend([r['average_time'] for r in successful_results])
        
        if not all_times:
            return {}
        
        baseline_mean = np.mean(all_times)
        baseline_p95 = np.percentile(all_times, 95)
        
        return {
            'target_execution_time': baseline_mean * 0.8,  # 20% improvement target
            'max_execution_time': baseline_p95 * 1.2,     # 20% tolerance
            'target_success_rate': 0.95,                  # 95% success rate target
            'baseline_metrics': {
                'mean': baseline_mean,
                'p95': baseline_p95
            }
        }
    
    def _identify_improvement_opportunities(self, test_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify improvement opportunities"""
        
        opportunities = []
        
        # Find slowest queries
        all_results = []
        for format_results in test_results.values():
            all_results.extend(format_results)
        
        slowest_queries = sorted(
            [r for r in all_results if r['status'] == 'success'],
            key=lambda x: x['average_time'],
            reverse=True
        )[:10]
        
        for result in slowest_queries:
            opportunities.append({
                'type': 'slow_query',
                'query_number': result['query_number'],
                'format': result['format'],
                'execution_time': result['average_time'],
                'improvement_potential': 'high'
            })
        
        # Find formats with low success rates
        for format_name, format_results in test_results.items():
            success_rate = sum(1 for r in format_results if r['status'] == 'success') / len(format_results)
            if success_rate < 0.9:
                opportunities.append({
                    'type': 'low_success_rate',
                    'format': format_name,
                    'success_rate': success_rate,
                    'improvement_potential': 'high'
                })
        
        return opportunities
    
    def _extract_key_insights(self, test_results: Dict[str, Any], format_performance: Dict[str, Any]) -> List[str]:
        """Extract key insights from the data"""
        
        insights = []
        
        # Overall performance insight
        total_queries = sum(len(format_results) for format_results in test_results.values())
        total_successful = sum(
            sum(1 for result in format_results if result['status'] == 'success')
            for format_results in test_results.values()
        )
        
        if total_successful / total_queries > 0.95:
            insights.append("Excellent overall success rate (>95%) across all formats")
        elif total_successful / total_queries > 0.9:
            insights.append("Good overall success rate (>90%) with room for improvement")
        else:
            insights.append("Success rate below 90% - significant issues detected")
        
        # Format performance insights
        if format_performance:
            best_format = min(format_performance.items(), key=lambda x: x[1]['avg_execution_time'])
            worst_format = max(format_performance.items(), key=lambda x: x[1]['avg_execution_time'])
            
            performance_ratio = worst_format[1]['avg_execution_time'] / best_format[1]['avg_execution_time']
            
            if performance_ratio > 2:
                insights.append(f"Significant performance gap: {worst_format[0]} is {performance_ratio:.1f}x slower than {best_format[0]}")
            elif performance_ratio > 1.5:
                insights.append(f"Moderate performance difference: {worst_format[0]} is {performance_ratio:.1f}x slower than {best_format[0]}")
            else:
                insights.append("Formats show similar performance characteristics")
        
        return insights
    
    def _analyze_performance_consistency(self, format_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze performance consistency within formats"""
        
        consistency = {}
        
        for format_name, data in format_data.items():
            times = data['execution_times']
            
            if len(times) > 1:
                cv = np.std(times) / np.mean(times) if np.mean(times) > 0 else 0
                
                consistency[format_name] = {
                    'coefficient_of_variation': cv,
                    'consistency_level': 'high' if cv < 0.2 else 'medium' if cv < 0.5 else 'low',
                    'std_deviation': np.std(times),
                    'range': np.max(times) - np.min(times)
                }
        
        return consistency
    
    def _categorize_queries_by_performance(self, query_analysis: Dict[str, Any]) -> Dict[str, List[str]]:
        """Categorize queries by performance characteristics"""
        
        categories = {
            'fast_queries': [],
            'slow_queries': [],
            'variable_performance': [],
            'consistent_performance': []
        }
        
        for query_key, analysis in query_analysis.items():
            query_num = query_key.replace('query_', '')
            
            # Categorize by speed
            if analysis['best_time'] < 1.0:  # Fast queries
                categories['fast_queries'].append(query_num)
            elif analysis['worst_time'] > 10.0:  # Slow queries
                categories['slow_queries'].append(query_num)
            
            # Categorize by consistency
            if analysis['coefficient_of_variation'] > 0.5:  # Variable performance
                categories['variable_performance'].append(query_num)
            else:  # Consistent performance
                categories['consistent_performance'].append(query_num)
        
        return categories
    
    def _interpret_correlations(self, correlations: Dict[str, float]) -> Dict[str, str]:
        """Interpret correlation coefficients"""
        
        interpretations = {}
        
        for metric, corr in correlations.items():
            if abs(corr) > 0.7:
                strength = 'strong'
            elif abs(corr) > 0.3:
                strength = 'moderate'
            else:
                strength = 'weak'
            
            direction = 'positive' if corr > 0 else 'negative'
            
            interpretations[metric] = f"{strength} {direction} correlation (r={corr:.3f})"
        
        return interpretations
    
    def _analyze_query_categories(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze performance by TPC-DS query categories"""
        
        # Define TPC-DS query categories based on README
        query_categories = {
            'reporting_queries': {
                'range': (1, 20),
                'description': 'Standard business reporting and dashboard queries',
                'characteristics': 'Simple aggregations, groupings, basic filters',
                'performance_focus': 'Fast execution for real-time dashboards'
            },
            'ad_hoc_queries': {
                'range': (21, 40),
                'description': 'Business analysis and exploration queries',
                'characteristics': 'Complex filtering, multiple joins, analytical functions',
                'performance_focus': 'Balanced performance for analytical workloads'
            },
            'iterative_queries': {
                'range': (41, 60),
                'description': 'Multi-step analytical processes',
                'characteristics': 'Complex business logic, subqueries, window functions',
                'performance_focus': 'Optimized for analytical processing'
            },
            'data_mining_queries': {
                'range': (61, 99),
                'description': 'Advanced analytics and statistical analysis',
                'characteristics': 'Complex joins, statistical functions, data mining operations',
                'performance_focus': 'Optimized for large-scale analytical processing'
            }
        }
        
        category_analysis = {}
        
        for category_name, category_info in query_categories.items():
            start_q, end_q = category_info['range']
            
            # Collect data for this category across all formats
            category_data = {
                'query_range': f"q{start_q:02d}-q{end_q:02d}",
                'total_queries': end_q - start_q + 1,
                'description': category_info['description'],
                'characteristics': category_info['characteristics'],
                'performance_focus': category_info['performance_focus'],
                'format_performance': {},
                'category_statistics': {},
                'query_details': []
            }
            
            # Analyze each format for this category
            for format_name, format_results in test_results.items():
                category_queries = [
                    r for r in format_results 
                    if start_q <= r['query_number'] <= end_q
                ]
                
                successful_queries = [r for r in category_queries if r['status'] == 'success']
                
                if successful_queries:
                    times = [r['average_time'] for r in successful_queries]
                    rows = [r.get('average_rows', 0) for r in successful_queries]
                    
                    category_data['format_performance'][format_name] = {
                        'queries_tested': len(category_queries),
                        'successful_queries': len(successful_queries),
                        'success_rate': len(successful_queries) / len(category_queries) if category_queries else 0,
                        'avg_execution_time': np.mean(times),
                        'median_execution_time': np.median(times),
                        'min_execution_time': np.min(times),
                        'max_execution_time': np.max(times),
                        'std_execution_time': np.std(times),
                        'p95_execution_time': np.percentile(times, 95),
                        'total_result_rows': sum(rows),
                        'avg_result_rows': np.mean(rows) if rows else 0
                    }
                    
                    # Store individual query details
                    for result in successful_queries:
                        category_data['query_details'].append({
                            'query_number': result['query_number'],
                            'format': format_name,
                            'execution_time': result['average_time'],
                            'result_rows': result.get('average_rows', 0),
                            'std_time': result.get('std_time', 0)
                        })
            
            # Calculate category-level statistics
            if category_data['format_performance']:
                all_times = []
                all_success_rates = []
                
                for format_perf in category_data['format_performance'].values():
                    all_times.extend([r['execution_time'] for r in category_data['query_details'] 
                                    if r['format'] == list(category_data['format_performance'].keys())[0]])
                    all_success_rates.append(format_perf['success_rate'])
                
                category_data['category_statistics'] = {
                    'overall_avg_time': np.mean(all_times) if all_times else 0,
                    'overall_median_time': np.median(all_times) if all_times else 0,
                    'overall_success_rate': np.mean(all_success_rates),
                    'performance_variance': np.var(all_times) if all_times else 0,
                    'fastest_format': min(category_data['format_performance'].items(), 
                                        key=lambda x: x[1]['avg_execution_time'])[0] if category_data['format_performance'] else None,
                    'slowest_format': max(category_data['format_performance'].items(), 
                                        key=lambda x: x[1]['avg_execution_time'])[0] if category_data['format_performance'] else None
                }
            
            category_analysis[category_name] = category_data
        
        # Generate category comparison
        category_comparison = self._compare_categories(category_analysis)
        
        return {
            'categories': category_analysis,
            'category_comparison': category_comparison,
            'summary': self._generate_category_summary(category_analysis)
        }
    
    def _compare_categories(self, category_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Compare performance across query categories"""
        
        comparison = {
            'performance_ranking': [],
            'complexity_analysis': {},
            'format_consistency': {},
            'category_insights': []
        }
        
        # Rank categories by average performance
        category_avg_times = {}
        for category_name, category_data in category_analysis.items():
            if category_data['category_statistics']:
                category_avg_times[category_name] = category_data['category_statistics']['overall_avg_time']
        
        if category_avg_times:
            sorted_categories = sorted(category_avg_times.items(), key=lambda x: x[1])
            comparison['performance_ranking'] = [
                {'category': cat, 'avg_time': time, 'rank': i+1}
                for i, (cat, time) in enumerate(sorted_categories)
            ]
        
        # Analyze complexity progression
        if len(category_avg_times) >= 2:
            times = list(category_avg_times.values())
            if all(t > 0 for t in times):
                # Check if there's a complexity progression
                complexity_trend = np.polyfit(range(len(times)), times, 1)[0]
                comparison['complexity_analysis'] = {
                    'trend_slope': complexity_trend,
                    'trend_direction': 'increasing' if complexity_trend > 0 else 'decreasing' if complexity_trend < 0 else 'stable',
                    'complexity_progression': abs(complexity_trend) > 0.1
                }
        
        # Analyze format consistency across categories
        for format_name in ['native', 'iceberg_sf', 'external']:
            format_times = []
            for category_data in category_analysis.values():
                if format_name in category_data['format_performance']:
                    format_times.append(category_data['format_performance'][format_name]['avg_execution_time'])
            
            if len(format_times) > 1:
                cv = np.std(format_times) / np.mean(format_times) if np.mean(format_times) > 0 else 0
                comparison['format_consistency'][format_name] = {
                    'coefficient_of_variation': cv,
                    'consistency_level': 'high' if cv < 0.2 else 'medium' if cv < 0.5 else 'low',
                    'time_range': max(format_times) - min(format_times)
                }
        
        return comparison
    
    def _generate_category_summary(self, category_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary insights for query categories"""
        
        summary = {
            'total_categories': len(category_analysis),
            'category_performance': {},
            'key_insights': [],
            'recommendations': []
        }
        
        # Analyze each category
        for category_name, category_data in category_analysis.items():
            if category_data['category_statistics']:
                stats = category_data['category_statistics']
                summary['category_performance'][category_name] = {
                    'avg_time': stats['overall_avg_time'],
                    'success_rate': stats['overall_success_rate'],
                    'query_count': category_data['total_queries'],
                    'performance_level': 'fast' if stats['overall_avg_time'] < 1.0 else 'medium' if stats['overall_avg_time'] < 5.0 else 'slow'
                }
        
        # Generate insights
        if summary['category_performance']:
            fastest_category = min(summary['category_performance'].items(), key=lambda x: x[1]['avg_time'])
            slowest_category = max(summary['category_performance'].items(), key=lambda x: x[1]['avg_time'])
            
            summary['key_insights'].append(
                f"Fastest category: {fastest_category[0]} ({fastest_category[1]['avg_time']:.3f}s avg)"
            )
            summary['key_insights'].append(
                f"Slowest category: {slowest_category[0]} ({slowest_category[1]['avg_time']:.3f}s avg)"
            )
            
            # Performance gap analysis
            if fastest_category[1]['avg_time'] > 0:
                gap_ratio = slowest_category[1]['avg_time'] / fastest_category[1]['avg_time']
                summary['key_insights'].append(
                    f"Performance gap: {gap_ratio:.1f}x difference between fastest and slowest categories"
                )
        
        return summary
    
    def _generate_detailed_query_comparison(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate detailed per-query comparison across formats"""
        
        detailed_comparison = {
            'query_comparisons': {},
            'format_rankings_per_query': {},
            'performance_insights': {},
            'optimization_opportunities': []
        }
        
        # Collect all query data
        query_data = defaultdict(lambda: defaultdict(dict))
        
        for format_name, format_results in test_results.items():
            for result in format_results:
                query_num = result['query_number']
                if result['status'] == 'success':
                    query_data[query_num][format_name] = {
                        'execution_time': result['average_time'],
                        'result_rows': result.get('average_rows', 0),
                        'std_time': result.get('std_time', 0),
                        'min_time': result.get('min_time', result['average_time']),
                        'max_time': result.get('max_time', result['average_time'])
                    }
        
        # Generate detailed comparison for each query
        for query_num, format_results in query_data.items():
            if len(format_results) > 1:  # Need multiple formats for comparison
                
                # Sort formats by execution time
                sorted_formats = sorted(format_results.items(), key=lambda x: x[1]['execution_time'])
                
                query_comparison = {
                    'query_number': query_num,
                    'formats_tested': len(format_results),
                    'performance_ranking': [
                        {
                            'format': fmt,
                            'execution_time': data['execution_time'],
                            'result_rows': data['result_rows'],
                            'rank': i + 1
                        }
                        for i, (fmt, data) in enumerate(sorted_formats)
                    ],
                    'performance_metrics': {
                        'fastest_format': sorted_formats[0][0],
                        'slowest_format': sorted_formats[-1][0],
                        'fastest_time': sorted_formats[0][1]['execution_time'],
                        'slowest_time': sorted_formats[-1][1]['execution_time'],
                        'performance_ratio': sorted_formats[-1][1]['execution_time'] / sorted_formats[0][1]['execution_time'] if sorted_formats[0][1]['execution_time'] > 0 else 0,
                        'time_variance': np.var([data['execution_time'] for data in format_results.values()]),
                        'coefficient_of_variation': np.std([data['execution_time'] for data in format_results.values()]) / np.mean([data['execution_time'] for data in format_results.values()]) if np.mean([data['execution_time'] for data in format_results.values()]) > 0 else 0
                    },
                    'format_details': format_results
                }
                
                # Add performance insights
                if query_comparison['performance_metrics']['performance_ratio'] > 2:
                    query_comparison['insights'] = ['Large performance difference between formats']
                elif query_comparison['performance_metrics']['coefficient_of_variation'] > 0.5:
                    query_comparison['insights'] = ['High performance variance across formats']
                else:
                    query_comparison['insights'] = ['Consistent performance across formats']
                
                detailed_comparison['query_comparisons'][f'q{query_num:02d}'] = query_comparison
        
        # Generate format rankings per query
        for format_name in ['native', 'iceberg_sf', 'external']:
            format_rankings = []
            for query_num, format_results in query_data.items():
                if format_name in format_results and len(format_results) > 1:
                    sorted_formats = sorted(format_results.items(), key=lambda x: x[1]['execution_time'])
                    rank = next(i + 1 for i, (fmt, _) in enumerate(sorted_formats) if fmt == format_name)
                    format_rankings.append({
                        'query_number': query_num,
                        'rank': rank,
                        'execution_time': format_results[format_name]['execution_time']
                    })
            
            if format_rankings:
                avg_rank = np.mean([r['rank'] for r in format_rankings])
                detailed_comparison['format_rankings_per_query'][format_name] = {
                    'average_rank': avg_rank,
                    'rankings': format_rankings,
                    'best_rank_count': sum(1 for r in format_rankings if r['rank'] == 1),
                    'worst_rank_count': sum(1 for r in format_rankings if r['rank'] == len(format_rankings[0]) if format_rankings else 0)
                }
        
        # Identify optimization opportunities
        for query_key, comparison in detailed_comparison['query_comparisons'].items():
            metrics = comparison['performance_metrics']
            
            if metrics['performance_ratio'] > 3:  # Very large difference
                detailed_comparison['optimization_opportunities'].append({
                    'query': query_key,
                    'type': 'large_performance_gap',
                    'description': f"Query {query_key} shows {metrics['performance_ratio']:.1f}x performance difference between formats",
                    'fastest_format': metrics['fastest_format'],
                    'slowest_format': metrics['slowest_format'],
                    'priority': 'high'
                })
            
            if metrics['coefficient_of_variation'] > 0.8:  # High variance
                detailed_comparison['optimization_opportunities'].append({
                    'query': query_key,
                    'type': 'high_variance',
                    'description': f"Query {query_key} shows high performance variance (CV: {metrics['coefficient_of_variation']:.2f})",
                    'priority': 'medium'
                })
        
        return detailed_comparison

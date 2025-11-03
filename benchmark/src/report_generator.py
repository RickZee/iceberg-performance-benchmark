#!/usr/bin/env python3
"""
Report Generator for TPC-DS Performance Testing
Generates comprehensive reports in multiple formats
"""

import json
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from datetime import datetime
import statistics
import base64
from io import BytesIO

logger = logging.getLogger(__name__)

class ReportGenerator:
    """Generates comprehensive performance reports"""
    
    def __init__(self, config: Dict[str, Any], results_dir: Path, reports_dir: Path):
        """Initialize report generator"""
        self.config = config
        self.results_dir = results_dir
        self.reports_dir = reports_dir
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Set up plotting style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
    def generate_all_reports(self, test_results: Dict[str, Any], 
                           metrics_collector=None) -> Dict[str, Path]:
        """Generate all configured reports"""
        
        generated_reports = {}
        
        # Generate JSON report
        if self.config.get('reporting', {}).get('generate_json', True):
            json_report = self.generate_json_report(test_results)
            generated_reports['json'] = json_report
        
        # Generate CSV report
        if self.config.get('reporting', {}).get('generate_csv', True):
            csv_report = self.generate_csv_report(test_results)
            generated_reports['csv'] = csv_report
        
        # Generate HTML report
        if self.config.get('reporting', {}).get('generate_html', True):
            html_report = self.generate_html_report(test_results, metrics_collector)
            generated_reports['html'] = html_report
        
        # Generate PDF report (if enabled)
        if self.config.get('reporting', {}).get('generate_pdf', False):
            pdf_report = self.generate_pdf_report(test_results, metrics_collector)
            generated_reports['pdf'] = pdf_report
        
        logger.info(f"Generated {len(generated_reports)} reports")
        return generated_reports
    
    def generate_json_report(self, test_results: Dict[str, Any]) -> Path:
        """Generate detailed JSON report"""
        
        report_data = {
            'report_metadata': {
                'generated_at': datetime.now().isoformat(),
                'report_type': 'tpcds_performance_test',
                'version': '1.0',
                'config': self.config
            },
            'test_summary': self._create_test_summary(test_results),
            'detailed_results': test_results,
            'performance_analysis': self._analyze_performance(test_results),
            'recommendations': self._generate_recommendations(test_results)
        }
        
        report_file = self.reports_dir / f"tpcds_performance_report_{self.timestamp}.json"
        
        with open(report_file, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        logger.info(f"JSON report generated: {report_file}")
        return report_file
    
    def generate_csv_report(self, test_results: Dict[str, Any]) -> Path:
        """Generate CSV report for data analysis"""
        
        rows = []
        
        for format_name, format_results in test_results.items():
            for result in format_results:
                rows.append({
                    'format': format_name,
                    'query_number': result['query_number'],
                    'query_file': result['query_file'],
                    'status': result['status'],
                    'average_time': result.get('average_time', 0),
                    'min_time': result.get('min_time', 0),
                    'max_time': result.get('max_time', 0),
                    'median_time': result.get('median_time', 0),
                    'std_time': result.get('std_time', 0),
                    'average_rows': result.get('average_rows', 0),
                    'min_rows': result.get('min_rows', 0),
                    'max_rows': result.get('max_rows', 0),
                    'error_count': result.get('error_count', 0),
                    'success_rate': result.get('success_rate', 0),
                    'total_runs': result.get('total_runs', 0),
                    'successful_runs': result.get('successful_runs', 0),
                    'timestamp': result.get('timestamp', '')
                })
        
        df = pd.DataFrame(rows)
        
        report_file = self.reports_dir / f"tpcds_performance_data_{self.timestamp}.csv"
        df.to_csv(report_file, index=False)
        
        logger.info(f"CSV report generated: {report_file}")
        return report_file
    
    def generate_html_report(self, test_results: Dict[str, Any], 
                           metrics_collector=None) -> Path:
        """Generate comprehensive HTML report"""
        
        report_file = self.reports_dir / f"tpcds_performance_report_{self.timestamp}.html"
        
        # Generate charts
        charts = self._generate_charts(test_results, metrics_collector)
        
        # Create HTML content
        html_content = self._create_html_content(test_results, charts, metrics_collector)
        
        with open(report_file, 'w') as f:
            f.write(html_content)
        
        logger.info(f"HTML report generated: {report_file}")
        return report_file
    
    def generate_pdf_report(self, test_results: Dict[str, Any], 
                          metrics_collector=None) -> Path:
        """Generate PDF report (requires additional setup)"""
        
        try:
            from reportlab.lib.pagesizes import letter
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
            from reportlab.lib.styles import getSampleStyleSheet
            
            report_file = self.reports_dir / f"tpcds_performance_report_{self.timestamp}.pdf"
            
            doc = SimpleDocTemplate(str(report_file), pagesize=letter)
            styles = getSampleStyleSheet()
            story = []
            
            # Add title
            title = Paragraph("TPC-DS Performance Test Report", styles['Title'])
            story.append(title)
            story.append(Spacer(1, 12))
            
            # Add summary
            summary = self._create_test_summary(test_results)
            summary_text = f"""
            <b>Test Summary:</b><br/>
            Total Queries: {summary['total_queries']}<br/>
            Successful Queries: {summary['successful_queries']}<br/>
            Success Rate: {summary['success_rate']:.2%}<br/>
            Average Execution Time: {summary['average_execution_time']:.2f}s<br/>
            """
            
            story.append(Paragraph(summary_text, styles['Normal']))
            story.append(Spacer(1, 12))
            
            # Add format comparison
            for format_name, format_data in summary['format_summary'].items():
                format_text = f"""
                <b>{format_name.upper()} Format:</b><br/>
                Queries: {format_data['total_queries']}<br/>
                Success Rate: {format_data['success_rate']:.2%}<br/>
                Average Time: {format_data['average_execution_time']:.2f}s<br/>
                """
                story.append(Paragraph(format_text, styles['Normal']))
                story.append(Spacer(1, 6))
            
            doc.build(story)
            
            logger.info(f"PDF report generated: {report_file}")
            return report_file
            
        except ImportError:
            logger.warning("reportlab not available, skipping PDF report generation")
            return None
        except Exception as e:
            logger.error(f"Failed to generate PDF report: {e}")
            return None
    
    def _create_test_summary(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Create test summary statistics"""
        
        total_queries = sum(len(format_results) for format_results in test_results.values())
        total_successful = sum(
            sum(1 for result in format_results if result['status'] == 'success')
            for format_results in test_results.values()
        )
        
        all_times = []
        format_summary = {}
        
        for format_name, format_results in test_results.items():
            successful_results = [r for r in format_results if r['status'] == 'success']
            
            if successful_results:
                format_times = [r['average_time'] for r in successful_results]
                all_times.extend(format_times)
                
                format_summary[format_name] = {
                    'total_queries': len(format_results),
                    'successful_queries': len(successful_results),
                    'success_rate': len(successful_results) / len(format_results),
                    'average_execution_time': statistics.mean(format_times),
                    'median_execution_time': statistics.median(format_times),
                    'min_execution_time': min(format_times),
                    'max_execution_time': max(format_times)
                }
            else:
                format_summary[format_name] = {
                    'total_queries': len(format_results),
                    'successful_queries': 0,
                    'success_rate': 0,
                    'average_execution_time': 0,
                    'median_execution_time': 0,
                    'min_execution_time': 0,
                    'max_execution_time': 0
                }
        
        return {
            'total_queries': total_queries,
            'successful_queries': total_successful,
            'success_rate': total_successful / total_queries if total_queries > 0 else 0,
            'average_execution_time': statistics.mean(all_times) if all_times else 0,
            'median_execution_time': statistics.median(all_times) if all_times else 0,
            'min_execution_time': min(all_times) if all_times else 0,
            'max_execution_time': max(all_times) if all_times else 0,
            'format_summary': format_summary
        }
    
    def _analyze_performance(self, test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze performance data"""
        
        analysis = {
            'format_performance': {},
            'query_performance': {},
            'statistical_analysis': {},
            'performance_trends': {}
        }
        
        # Format performance analysis
        for format_name, format_results in test_results.items():
            successful_results = [r for r in format_results if r['status'] == 'success']
            
            if successful_results:
                times = [r['average_time'] for r in successful_results]
                analysis['format_performance'][format_name] = {
                    'mean_time': statistics.mean(times),
                    'median_time': statistics.median(times),
                    'std_time': statistics.stdev(times) if len(times) > 1 else 0,
                    'cv_time': statistics.stdev(times) / statistics.mean(times) if len(times) > 1 and statistics.mean(times) > 0 else 0,
                    'percentile_95': sorted(times)[int(len(times) * 0.95)] if times else 0,
                    'percentile_99': sorted(times)[int(len(times) * 0.99)] if times else 0
                }
        
        # Query performance analysis
        query_stats = {}
        for format_name, format_results in test_results.items():
            for result in format_results:
                query_num = result['query_number']
                if query_num not in query_stats:
                    query_stats[query_num] = {}
                
                if result['status'] == 'success':
                    query_stats[query_num][format_name] = result['average_time']
        
        analysis['query_performance'] = query_stats
        
        return analysis
    
    def _generate_recommendations(self, test_results: Dict[str, Any]) -> List[str]:
        """Generate performance recommendations"""
        
        recommendations = []
        summary = self._create_test_summary(test_results)
        
        # Check overall success rate
        if summary['success_rate'] < 0.95:
            recommendations.append(f"Overall success rate is {summary['success_rate']:.2%}, which is below the 95% threshold. Investigate failed queries.")
        
        # Check format performance
        format_times = [(name, data['average_execution_time']) for name, data in summary['format_summary'].items() if data['average_execution_time'] > 0]
        
        if len(format_times) > 1:
            format_times.sort(key=lambda x: x[1])
            fastest = format_times[0]
            slowest = format_times[-1]
            
            if slowest[1] > fastest[1] * 2:
                recommendations.append(f"Significant performance difference: {slowest[0]} is {slowest[1]/fastest[1]:.1f}x slower than {fastest[0]}")
        
        # Check individual format success rates
        for format_name, format_data in summary['format_summary'].items():
            if format_data['success_rate'] < 0.9:
                recommendations.append(f"Format {format_name} has low success rate ({format_data['success_rate']:.2%}). Review error logs.")
        
        return recommendations
    
    def _generate_charts(self, test_results: Dict[str, Any], 
                        metrics_collector=None) -> Dict[str, str]:
        """Generate performance charts"""
        
        charts = {}
        
        try:
            # Format comparison chart
            charts['format_comparison'] = self._create_format_comparison_chart(test_results)
            
            # Query performance chart
            charts['query_performance'] = self._create_query_performance_chart(test_results)
            
            # Success rate chart
            charts['success_rates'] = self._create_success_rate_chart(test_results)
            
            # Execution time distribution
            charts['time_distribution'] = self._create_time_distribution_chart(test_results)
            
        except Exception as e:
            logger.error(f"Error generating charts: {e}")
        
        return charts
    
    def _create_format_comparison_chart(self, test_results: Dict[str, Any]) -> str:
        """Create format comparison chart"""
        
        try:
            fig, ax = plt.subplots(figsize=(10, 6))
            
            formats = []
            avg_times = []
            success_rates = []
            
            for format_name, format_results in test_results.items():
                successful_results = [r for r in format_results if r['status'] == 'success']
                
                if successful_results:
                    formats.append(format_name)
                    avg_times.append(statistics.mean([r['average_time'] for r in successful_results]))
                    success_rates.append(len(successful_results) / len(format_results))
            
            # Create dual-axis chart
            ax2 = ax.twinx()
            
            bars1 = ax.bar([f + '_time' for f in formats], avg_times, alpha=0.7, label='Avg Execution Time (s)')
            bars2 = ax2.bar([f + '_rate' for f in formats], success_rates, alpha=0.7, label='Success Rate', color='orange')
            
            ax.set_xlabel('Format')
            ax.set_ylabel('Average Execution Time (seconds)', color='blue')
            ax2.set_ylabel('Success Rate', color='orange')
            ax.set_title('Format Performance Comparison')
            
            # Set x-axis labels
            ax.set_xticks(range(len(formats) * 2))
            ax.set_xticklabels([f'{f}\n(Time)' for f in formats] + [f'{f}\n(Rate)' for f in formats], rotation=45)
            
            ax.legend(loc='upper left')
            ax2.legend(loc='upper right')
            
            plt.tight_layout()
            
            # Convert to base64
            chart_data = self._chart_to_base64(fig)
            plt.close(fig)
            
            return chart_data
            
        except Exception as e:
            logger.error(f"Error creating format comparison chart: {e}")
            return ""
    
    def _create_query_performance_chart(self, test_results: Dict[str, Any]) -> str:
        """Create query performance chart"""
        
        try:
            fig, ax = plt.subplots(figsize=(12, 8))
            
            # Get top 20 slowest queries across all formats
            all_query_times = []
            for format_name, format_results in test_results.items():
                for result in format_results:
                    if result['status'] == 'success':
                        all_query_times.append((result['query_number'], format_name, result['average_time']))
            
            # Sort by execution time and take top 20
            all_query_times.sort(key=lambda x: x[2], reverse=True)
            top_queries = all_query_times[:20]
            
            query_nums = [f"Q{q[0]}" for q in top_queries]
            times = [q[2] for q in top_queries]
            formats = [q[1] for q in top_queries]
            
            # Create color map for formats
            format_colors = {'native': 'blue', 'iceberg_sf': 'green', 'iceberg_glue': 'orange', 'external': 'red'}
            colors = [format_colors.get(f, 'gray') for f in formats]
            
            bars = ax.bar(query_nums, times, color=colors, alpha=0.7)
            
            ax.set_xlabel('Query Number')
            ax.set_ylabel('Execution Time (seconds)')
            ax.set_title('Top 20 Slowest Queries')
            ax.tick_params(axis='x', rotation=45)
            
            # Add legend
            legend_elements = [plt.Rectangle((0,0),1,1, color=color, alpha=0.7, label=format_name) 
                             for format_name, color in format_colors.items()]
            ax.legend(handles=legend_elements)
            
            plt.tight_layout()
            
            chart_data = self._chart_to_base64(fig)
            plt.close(fig)
            
            return chart_data
            
        except Exception as e:
            logger.error(f"Error creating query performance chart: {e}")
            return ""
    
    def _create_success_rate_chart(self, test_results: Dict[str, Any]) -> str:
        """Create success rate chart"""
        
        try:
            fig, ax = plt.subplots(figsize=(8, 6))
            
            formats = []
            success_rates = []
            
            for format_name, format_results in test_results.items():
                successful = sum(1 for r in format_results if r['status'] == 'success')
                total = len(format_results)
                success_rate = successful / total if total > 0 else 0
                
                formats.append(format_name)
                success_rates.append(success_rate)
            
            bars = ax.bar(formats, success_rates, alpha=0.7, color=['blue', 'green', 'orange', 'red'])
            
            ax.set_xlabel('Format')
            ax.set_ylabel('Success Rate')
            ax.set_title('Success Rate by Format')
            ax.set_ylim(0, 1)
            
            # Add percentage labels on bars
            for bar, rate in zip(bars, success_rates):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                       f'{rate:.1%}', ha='center', va='bottom')
            
            plt.tight_layout()
            
            chart_data = self._chart_to_base64(fig)
            plt.close(fig)
            
            return chart_data
            
        except Exception as e:
            logger.error(f"Error creating success rate chart: {e}")
            return ""
    
    def _create_time_distribution_chart(self, test_results: Dict[str, Any]) -> str:
        """Create execution time distribution chart"""
        
        try:
            fig, ax = plt.subplots(figsize=(10, 6))
            
            for format_name, format_results in test_results.items():
                successful_results = [r for r in format_results if r['status'] == 'success']
                if successful_results:
                    times = [r['average_time'] for r in successful_results]
                    ax.hist(times, bins=20, alpha=0.6, label=format_name, density=True)
            
            ax.set_xlabel('Execution Time (seconds)')
            ax.set_ylabel('Density')
            ax.set_title('Execution Time Distribution by Format')
            ax.legend()
            
            plt.tight_layout()
            
            chart_data = self._chart_to_base64(fig)
            plt.close(fig)
            
            return chart_data
            
        except Exception as e:
            logger.error(f"Error creating time distribution chart: {e}")
            return ""
    
    def _chart_to_base64(self, fig) -> str:
        """Convert matplotlib figure to base64 string"""
        
        buffer = BytesIO()
        fig.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
        buffer.seek(0)
        
        image_base64 = base64.b64encode(buffer.getvalue()).decode()
        buffer.close()
        
        return f"data:image/png;base64,{image_base64}"
    
    def _create_html_content(self, test_results: Dict[str, Any], 
                           charts: Dict[str, str], metrics_collector=None) -> str:
        """Create comprehensive HTML content"""
        
        summary = self._create_test_summary(test_results)
        analysis = self._analyze_performance(test_results)
        recommendations = self._generate_recommendations(test_results)
        
        # Generate enhanced analytics if available
        enhanced_analytics = None
        if metrics_collector and hasattr(metrics_collector, 'analytics_engine'):
            try:
                enhanced_analytics = metrics_collector.analytics_engine.generate_comprehensive_analytics(test_results)
            except Exception as e:
                logger.warning(f"Could not generate enhanced analytics: {e}")
                enhanced_analytics = None
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>TPC-DS Performance Test Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; line-height: 1.6; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; }}
        .summary {{ background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0; }}
        .format-section {{ margin: 30px 0; border: 1px solid #dee2e6; border-radius: 8px; overflow: hidden; }}
        .format-header {{ background-color: #e9ecef; padding: 15px; font-weight: bold; font-size: 1.2em; }}
        .format-content {{ padding: 20px; }}
        .query-result {{ margin: 10px 0; padding: 15px; background-color: #f8f9fa; border-radius: 5px; border-left: 5px solid #007bff; }}
        .query-result.success {{ border-left-color: #28a745; }}
        .query-result.failed {{ border-left-color: #dc3545; }}
        .query-result.partial {{ border-left-color: #ffc107; }}
        .query-result.error {{ border-left-color: #dc3545; }}
        .chart {{ text-align: center; margin: 20px 0; }}
        .chart img {{ max-width: 100%; height: auto; border: 1px solid #dee2e6; border-radius: 5px; }}
        .recommendations {{ background-color: #fff3cd; border: 1px solid #ffeaa7; border-radius: 8px; padding: 20px; margin: 20px 0; }}
        .recommendations h3 {{ color: #856404; margin-top: 0; }}
        .recommendations ul {{ margin: 0; }}
        .recommendations li {{ margin: 10px 0; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #dee2e6; padding: 12px; text-align: left; }}
        th {{ background-color: #e9ecef; font-weight: bold; }}
        .metric {{ display: inline-block; margin: 10px 20px 10px 0; }}
        .metric-value {{ font-size: 1.5em; font-weight: bold; color: #007bff; }}
        .metric-label {{ font-size: 0.9em; color: #6c757d; }}
        .status-success {{ color: #28a745; font-weight: bold; }}
        .status-failed {{ color: #dc3545; font-weight: bold; }}
        .status-partial {{ color: #ffc107; font-weight: bold; }}
        .status-error {{ color: #dc3545; font-weight: bold; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>TPC-DS Performance Test Report</h1>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Comprehensive performance analysis across all Snowflake table formats</p>
    </div>
    
    <div class="summary">
        <h2>Test Summary</h2>
        <div class="metric">
            <div class="metric-value">{summary['total_queries']}</div>
            <div class="metric-label">Total Queries</div>
        </div>
        <div class="metric">
            <div class="metric-value">{summary['successful_queries']}</div>
            <div class="metric-label">Successful</div>
        </div>
        <div class="metric">
            <div class="metric-value">{summary['success_rate']:.1%}</div>
            <div class="metric-label">Success Rate</div>
        </div>
        <div class="metric">
            <div class="metric-value">{summary['average_execution_time']:.2f}s</div>
            <div class="metric-label">Avg Time</div>
        </div>
    </div>
"""
        
        # Add charts
        if charts:
            html += """
    <div class="summary">
        <h2>Performance Charts</h2>
"""
            for chart_name, chart_data in charts.items():
                if chart_data:
                    html += f"""
        <div class="chart">
            <h3>{chart_name.replace('_', ' ').title()}</h3>
            <img src="{chart_data}" alt="{chart_name}">
        </div>
"""
            html += "</div>"
        
        # Add format-specific results
        for format_name, format_results in test_results.items():
            format_data = summary['format_summary'][format_name]
            
            html += f"""
    <div class="format-section">
        <div class="format-header">
            Format: {format_name.upper()} 
            <span class="status-{format_data['success_rate'] > 0.9 and 'success' or 'failed'}">
                ({format_data['success_rate']:.1%} success rate)
            </span>
        </div>
        <div class="format-content">
            <p><strong>Queries Tested:</strong> {format_data['total_queries']}</p>
            <p><strong>Successful:</strong> {format_data['successful_queries']}</p>
            <p><strong>Average Time:</strong> {format_data['average_execution_time']:.2f}s</p>
            <p><strong>Median Time:</strong> {format_data['median_execution_time']:.2f}s</p>
            
            <table>
                <tr>
                    <th>Query</th>
                    <th>Status</th>
                    <th>Avg Time (s)</th>
                    <th>Min Time (s)</th>
                    <th>Max Time (s)</th>
                    <th>Std Dev</th>
                    <th>Avg Rows</th>
                    <th>Success Rate</th>
                </tr>
"""
            
            for result in format_results:
                status_class = result['status']
                html += f"""
                <tr>
                    <td>{result['query_file']}</td>
                    <td class="status-{status_class}">{result['status']}</td>
                    <td>{result.get('average_time', 0):.2f}</td>
                    <td>{result.get('min_time', 0):.2f}</td>
                    <td>{result.get('max_time', 0):.2f}</td>
                    <td>{result.get('std_time', 0):.2f}</td>
                    <td>{result.get('average_rows', 0):.0f}</td>
                    <td>{result.get('success_rate', 0):.1%}</td>
                </tr>
"""
            
            html += """
            </table>
        </div>
    </div>
"""
        
        # Add enhanced analytics sections
        if enhanced_analytics:
            html += self._add_enhanced_analytics_sections(enhanced_analytics)
        
        # Add recommendations
        if recommendations:
            html += f"""
    <div class="recommendations">
        <h3>Recommendations</h3>
        <ul>
"""
            for rec in recommendations:
                html += f"<li>{rec}</li>"
            
            html += """
        </ul>
    </div>
"""
        
        html += """
</body>
</html>
"""
        
        return html
    
    def _add_enhanced_analytics_sections(self, enhanced_analytics: Dict[str, Any]) -> str:
        """Add enhanced analytics sections to HTML report"""
        
        html = ""
        
        # Query Category Breakdown Section
        if 'query_category_breakdown' in enhanced_analytics:
            category_breakdown = enhanced_analytics['query_category_breakdown']
            html += """
    <div class="format-section">
        <div class="format-header">
            📊 Query Category Performance Analysis
        </div>
        <div class="format-content">
"""
            
            # Category summary table
            if 'summary' in category_breakdown and 'category_performance' in category_breakdown['summary']:
                html += """
            <h3>Category Performance Summary</h3>
            <table>
                <tr>
                    <th>Category</th>
                    <th>Query Range</th>
                    <th>Avg Time (s)</th>
                    <th>Success Rate</th>
                    <th>Query Count</th>
                    <th>Performance Level</th>
                </tr>
"""
                
                for category_name, perf_data in category_breakdown['summary']['category_performance'].items():
                    html += f"""
                <tr>
                    <td>{category_name.replace('_', ' ').title()}</td>
                    <td>{category_breakdown['categories'][category_name]['query_range']}</td>
                    <td>{perf_data['avg_time']:.3f}</td>
                    <td>{perf_data['success_rate']:.1%}</td>
                    <td>{perf_data['query_count']}</td>
                    <td>{perf_data['performance_level'].title()}</td>
                </tr>
"""
                
                html += """
            </table>
"""
            
            # Category insights
            if 'key_insights' in category_breakdown['summary']:
                html += """
            <h3>Category Insights</h3>
            <ul>
"""
                for insight in category_breakdown['summary']['key_insights']:
                    html += f"<li>{insight}</li>"
                html += "</ul>"
            
            html += """
        </div>
    </div>
"""
        
        # Detailed Query Comparison Section
        if 'detailed_query_comparison' in enhanced_analytics:
            detailed_comparison = enhanced_analytics['detailed_query_comparison']
            html += """
    <div class="format-section">
        <div class="format-header">
            🔍 Detailed Query Performance Comparison
        </div>
        <div class="format-content">
"""
            
            # Format rankings summary
            if 'format_rankings_per_query' in detailed_comparison:
                html += """
            <h3>Format Performance Rankings</h3>
            <table>
                <tr>
                    <th>Format</th>
                    <th>Average Rank</th>
                    <th>Best Rank Count</th>
                    <th>Worst Rank Count</th>
                </tr>
"""
                
                for format_name, ranking_data in detailed_comparison['format_rankings_per_query'].items():
                    html += f"""
                <tr>
                    <td>{format_name.upper()}</td>
                    <td>{ranking_data['average_rank']:.2f}</td>
                    <td>{ranking_data['best_rank_count']}</td>
                    <td>{ranking_data['worst_rank_count']}</td>
                </tr>
"""
                
                html += """
            </table>
"""
            
            # Top 10 queries with largest performance differences
            if 'query_comparisons' in detailed_comparison:
                # Sort queries by performance ratio
                query_ratios = []
                for query_key, comparison in detailed_comparison['query_comparisons'].items():
                    ratio = comparison['performance_metrics']['performance_ratio']
                    query_ratios.append((query_key, ratio, comparison))
                
                query_ratios.sort(key=lambda x: x[1], reverse=True)
                top_queries = query_ratios[:10]
                
                if top_queries:
                    html += """
            <h3>Top 10 Queries with Largest Performance Differences</h3>
            <table>
                <tr>
                    <th>Query</th>
                    <th>Fastest Format</th>
                    <th>Slowest Format</th>
                    <th>Performance Ratio</th>
                    <th>Fastest Time (s)</th>
                    <th>Slowest Time (s)</th>
                </tr>
"""
                    
                    for query_key, ratio, comparison in top_queries:
                        metrics = comparison['performance_metrics']
                        html += f"""
                <tr>
                    <td>{query_key.upper()}</td>
                    <td>{metrics['fastest_format'].upper()}</td>
                    <td>{metrics['slowest_format'].upper()}</td>
                    <td>{ratio:.2f}x</td>
                    <td>{metrics['fastest_time']:.3f}</td>
                    <td>{metrics['slowest_time']:.3f}</td>
                </tr>
"""
                    
                    html += """
            </table>
"""
            
            # Optimization opportunities
            if 'optimization_opportunities' in detailed_comparison and detailed_comparison['optimization_opportunities']:
                html += """
            <h3>Optimization Opportunities</h3>
            <ul>
"""
                for opp in detailed_comparison['optimization_opportunities']:
                    priority_color = "red" if opp['priority'] == 'high' else "orange" if opp['priority'] == 'medium' else "green"
                    html += f"""
                <li style="color: {priority_color};">
                    <strong>{opp['query'].upper()}</strong> - {opp['description']} 
                    <span style="font-size: 0.8em;">(Priority: {opp['priority']})</span>
                </li>
"""
                html += "</ul>"
            
            html += """
        </div>
    </div>
"""
        
        return html

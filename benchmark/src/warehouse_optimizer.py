#!/usr/bin/env python3
"""
Warehouse Optimizer for TPC-DS Performance Testing
Analyzes warehouse utilization and recommends optimal warehouse sizes
"""

import logging
from typing import Dict, List, Any, Optional
import statistics

logger = logging.getLogger(__name__)

class WarehouseOptimizer:
    """Analyzes warehouse utilization and provides optimization recommendations"""
    
    # Warehouse size multipliers for cost calculation
    WAREHOUSE_SIZE_MULTIPLIERS = {
        'X-Small': 1,
        'XSMALL': 1,
        'Small': 2,
        'SMALL': 2,
        'Medium': 4,
        'MEDIUM': 4,
        'Large': 8,
        'LARGE': 8,
        'X-Large': 16,
        'XLARGE': 16,
        '2X-Large': 32,
        '2XLARGE': 32,
        '3X-Large': 64,
        '3XLARGE': 64,
        '4X-Large': 128,
        '4XLARGE': 128
    }
    
    # Warehouse size order for recommendations
    WAREHOUSE_SIZES = ['X-Small', 'Small', 'Medium', 'Large', 'X-Large', '2X-Large', '3X-Large', '4X-Large']
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize warehouse optimizer"""
        self.config = config or {}
        self.credit_price = config.get('cost_pricing', {}).get('snowflake_credit_price_usd', 3.0)
    
    def analyze_warehouse_utilization(self, warehouse_metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze warehouse utilization metrics.
        
        Args:
            warehouse_metrics: List of warehouse metric dictionaries with:
                - warehouse_size: Warehouse size
                - execution_time_seconds: Execution time
                - credits_consumed: Credits consumed
                
        Returns:
            Analysis dictionary with recommendations
        """
        if not warehouse_metrics:
            return {'error': 'No warehouse metrics provided'}
        
        analysis = {
            'current_warehouse_size': warehouse_metrics[0].get('warehouse_size', 'X-Small'),
            'total_queries': len(warehouse_metrics),
            'total_execution_time': sum(m.get('execution_time_seconds', 0) for m in warehouse_metrics),
            'total_credits': sum(m.get('credits_consumed', 0) for m in warehouse_metrics),
            'avg_execution_time': statistics.mean([m.get('execution_time_seconds', 0) for m in warehouse_metrics]),
            'recommendations': [],
            'optimization_opportunities': []
        }
        
        # Analyze current warehouse size
        current_size = analysis['current_warehouse_size']
        avg_time = analysis['avg_execution_time']
        
        # Recommend warehouse size based on execution time
        recommended_size = self._recommend_warehouse_size(avg_time, current_size)
        
        if recommended_size != current_size:
            analysis['recommendations'].append({
                'type': 'warehouse_size',
                'current': current_size,
                'recommended': recommended_size,
                'reason': f'Average execution time ({avg_time:.2f}s) suggests {recommended_size} may be more cost-effective',
                'potential_savings': self._calculate_potential_savings(warehouse_metrics, current_size, recommended_size)
            })
        
        # Analyze query patterns
        slow_queries = [m for m in warehouse_metrics if m.get('execution_time_seconds', 0) > 5.0]
        if slow_queries:
            analysis['optimization_opportunities'].append({
                'type': 'slow_queries',
                'count': len(slow_queries),
                'description': f'{len(slow_queries)} queries exceed 5 seconds',
                'suggestion': 'Consider larger warehouse size for slow queries or optimize queries'
            })
        
        # Analyze credit consumption
        if analysis['total_credits'] > 0:
            analysis['avg_credits_per_query'] = analysis['total_credits'] / len(warehouse_metrics)
            analysis['total_cost_usd'] = analysis['total_credits'] * self.credit_price
        
        return analysis
    
    def _recommend_warehouse_size(self, avg_execution_time: float, current_size: str) -> str:
        """Recommend optimal warehouse size based on execution time"""
        
        # Simple heuristic: if queries are very fast, consider smaller warehouse
        # If queries are slow, consider larger warehouse
        if avg_execution_time < 0.5:
            # Very fast queries - consider smaller warehouse
            current_idx = self.WAREHOUSE_SIZES.index(current_size) if current_size in self.WAREHOUSE_SIZES else 0
            if current_idx > 0:
                return self.WAREHOUSE_SIZES[current_idx - 1]
        elif avg_execution_time > 10.0:
            # Slow queries - consider larger warehouse
            current_idx = self.WAREHOUSE_SIZES.index(current_size) if current_size in self.WAREHOUSE_SIZES else 0
            if current_idx < len(self.WAREHOUSE_SIZES) - 1:
                return self.WAREHOUSE_SIZES[current_idx + 1]
        
        return current_size
    
    def _calculate_potential_savings(self, warehouse_metrics: List[Dict[str, Any]], 
                                    current_size: str, recommended_size: str) -> Dict[str, float]:
        """Calculate potential cost savings from warehouse size change"""
        
        current_multiplier = self.WAREHOUSE_SIZE_MULTIPLIERS.get(current_size, 1)
        recommended_multiplier = self.WAREHOUSE_SIZE_MULTIPLIERS.get(recommended_size, 1)
        
        if current_multiplier == recommended_multiplier:
            return {'savings_usd': 0, 'savings_percentage': 0}
        
        # Calculate current and recommended costs
        current_credits = 0
        for metric in warehouse_metrics:
            exec_time = metric.get('execution_time_seconds', 0)
            current_credits += (current_multiplier * exec_time) / 3600
        
        recommended_credits = 0
        for metric in warehouse_metrics:
            exec_time = metric.get('execution_time_seconds', 0)
            recommended_credits += (recommended_multiplier * exec_time) / 3600
        
        current_cost = current_credits * self.credit_price
        recommended_cost = recommended_credits * self.credit_price
        savings = current_cost - recommended_cost
        
        return {
            'savings_usd': savings,
            'savings_percentage': (savings / current_cost * 100) if current_cost > 0 else 0,
            'current_cost_usd': current_cost,
            'recommended_cost_usd': recommended_cost
        }
    
    def test_warehouse_sizes(self, query_execution_times: List[float], 
                            current_size: str = 'X-Small') -> Dict[str, Any]:
        """Test different warehouse sizes and calculate costs"""
        
        results = {}
        
        for size in self.WAREHOUSE_SIZES:
            multiplier = self.WAREHOUSE_SIZE_MULTIPLIERS.get(size, 1)
            total_credits = sum((multiplier * exec_time) / 3600 for exec_time in query_execution_times)
            total_cost = total_credits * self.credit_price
            
            results[size] = {
                'multiplier': multiplier,
                'total_credits': total_credits,
                'total_cost_usd': total_cost,
                'avg_credits_per_query': total_credits / len(query_execution_times) if query_execution_times else 0
            }
        
        # Find optimal size (lowest cost)
        optimal_size = min(results.items(), key=lambda x: x[1]['total_cost_usd'])
        
        return {
            'warehouse_comparison': results,
            'optimal_size': optimal_size[0],
            'optimal_cost_usd': optimal_size[1]['total_cost_usd'],
            'current_size': current_size,
            'current_cost_usd': results.get(current_size, {}).get('total_cost_usd', 0),
            'potential_savings_usd': results.get(current_size, {}).get('total_cost_usd', 0) - optimal_size[1]['total_cost_usd']
        }
    
    def get_warehouse_recommendations(self, avg_execution_time: float,
                                     max_execution_time: float,
                                     total_queries: int,
                                     current_size: str = 'X-Small') -> List[str]:
        """Get warehouse size recommendations based on query characteristics"""
        
        recommendations = []
        
        # Check if warehouse is too small
        if max_execution_time > 30.0 and current_size in ['X-Small', 'Small']:
            recommendations.append(f"Consider upgrading to Medium or Large warehouse - max execution time ({max_execution_time:.2f}s) is high")
        
        # Check if warehouse is too large
        if avg_execution_time < 0.5 and current_size in ['Large', 'X-Large', '2X-Large']:
            recommendations.append(f"Consider downgrading to Small or Medium warehouse - average execution time ({avg_execution_time:.2f}s) is low")
        
        # Check for auto-suspend optimization
        if total_queries < 100:
            recommendations.append("Consider setting auto_suspend to 60 seconds for warehouses with low query frequency")
        
        # Check for multi-cluster optimization
        if total_queries > 1000 and current_size in ['Large', 'X-Large']:
            recommendations.append("Consider enabling multi-cluster for high query concurrency")
        
        return recommendations


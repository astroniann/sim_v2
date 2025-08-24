#!/usr/bin/env python3
"""
Detailed Network Analysis for Tiered Edge Caching
Focus on Latency, Packet Drops, and QoS Metrics
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DetailedNetworkAnalysis:
    def __init__(self, results_dir: str = "results"):
        self.results_dir = Path(results_dir)
        self.summary_df = None
        self.nodes_df = None
        self.network_df = None
        
    def load_data(self):
        """Load all simulation data"""
        logging.info("Loading simulation data...")
        
        # Load summary data
        summary_file = self.results_dir / "merged_summaries.csv"
        if summary_file.exists():
            self.summary_df = pd.read_csv(summary_file)
            logging.info(f"Loaded {len(self.summary_df)} summary records")
        
        # Load node data
        nodes_file = self.results_dir / "merged_nodes.csv"
        if nodes_file.exists():
            self.nodes_df = pd.read_csv(nodes_file)
            logging.info(f"Loaded {len(self.nodes_df)} node records")
        
        # Load network data
        network_file = self.results_dir / "merged_network.csv"
        if network_file.exists():
            self.network_df = pd.read_csv(network_file)
            logging.info(f"Loaded {len(self.network_df)} network records")
    
    def analyze_latency_performance(self):
        """Comprehensive latency analysis"""
        logging.info("Analyzing latency performance...")
        
        if self.summary_df is None:
            logging.error("No summary data available")
            return
        
        # Create latency analysis figure
        fig, axes = plt.subplots(2, 3, figsize=(20, 12))
        fig.suptitle('Comprehensive Latency Analysis', fontsize=16, fontweight='bold')
        
        # 1. Latency distribution by scenario
        ax1 = axes[0, 0]
        latency_metrics = ['latency_p50', 'latency_p95', 'latency_p99']
        scenario_latency = self.summary_df.groupby('scenario')[latency_metrics].mean()
        scenario_latency.plot(kind='bar', ax=ax1)
        ax1.set_title('Latency Distribution by Scenario')
        ax1.set_xlabel('Scenario')
        ax1.set_ylabel('Latency (μs)')
        ax1.legend(['P50', 'P95', 'P99'])
        ax1.grid(True, alpha=0.3)
        
        # 2. Latency vs cache configuration
        ax2 = axes[0, 1]
        if 'cache_config' in self.summary_df.columns:
            cache_latency = self.summary_df.groupby('cache_config')[latency_metrics].mean()
            cache_latency.plot(kind='bar', ax=ax2)
            ax2.set_title('Latency by Cache Configuration')
            ax2.set_xlabel('Cache Configuration')
            ax2.set_ylabel('Latency (μs)')
            ax2.legend(['P50', 'P95', 'P99'])
            ax2.grid(True, alpha=0.3)
        
        # 3. Latency stability (coefficient of variation)
        ax3 = axes[0, 2]
        latency_cv = self.summary_df.groupby('scenario')['latency_p50'].agg(['mean', 'std']).reset_index()
        latency_cv['cv'] = latency_cv['std'] / latency_cv['mean']
        ax3.bar(latency_cv['scenario'], latency_cv['cv'], alpha=0.7, color='orange')
        ax3.set_title('Latency Stability (Coefficient of Variation)')
        ax3.set_xlabel('Scenario')
        ax3.set_ylabel('CV (Lower is Better)')
        ax3.grid(True, alpha=0.3)
        
        # 4. Latency percentiles comparison
        ax4 = axes[1, 0]
        p50_data = self.summary_df['latency_p50'].values
        p95_data = self.summary_df['latency_p95'].values
        p99_data = self.summary_df['latency_p99'].values
        
        ax4.boxplot([p50_data, p95_data, p99_data], labels=['P50', 'P95', 'P99'])
        ax4.set_title('Latency Percentile Distribution')
        ax4.set_ylabel('Latency (μs)')
        ax4.grid(True, alpha=0.3)
        
        # 5. Latency vs hit ratio correlation
        ax5 = axes[1, 1]
        ax5.scatter(self.summary_df['hL'], self.summary_df['latency_p50'], alpha=0.6)
        ax5.set_xlabel('Local Hit Ratio')
        ax5.set_ylabel('P50 Latency (μs)')
        ax5.set_title('Latency vs Cache Hit Ratio')
        ax5.grid(True, alpha=0.3)
        
        # Add correlation coefficient
        corr = np.corrcoef(self.summary_df['hL'], self.summary_df['latency_p50'])[0, 1]
        ax5.text(0.05, 0.95, f'Correlation: {corr:.3f}', transform=ax5.transAxes, 
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        # 6. Latency improvement ratio (P99/P50)
        ax6 = axes[1, 2]
        self.summary_df['latency_ratio'] = self.summary_df['latency_p99'] / self.summary_df['latency_p50']
        ratio_by_scenario = self.summary_df.groupby('scenario')['latency_ratio'].mean()
        ax6.bar(ratio_by_scenario.index, ratio_by_scenario.values, alpha=0.7, color='green')
        ax6.set_title('Latency Tail Ratio (P99/P50)')
        ax6.set_xlabel('Scenario')
        ax6.set_ylabel('Ratio (Lower is Better)')
        ax6.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(self.results_dir / 'detailed_latency_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        logging.info("Latency analysis completed")
    
    def analyze_packet_loss_performance(self):
        """Comprehensive packet loss analysis"""
        logging.info("Analyzing packet loss performance...")
        
        if self.summary_df is None:
            logging.error("No summary data available")
            return
        
        # Create packet loss analysis figure
        fig, axes = plt.subplots(2, 3, figsize=(20, 12))
        fig.suptitle('Comprehensive Packet Loss Analysis', fontsize=16, fontweight='bold')
        
        # 1. Packet loss by scenario
        ax1 = axes[0, 0]
        loss_by_scenario = self.summary_df.groupby('scenario')['packet_loss'].agg(['mean', 'std']).reset_index()
        ax1.bar(loss_by_scenario['scenario'], loss_by_scenario['mean'], 
               yerr=loss_by_scenario['std'], capsize=5, alpha=0.7, color='red')
        ax1.set_title('Packet Loss Rate by Scenario')
        ax1.set_xlabel('Scenario')
        ax1.set_ylabel('Packet Loss Rate')
        ax1.grid(True, alpha=0.3)
        
        # 2. Packet loss vs cache configuration
        ax2 = axes[0, 1]
        if 'cache_config' in self.summary_df.columns:
            loss_by_cache = self.summary_df.groupby('cache_config')['packet_loss'].agg(['mean', 'std']).reset_index()
            ax2.bar(loss_by_cache['cache_config'], loss_by_cache['mean'], 
                   yerr=loss_by_cache['std'], capsize=5, alpha=0.7, color='orange')
            ax2.set_title('Packet Loss Rate by Cache Configuration')
            ax2.set_xlabel('Cache Configuration')
            ax2.set_ylabel('Packet Loss Rate')
            ax2.grid(True, alpha=0.3)
        
        # 3. Packet loss distribution
        ax3 = axes[0, 2]
        ax3.hist(self.summary_df['packet_loss'], bins=20, alpha=0.7, color='red', edgecolor='black')
        ax3.set_title('Packet Loss Rate Distribution')
        ax3.set_xlabel('Packet Loss Rate')
        ax3.set_ylabel('Frequency')
        ax3.grid(True, alpha=0.3)
        
        # 4. Packet loss vs throughput
        ax4 = axes[1, 0]
        ax4.scatter(self.summary_df['packet_loss'], self.summary_df['throughput_mbps'], alpha=0.6)
        ax4.set_xlabel('Packet Loss Rate')
        ax4.set_ylabel('Throughput (Mbps)')
        ax4.set_title('Packet Loss vs Throughput')
        ax4.grid(True, alpha=0.3)
        
        # Add correlation coefficient
        corr = np.corrcoef(self.summary_df['packet_loss'], self.summary_df['throughput_mbps'])[0, 1]
        ax4.text(0.05, 0.95, f'Correlation: {corr:.3f}', transform=ax4.transAxes, 
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        # 5. Packet loss vs latency
        ax5 = axes[1, 1]
        ax5.scatter(self.summary_df['packet_loss'], self.summary_df['latency_p50'], alpha=0.6)
        ax5.set_xlabel('Packet Loss Rate')
        ax5.set_ylabel('P50 Latency (μs)')
        ax5.set_title('Packet Loss vs Latency')
        ax5.grid(True, alpha=0.3)
        
        # Add correlation coefficient
        corr = np.corrcoef(self.summary_df['packet_loss'], self.summary_df['latency_p50'])[0, 1]
        ax5.text(0.05, 0.95, f'Correlation: {corr:.3f}', transform=ax5.transAxes, 
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        # 6. Packet loss stability
        ax6 = axes[1, 2]
        loss_stability = self.summary_df.groupby('scenario')['packet_loss'].agg(['mean', 'std']).reset_index()
        loss_stability['cv'] = loss_stability['std'] / loss_stability['mean']
        ax6.bar(loss_stability['scenario'], loss_stability['cv'], alpha=0.7, color='purple')
        ax6.set_title('Packet Loss Stability (CV)')
        ax6.set_xlabel('Scenario')
        ax6.set_ylabel('Coefficient of Variation')
        ax6.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(self.results_dir / 'detailed_packet_loss_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        logging.info("Packet loss analysis completed")
    
    def analyze_qos_metrics(self):
        """Quality of Service metrics analysis"""
        logging.info("Analyzing QoS metrics...")
        
        if self.summary_df is None:
            logging.error("No summary data available")
            return
        
        # Create QoS analysis figure
        fig, axes = plt.subplots(2, 3, figsize=(20, 12))
        fig.suptitle('Quality of Service (QoS) Analysis', fontsize=16, fontweight='bold')
        
        # 1. Overall QoS score (composite metric)
        ax1 = axes[0, 0]
        # Calculate composite QoS score: (hit_ratio * 0.4) + (1 - normalized_latency * 0.3) + (1 - packet_loss * 0.3)
        self.summary_df['qos_score'] = (
            self.summary_df['hL'] * 0.4 + 
            (1 - self.summary_df['latency_p50'] / self.summary_df['latency_p50'].max()) * 0.3 +
            (1 - self.summary_df['packet_loss']) * 0.3
        )
        
        qos_by_scenario = self.summary_df.groupby('scenario')['qos_score'].agg(['mean', 'std']).reset_index()
        ax1.bar(qos_by_scenario['scenario'], qos_by_scenario['mean'], 
               yerr=qos_by_scenario['std'], capsize=5, alpha=0.7, color='green')
        ax1.set_title('Composite QoS Score by Scenario')
        ax1.set_xlabel('Scenario')
        ax1.set_ylabel('QoS Score (Higher is Better)')
        ax1.grid(True, alpha=0.3)
        
        # 2. Task success rate
        ax2 = axes[0, 1]
        self.summary_df['task_success_rate'] = (
            self.summary_df['tasks_processed'] / 
            (self.summary_df['tasks_processed'] + self.summary_df['tasks_dropped'])
        )
        success_by_scenario = self.summary_df.groupby('scenario')['task_success_rate'].agg(['mean', 'std']).reset_index()
        ax2.bar(success_by_scenario['scenario'], success_by_scenario['mean'], 
               yerr=success_by_scenario['std'], capsize=5, alpha=0.7, color='blue')
        ax2.set_title('Task Success Rate by Scenario')
        ax2.set_xlabel('Scenario')
        ax2.set_ylabel('Success Rate')
        ax2.grid(True, alpha=0.3)
        
        # 3. System efficiency (local processing ratio)
        ax3 = axes[0, 2]
        efficiency_by_scenario = self.summary_df.groupby('scenario')['hL'].agg(['mean', 'std']).reset_index()
        ax3.bar(efficiency_by_scenario['scenario'], efficiency_by_scenario['mean'], 
               yerr=efficiency_by_scenario['std'], capsize=5, alpha=0.7, color='orange')
        ax3.set_title('System Efficiency (Local Hit Ratio)')
        ax3.set_xlabel('Scenario')
        ax3.set_ylabel('Local Hit Ratio')
        ax3.grid(True, alpha=0.3)
        
        # 4. QoS vs cache configuration
        ax4 = axes[1, 0]
        if 'cache_config' in self.summary_df.columns:
            qos_by_cache = self.summary_df.groupby('cache_config')['qos_score'].agg(['mean', 'std']).reset_index()
            ax4.bar(qos_by_cache['cache_config'], qos_by_cache['mean'], 
                   yerr=qos_by_cache['std'], capsize=5, alpha=0.7, color='purple')
            ax4.set_title('QoS Score by Cache Configuration')
            ax4.set_xlabel('Cache Configuration')
            ax4.set_ylabel('QoS Score')
            ax4.grid(True, alpha=0.3)
        
        # 5. Throughput efficiency
        ax5 = axes[1, 1]
        throughput_by_scenario = self.summary_df.groupby('scenario')['throughput_mbps'].agg(['mean', 'std']).reset_index()
        ax5.bar(throughput_by_scenario['scenario'], throughput_by_scenario['mean'], 
               yerr=throughput_by_scenario['std'], capsize=5, alpha=0.7, color='cyan')
        ax5.set_title('Network Throughput by Scenario')
        ax5.set_xlabel('Scenario')
        ax5.set_ylabel('Throughput (Mbps)')
        ax5.grid(True, alpha=0.3)
        
        # 6. QoS correlation matrix
        ax6 = axes[1, 2]
        qos_metrics = ['hL', 'latency_p50', 'packet_loss', 'throughput_mbps', 'task_success_rate', 'qos_score']
        correlation_matrix = self.summary_df[qos_metrics].corr()
        
        im = ax6.imshow(correlation_matrix, cmap='coolwarm', aspect='auto', vmin=-1, vmax=1)
        ax6.set_xticks(range(len(correlation_matrix.columns)))
        ax6.set_yticks(range(len(correlation_matrix.columns)))
        ax6.set_xticklabels(correlation_matrix.columns, rotation=45, ha='right')
        ax6.set_yticklabels(correlation_matrix.columns)
        
        # Add correlation values
        for i in range(len(correlation_matrix.columns)):
            for j in range(len(correlation_matrix.columns)):
                ax6.text(j, i, f'{correlation_matrix.iloc[i, j]:.2f}', 
                        ha='center', va='center', fontsize=8)
        
        ax6.set_title('QoS Metrics Correlation Matrix')
        plt.colorbar(im, ax=ax6)
        
        plt.tight_layout()
        plt.savefig(self.results_dir / 'detailed_qos_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        logging.info("QoS analysis completed")
    
    def generate_statistical_report(self):
        """Generate comprehensive statistical report"""
        logging.info("Generating statistical report...")
        
        if self.summary_df is None:
            logging.error("No summary data available")
            return
        
        report_file = self.results_dir / "detailed_network_analysis_report.txt"
        
        with open(report_file, 'w') as f:
            f.write("DETAILED NETWORK ANALYSIS REPORT\n")
            f.write("=" * 60 + "\n")
            f.write(f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Overall statistics
            f.write("OVERALL NETWORK PERFORMANCE:\n")
            f.write("-" * 30 + "\n")
            f.write(f"Total Runs: {len(self.summary_df)}\n")
            f.write(f"Average P50 Latency: {self.summary_df['latency_p50'].mean():.2f} ± {self.summary_df['latency_p50'].std():.2f} μs\n")
            f.write(f"Average P95 Latency: {self.summary_df['latency_p95'].mean():.2f} ± {self.summary_df['latency_p95'].std():.2f} μs\n")
            f.write(f"Average P99 Latency: {self.summary_df['latency_p99'].mean():.2f} ± {self.summary_df['latency_p99'].std():.2f} μs\n")
            f.write(f"Average Packet Loss Rate: {self.summary_df['packet_loss'].mean():.4f} ± {self.summary_df['packet_loss'].std():.4f}\n")
            f.write(f"Average Throughput: {self.summary_df['throughput_mbps'].mean():.2f} ± {self.summary_df['throughput_mbps'].std():.2f} Mbps\n")
            f.write(f"Average Local Hit Ratio: {self.summary_df['hL'].mean():.4f} ± {self.summary_df['hL'].std():.4f}\n\n")
            
            # Per-scenario analysis
            f.write("PER-SCENARIO NETWORK ANALYSIS:\n")
            f.write("-" * 30 + "\n")
            
            for scenario in sorted(self.summary_df['scenario'].unique()):
                sc_data = self.summary_df[self.summary_df['scenario'] == scenario]
                f.write(f"\nScenario {scenario}:\n")
                f.write(f"  Runs: {len(sc_data)}\n")
                f.write(f"  P50 Latency: {sc_data['latency_p50'].mean():.2f} ± {sc_data['latency_p50'].std():.2f} μs\n")
                f.write(f"  P95 Latency: {sc_data['latency_p95'].mean():.2f} ± {sc_data['latency_p95'].std():.2f} μs\n")
                f.write(f"  P99 Latency: {sc_data['latency_p99'].mean():.2f} ± {sc_data['latency_p99'].std():.2f} μs\n")
                f.write(f"  Packet Loss: {sc_data['packet_loss'].mean():.4f} ± {sc_data['packet_loss'].std():.4f}\n")
                f.write(f"  Throughput: {sc_data['throughput_mbps'].mean():.2f} ± {sc_data['throughput_mbps'].std():.2f} Mbps\n")
                f.write(f"  Hit Ratio: {sc_data['hL'].mean():.4f} ± {sc_data['hL'].std():.4f}\n")
                
                # Latency stability
                cv = sc_data['latency_p50'].std() / sc_data['latency_p50'].mean()
                f.write(f"  Latency Stability (CV): {cv:.4f}\n")
                
                # Task success rate
                success_rate = sc_data['tasks_processed'].sum() / (sc_data['tasks_processed'].sum() + sc_data['tasks_dropped'].sum())
                f.write(f"  Task Success Rate: {success_rate:.4f}\n")
            
            # Statistical significance tests
            f.write("\nSTATISTICAL SIGNIFICANCE TESTS:\n")
            f.write("-" * 30 + "\n")
            
            # Compare scenarios for latency
            scenarios = sorted(self.summary_df['scenario'].unique())
            for i in range(len(scenarios)):
                for j in range(i+1, len(scenarios)):
                    sc1_data = self.summary_df[self.summary_df['scenario'] == scenarios[i]]['latency_p50']
                    sc2_data = self.summary_df[self.summary_df['scenario'] == scenarios[j]]['latency_p50']
                    
                    if len(sc1_data) > 1 and len(sc2_data) > 1:
                        t_stat, p_value = stats.ttest_ind(sc1_data, sc2_data)
                        f.write(f"Scenario {scenarios[i]} vs {scenarios[j]} (P50 Latency): t={t_stat:.3f}, p={p_value:.4f}\n")
            
            # Performance rankings
            f.write("\nPERFORMANCE RANKINGS:\n")
            f.write("-" * 20 + "\n")
            
            # Best latency
            best_latency = self.summary_df.groupby('scenario')['latency_p50'].mean().idxmin()
            f.write(f"Best P50 Latency: Scenario {best_latency}\n")
            
            # Best packet loss
            best_loss = self.summary_df.groupby('scenario')['packet_loss'].mean().idxmin()
            f.write(f"Best Packet Loss Rate: Scenario {best_loss}\n")
            
            # Best throughput
            best_throughput = self.summary_df.groupby('scenario')['throughput_mbps'].mean().idxmax()
            f.write(f"Best Throughput: Scenario {best_throughput}\n")
            
            # Best hit ratio
            best_hit = self.summary_df.groupby('scenario')['hL'].mean().idxmax()
            f.write(f"Best Hit Ratio: Scenario {best_hit}\n")
            
            # Recommendations
            f.write("\nRECOMMENDATIONS:\n")
            f.write("-" * 15 + "\n")
            
            if self.summary_df['packet_loss'].mean() < 0.01:
                f.write("✓ Network conditions are excellent with low packet loss\n")
            elif self.summary_df['packet_loss'].mean() < 0.05:
                f.write("✓ Network conditions are good with acceptable packet loss\n")
            else:
                f.write("⚠ Network conditions need improvement - high packet loss detected\n")
            
            if self.summary_df['latency_p95'].mean() < 1000:  # 1ms
                f.write("✓ Latency performance is excellent for real-time applications\n")
            elif self.summary_df['latency_p95'].mean() < 5000:  # 5ms
                f.write("✓ Latency performance is good for most applications\n")
            else:
                f.write("⚠ Latency performance may need optimization for real-time applications\n")
            
            if self.summary_df['hL'].mean() > 0.7:
                f.write("✓ Edge caching is highly effective\n")
            elif self.summary_df['hL'].mean() > 0.5:
                f.write("✓ Edge caching is moderately effective\n")
            else:
                f.write("⚠ Edge caching effectiveness needs improvement\n")
        
        logging.info(f"Statistical report saved to {report_file}")
    
    def run_complete_analysis(self):
        """Run complete network analysis"""
        logging.info("Starting detailed network analysis...")
        
        self.load_data()
        self.analyze_latency_performance()
        self.analyze_packet_loss_performance()
        self.analyze_qos_metrics()
        self.generate_statistical_report()
        
        logging.info("Detailed network analysis completed!")

if __name__ == "__main__":
    analyzer = DetailedNetworkAnalysis()
    analyzer.run_complete_analysis()
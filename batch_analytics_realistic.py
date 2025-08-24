#!/usr/bin/env python3
"""
Comprehensive Realistic Batch Analytics for Tiered Edge Caching
"""

import os, subprocess, shutil, pathlib, csv, glob, itertools, pandas as pd, numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import time
import json
import logging
from typing import Dict, List, Tuple
import multiprocessing as mp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_analytics_realistic.log'),
        logging.StreamHandler()
    ]
)

class RealisticBatchAnalytics:
    def __init__(self):
        self.NS3_EXECUTABLE = "./ns3"
        self.OUT_DIR = pathlib.Path("results")
        self.SCENARIOS = [1, 2, 3, 4]
        self.REPLICAS = 3
        # No duration constraint - requests resolve naturally
        self.JOBS = min(4, mp.cpu_count())
        
        # Realistic configurations with different cache sizes
        self.CACHE_CONFIGS = {
            "small": {"mecCacheMB": 128, "fogCacheMB": 64, "cdnCacheMB": 512},
            "medium": {"mecCacheMB": 512, "fogCacheMB": 256, "cdnCacheMB": 2048},
            "large": {"mecCacheMB": 1024, "fogCacheMB": 512, "cdnCacheMB": 4096}
        }
        
        # Scenario configurations
        self.SCENARIO_CONFIGS = {
            1: {"users": 1000, "mecNodes": 0, "fogNodes": 0, "cdnNodes": 5, "mecMaxTasks": 0, "fogMaxTasks": 0, "cdnMaxTasks": 200},  # CDN-only
            2: {"users": 1000, "mecNodes": 10, "fogNodes": 0, "cdnNodes": 5, "mecMaxTasks": 100, "fogMaxTasks": 0, "cdnMaxTasks": 200},  # MEC+CDN
            3: {"users": 1000, "mecNodes": 0, "fogNodes": 20, "cdnNodes": 5, "mecMaxTasks": 0, "fogMaxTasks": 50, "cdnMaxTasks": 200},  # FOG+CDN
            4: {"users": 1000, "mecNodes": 10, "fogNodes": 20, "cdnNodes": 5, "mecMaxTasks": 100, "fogMaxTasks": 50, "cdnMaxTasks": 200}  # Full P2P
        }
        
        self.start_time = None
        self.completed_tasks = 0
        self.failed_tasks = 0
        self.total_tasks = len(self.SCENARIOS) * len(self.CACHE_CONFIGS) * self.REPLICAS
        
    def run_single_simulation(self, scenario: int, cache_config: str, replica: int) -> Dict:
        """Run a single realistic simulation"""
        tag = f"sc{scenario}_{cache_config}_rep{replica:03d}"
        seed = replica
        run = replica
        
        config = self.SCENARIO_CONFIGS[scenario]
        cache_config_data = self.CACHE_CONFIGS[cache_config]
        
        cmd = [
            self.NS3_EXECUTABLE, "run",
            f"scratch/tiered-edge-realistic "
            f"--scenario={scenario} "
            f"--runTag={tag} "
            f"--users={config['users']} "
            f"--requestsPerUser=10 "  # 10,000 total requests
            f"--mecNodes={config['mecNodes']} "
            f"--fogNodes={config['fogNodes']} "
            f"--cdnNodes={config['cdnNodes']} "
            f"--mecCacheMB={cache_config_data['mecCacheMB']} "
            f"--fogCacheMB={cache_config_data['fogCacheMB']} "
            f"--cdnCacheMB={cache_config_data['cdnCacheMB']} "
            f"--mecMaxTasks={config['mecMaxTasks']} "
            f"--fogMaxTasks={config['fogMaxTasks']} "
            f"--cdnMaxTasks={config['cdnMaxTasks']}"
        ]
        
        try:
            start = time.time()
            result = subprocess.run(
                cmd, 
                check=True, 
                capture_output=True, 
                text=True,
                                 timeout=None,  # No timeout - let simulation run naturally
                env=dict(os.environ, NS_GLOBAL_VALUE=f"RngSeed={seed}", NS_GLOBAL_VALUE2=f"RngRun={run}")
            )
            duration = time.time() - start
            
            return {
                'scenario': scenario,
                'cache_config': cache_config,
                'replica': replica,
                'status': 'success',
                'duration': duration,
                'tag': tag
            }
            
        except subprocess.TimeoutExpired:
            logging.error(f"Timeout for {tag}")
            return {'scenario': scenario, 'cache_config': cache_config, 'replica': replica, 'status': 'timeout', 'tag': tag}
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed {tag}: {e}")
            return {'scenario': scenario, 'cache_config': cache_config, 'replica': replica, 'status': 'failed', 'error': str(e), 'tag': tag}
        except Exception as e:
            logging.error(f"Unexpected error for {tag}: {e}")
            return {'scenario': scenario, 'cache_config': cache_config, 'replica': replica, 'status': 'error', 'error': str(e), 'tag': tag}

    def run_batch_simulations(self):
        """Run all realistic simulations"""
        logging.info(f"Starting realistic batch analysis")
        logging.info(f"Scenarios: {self.SCENARIOS}")
        logging.info(f"Cache configs: {list(self.CACHE_CONFIGS.keys())}")
        logging.info(f"Replicas per config: {self.REPLICAS}")
        logging.info(f"Total tasks: {self.total_tasks}")
        logging.info(f"Parallel jobs: {self.JOBS}")
        
        self.start_time = datetime.now()
        
        # Clean previous results
        shutil.rmtree(self.OUT_DIR, ignore_errors=True)
        self.OUT_DIR.mkdir(exist_ok=True)
        
        # Create tasks
        tasks = []
        for scenario in self.SCENARIOS:
            for cache_config in self.CACHE_CONFIGS.keys():
                for replica in range(1, self.REPLICAS + 1):
                    tasks.append((scenario, cache_config, replica))
        
        # Run with progress monitoring
        with ThreadPoolExecutor(max_workers=self.JOBS) as executor:
            future_to_task = {executor.submit(self.run_single_simulation, sc, cc, rep): (sc, cc, rep) 
                            for sc, cc, rep in tasks}
            
            for future in as_completed(future_to_task):
                result = future.result()
                if result['status'] == 'success':
                    self.completed_tasks += 1
                    logging.info(f"✓ {result['tag']} completed in {result['duration']:.1f}s")
                else:
                    self.failed_tasks += 1
                    logging.error(f"✗ {result['tag']} failed: {result.get('error', 'Unknown error')}")
                
                # Progress update
                total_completed = self.completed_tasks + self.failed_tasks
                if total_completed % 2 == 0:
                    elapsed = datetime.now() - self.start_time
                    if total_completed > 0:
                        remaining = (self.total_tasks - total_completed) * elapsed / total_completed
                        logging.info(f"Progress: {total_completed}/{self.total_tasks} "
                                   f"({total_completed/self.total_tasks*100:.1f}%) "
                                   f"ETA: {remaining}")
        
        logging.info(f"Batch completed. Success: {self.completed_tasks}, Failed: {self.failed_tasks}")

    def merge_and_analyze_results(self):
        """Comprehensive data analysis"""
        logging.info("Starting comprehensive data analysis...")
        
        # Find all summary files
        summary_files = glob.glob(str(self.OUT_DIR / "*" / "summary.csv"))
        nodes_files = glob.glob(str(self.OUT_DIR / "*" / "nodes.csv"))
        network_files = glob.glob(str(self.OUT_DIR / "*" / "network_metrics.csv"))
        
        logging.info(f"Found {len(summary_files)} summary files, {len(nodes_files)} node files, {len(network_files)} network files")
        
        # Merge summary data
        all_summaries = []
        for file in summary_files:
            try:
                df = pd.read_csv(file)
                # Extract scenario and cache config from filename
                parts = file.split('/')
                tag = parts[-2] if len(parts) > 1 else "unknown"
                df['tag'] = tag
                df['cache_config'] = tag.split('_')[1] if '_' in tag else "unknown"
                all_summaries.append(df)
            except Exception as e:
                logging.error(f"Error reading {file}: {e}")
        
        if not all_summaries:
            logging.error("No summary files found!")
            return
        
        summary_df = pd.concat(all_summaries, ignore_index=True)
        summary_df.to_csv(self.OUT_DIR / "merged_summaries.csv", index=False)
        
        # Merge node data
        all_nodes = []
        for file in nodes_files:
            try:
                df = pd.read_csv(file)
                parts = file.split('/')
                tag = parts[-2] if len(parts) > 1 else "unknown"
                df['tag'] = tag
                df['cache_config'] = tag.split('_')[1] if '_' in tag else "unknown"
                all_nodes.append(df)
            except Exception as e:
                logging.error(f"Error reading {file}: {e}")
        
        if all_nodes:
            nodes_df = pd.concat(all_nodes, ignore_index=True)
            nodes_df.to_csv(self.OUT_DIR / "merged_nodes.csv", index=False)
        else:
            nodes_df = pd.DataFrame()
        
        # Merge network data
        all_networks = []
        for file in network_files:
            try:
                df = pd.read_csv(file)
                parts = file.split('/')
                tag = parts[-2] if len(parts) > 1 else "unknown"
                df['tag'] = tag
                df['cache_config'] = tag.split('_')[1] if '_' in tag else "unknown"
                all_networks.append(df)
            except Exception as e:
                logging.error(f"Error reading {file}: {e}")
        
        if all_networks:
            network_df = pd.concat(all_networks, ignore_index=True)
            network_df.to_csv(self.OUT_DIR / "merged_network.csv", index=False)
        else:
            network_df = pd.DataFrame()
        
        # Create comprehensive analysis
        self.create_detailed_analysis(summary_df, nodes_df, network_df)
        self.create_visualizations(summary_df, nodes_df, network_df)
        self.create_statistical_report(summary_df, nodes_df, network_df)
        
        logging.info("Analysis completed!")

    def create_detailed_analysis(self, summary_df: pd.DataFrame, nodes_df: pd.DataFrame, network_df: pd.DataFrame):
        """Create detailed performance analysis"""
        analysis_file = self.OUT_DIR / "detailed_analysis.json"
        
        analysis = {
            "metadata": {
                "total_runs": len(summary_df),
                "scenarios": sorted(summary_df['scenario'].unique().tolist()),
                "cache_configs": sorted(summary_df['cache_config'].unique().tolist()),
                "analysis_timestamp": datetime.now().isoformat()
            },
            "overall_statistics": {
                "total_requests": int(summary_df['req'].sum()),
                "avg_latency_p50": float(summary_df['latency_p50'].mean()),
                "avg_latency_p95": float(summary_df['latency_p95'].mean()),
                "avg_latency_p99": float(summary_df['latency_p99'].mean()),
                "avg_packet_loss": float(summary_df['packet_loss'].mean()),
                "avg_throughput": float(summary_df['throughput_mbps'].mean()),
                "total_tasks_processed": int(summary_df['tasks_processed'].sum()),
                "total_tasks_dropped": int(summary_df['tasks_dropped'].sum())
            },
            "scenario_analysis": {},
            "cache_analysis": {}
        }
        
        # Per-scenario analysis
        for scenario in sorted(summary_df['scenario'].unique()):
            sc_data = summary_df[summary_df['scenario'] == scenario]
            
            analysis["scenario_analysis"][f"scenario_{scenario}"] = {
                "runs": len(sc_data),
                "avg_local_hit_ratio": float(sc_data['hL'].mean()),
                "avg_latency_p50": float(sc_data['latency_p50'].mean()),
                "avg_latency_p95": float(sc_data['latency_p95'].mean()),
                "avg_packet_loss": float(sc_data['packet_loss'].mean()),
                "avg_throughput": float(sc_data['throughput_mbps'].mean()),
                "tasks_processed": int(sc_data['tasks_processed'].sum()),
                "tasks_dropped": int(sc_data['tasks_dropped'].sum())
            }
        
        # Per-cache-config analysis
        for cache_config in sorted(summary_df['cache_config'].unique()):
            cc_data = summary_df[summary_df['cache_config'] == cache_config]
            
            analysis["cache_analysis"][cache_config] = {
                "runs": len(cc_data),
                "avg_local_hit_ratio": float(cc_data['hL'].mean()),
                "avg_latency_p50": float(cc_data['latency_p50'].mean()),
                "avg_latency_p95": float(cc_data['latency_p95'].mean()),
                "avg_packet_loss": float(cc_data['packet_loss'].mean()),
                "avg_throughput": float(cc_data['throughput_mbps'].mean())
            }
        
        with open(analysis_file, 'w') as f:
            json.dump(analysis, f, indent=2)
        
        logging.info(f"Detailed analysis saved to {analysis_file}")

    def create_visualizations(self, summary_df: pd.DataFrame, nodes_df: pd.DataFrame, network_df: pd.DataFrame):
        """Create comprehensive visualizations"""
        logging.info("Creating visualizations...")
        
        plt.style.use('default')
        sns.set_palette("husl")
        
        # 1. Performance comparison by scenario and cache config
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        fig.suptitle('Realistic Tiered Edge Caching Performance Analysis', fontsize=16, fontweight='bold')
        
        # Hit ratios
        ax1 = axes[0, 0]
        pivot_hit = summary_df.pivot_table(values='hL', index='scenario', columns='cache_config', aggfunc='mean')
        pivot_hit.plot(kind='bar', ax=ax1)
        ax1.set_title('Cache Hit Ratios by Scenario & Cache Size')
        ax1.set_xlabel('Scenario')
        ax1.set_ylabel('Local Hit Ratio')
        ax1.legend(title='Cache Config')
        ax1.grid(True, alpha=0.3)
        
        # Latency P50
        ax2 = axes[0, 1]
        pivot_lat50 = summary_df.pivot_table(values='latency_p50', index='scenario', columns='cache_config', aggfunc='mean')
        pivot_lat50.plot(kind='bar', ax=ax2)
        ax2.set_title('P50 Latency by Scenario & Cache Size')
        ax2.set_xlabel('Scenario')
        ax2.set_ylabel('P50 Latency (μs)')
        ax2.legend(title='Cache Config')
        ax2.grid(True, alpha=0.3)
        
        # Latency P95
        ax3 = axes[0, 2]
        pivot_lat95 = summary_df.pivot_table(values='latency_p95', index='scenario', columns='cache_config', aggfunc='mean')
        pivot_lat95.plot(kind='bar', ax=ax3)
        ax3.set_title('P95 Latency by Scenario & Cache Size')
        ax3.set_xlabel('Scenario')
        ax3.set_ylabel('P95 Latency (μs)')
        ax3.legend(title='Cache Config')
        ax3.grid(True, alpha=0.3)
        
        # Packet loss
        ax4 = axes[1, 0]
        pivot_loss = summary_df.pivot_table(values='packet_loss', index='scenario', columns='cache_config', aggfunc='mean')
        pivot_loss.plot(kind='bar', ax=ax4)
        ax4.set_title('Packet Loss Rate by Scenario & Cache Size')
        ax4.set_xlabel('Scenario')
        ax4.set_ylabel('Packet Loss Rate')
        ax4.legend(title='Cache Config')
        ax4.grid(True, alpha=0.3)
        
        # Throughput
        ax5 = axes[1, 1]
        pivot_tp = summary_df.pivot_table(values='throughput_mbps', index='scenario', columns='cache_config', aggfunc='mean')
        pivot_tp.plot(kind='bar', ax=ax5)
        ax5.set_title('Throughput by Scenario & Cache Size')
        ax5.set_xlabel('Scenario')
        ax5.set_ylabel('Throughput (Mbps)')
        ax5.legend(title='Cache Config')
        ax5.grid(True, alpha=0.3)
        
        # Tasks processed vs dropped
        ax6 = axes[1, 2]
        task_data = summary_df.groupby('scenario')[['tasks_processed', 'tasks_dropped']].mean()
        task_data.plot(kind='bar', ax=ax6)
        ax6.set_title('Tasks Processed vs Dropped by Scenario')
        ax6.set_xlabel('Scenario')
        ax6.set_ylabel('Number of Tasks')
        ax6.legend(['Processed', 'Dropped'])
        ax6.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(self.OUT_DIR / 'performance_comparison.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # 2. Network metrics analysis
        if not network_df.empty:
            fig, axes = plt.subplots(2, 2, figsize=(16, 12))
            fig.suptitle('Network Performance Analysis', fontsize=16, fontweight='bold')
            
            # Latency distribution
            ax1 = axes[0, 0]
            for scenario in sorted(network_df['scenario'].unique()):
                sc_data = network_df[network_df['scenario'] == scenario]
                ax1.scatter(sc_data['p50_latency_ms'], sc_data['p95_latency_ms'], 
                           alpha=0.6, label=f'Scenario {scenario}')
            ax1.set_xlabel('P50 Latency (ms)')
            ax1.set_ylabel('P95 Latency (ms)')
            ax1.set_title('Latency Distribution')
            ax1.legend()
            ax1.grid(True, alpha=0.3)
            
            # Packet loss vs throughput
            ax2 = axes[0, 1]
            for cache_config in sorted(network_df['cache_config'].unique()):
                cc_data = network_df[network_df['cache_config'] == cache_config]
                ax2.scatter(cc_data['packet_loss_rate'], cc_data['throughput_mbps'], 
                           alpha=0.6, label=cache_config)
            ax2.set_xlabel('Packet Loss Rate')
            ax2.set_ylabel('Throughput (Mbps)')
            ax2.set_title('Packet Loss vs Throughput')
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            
            # Latency by cache config
            ax3 = axes[1, 0]
            latency_data = network_df.groupby('cache_config')[['p50_latency_ms', 'p95_latency_ms', 'p99_latency_ms']].mean()
            latency_data.plot(kind='bar', ax=ax3)
            ax3.set_title('Latency by Cache Configuration')
            ax3.set_xlabel('Cache Configuration')
            ax3.set_ylabel('Latency (ms)')
            ax3.legend(['P50', 'P95', 'P99'])
            ax3.grid(True, alpha=0.3)
            
            # Throughput by scenario
            ax4 = axes[1, 1]
            throughput_data = network_df.groupby('scenario')['throughput_mbps'].agg(['mean', 'std']).reset_index()
            ax4.bar(throughput_data['scenario'], throughput_data['mean'], 
                   yerr=throughput_data['std'], capsize=5, alpha=0.7)
            ax4.set_title('Throughput by Scenario')
            ax4.set_xlabel('Scenario')
            ax4.set_ylabel('Throughput (Mbps)')
            ax4.grid(True, alpha=0.3)
            
            plt.tight_layout()
            plt.savefig(self.OUT_DIR / 'network_analysis.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        # 3. Node-level analysis
        if not nodes_df.empty:
            fig, axes = plt.subplots(2, 2, figsize=(16, 12))
            fig.suptitle('Node-Level Performance Analysis', fontsize=16, fontweight='bold')
            
            # Cache hit ratios by node type
            ax1 = axes[0, 0]
            node_hits = nodes_df.groupby('role')['hitRatio'].agg(['mean', 'std']).reset_index()
            ax1.bar(node_hits['role'], node_hits['mean'], 
                   yerr=node_hits['std'], capsize=5, alpha=0.7)
            ax1.set_title('Cache Hit Ratios by Node Type')
            ax1.set_xlabel('Node Type')
            ax1.set_ylabel('Hit Ratio')
            ax1.grid(True, alpha=0.3)
            
            # Processing time by node type
            ax2 = axes[0, 1]
            proc_time = nodes_df.groupby('role')['avgProcessingTime'].agg(['mean', 'std']).reset_index()
            ax2.bar(proc_time['role'], proc_time['mean'], 
                   yerr=proc_time['std'], capsize=5, alpha=0.7)
            ax2.set_title('Average Processing Time by Node Type')
            ax2.set_xlabel('Node Type')
            ax2.set_ylabel('Processing Time (μs)')
            ax2.grid(True, alpha=0.3)
            
            # Tasks processed vs dropped by node type
            ax3 = axes[1, 0]
            task_stats = nodes_df.groupby('role')[['tasksProcessed', 'tasksDropped']].sum()
            task_stats.plot(kind='bar', ax=ax3)
            ax3.set_title('Tasks by Node Type')
            ax3.set_xlabel('Node Type')
            ax3.set_ylabel('Number of Tasks')
            ax3.legend(['Processed', 'Dropped'])
            ax3.grid(True, alpha=0.3)
            
            # Cache utilization by node type
            ax4 = axes[1, 1]
            cache_util = nodes_df.groupby('role').apply(
                lambda x: x['cacheSz'].sum() / x['cacheCap'].sum()
            )
            ax4.plot(cache_util.index, cache_util.values, 'o-', linewidth=2, markersize=8)
            ax4.set_title('Cache Utilization by Node Type')
            ax4.set_xlabel('Node Type')
            ax4.set_ylabel('Cache Utilization Ratio')
            ax4.grid(True, alpha=0.3)
            
            plt.tight_layout()
            plt.savefig(self.OUT_DIR / 'node_analysis.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        logging.info("Visualizations completed!")

    def create_statistical_report(self, summary_df: pd.DataFrame, nodes_df: pd.DataFrame, network_df: pd.DataFrame):
        """Create comprehensive statistical report"""
        report_file = self.OUT_DIR / "comprehensive_report.txt"
        
        with open(report_file, 'w') as f:
            f.write("REALISTIC TIERED EDGE CACHING - COMPREHENSIVE ANALYSIS REPORT\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total simulation runs: {len(summary_df)}\n")
            f.write(f"Scenarios analyzed: {sorted(summary_df['scenario'].unique())}\n")
            f.write(f"Cache configurations: {sorted(summary_df['cache_config'].unique())}\n")
            f.write(f"Total requests processed: {summary_df['req'].sum():,}\n")
            f.write(f"Total tasks processed: {summary_df['tasks_processed'].sum():,}\n")
            f.write(f"Total tasks dropped: {summary_df['tasks_dropped'].sum():,}\n\n")
            
            # Overall performance summary
            f.write("OVERALL PERFORMANCE SUMMARY:\n")
            f.write("-" * 40 + "\n")
            f.write(f"Average P50 Latency: {summary_df['latency_p50'].mean():.2f} μs\n")
            f.write(f"Average P95 Latency: {summary_df['latency_p95'].mean():.2f} μs\n")
            f.write(f"Average P99 Latency: {summary_df['latency_p99'].mean():.2f} μs\n")
            f.write(f"Average Packet Loss Rate: {summary_df['packet_loss'].mean():.4f}\n")
            f.write(f"Average Throughput: {summary_df['throughput_mbps'].mean():.2f} Mbps\n")
            f.write(f"Average Local Hit Ratio: {summary_df['hL'].mean():.4f}\n")
            f.write(f"Task Success Rate: {summary_df['tasks_processed'].sum() / (summary_df['tasks_processed'].sum() + summary_df['tasks_dropped'].sum()):.4f}\n\n")
            
            # Per-scenario analysis
            f.write("PER-SCENARIO DETAILED ANALYSIS:\n")
            f.write("-" * 40 + "\n")
            
            for scenario in sorted(summary_df['scenario'].unique()):
                sc_data = summary_df[summary_df['scenario'] == scenario]
                f.write(f"\nScenario {scenario}:\n")
                f.write(f"  Runs: {len(sc_data)}\n")
                f.write(f"  Avg Local Hit Ratio: {sc_data['hL'].mean():.4f} ± {sc_data['hL'].std():.4f}\n")
                f.write(f"  Avg P50 Latency: {sc_data['latency_p50'].mean():.2f} ± {sc_data['latency_p50'].std():.2f} μs\n")
                f.write(f"  Avg P95 Latency: {sc_data['latency_p95'].mean():.2f} ± {sc_data['latency_p95'].std():.2f} μs\n")
                f.write(f"  Avg Packet Loss: {sc_data['packet_loss'].mean():.4f} ± {sc_data['packet_loss'].std():.4f}\n")
                f.write(f"  Avg Throughput: {sc_data['throughput_mbps'].mean():.2f} ± {sc_data['throughput_mbps'].std():.2f} Mbps\n")
                f.write(f"  Tasks Processed: {sc_data['tasks_processed'].sum()}\n")
                f.write(f"  Tasks Dropped: {sc_data['tasks_dropped'].sum()}\n")
            
            # Per-cache-config analysis
            f.write("\nPER-CACHE-CONFIGURATION ANALYSIS:\n")
            f.write("-" * 40 + "\n")
            
            for cache_config in sorted(summary_df['cache_config'].unique()):
                cc_data = summary_df[summary_df['cache_config'] == cache_config]
                f.write(f"\nCache Configuration '{cache_config}':\n")
                f.write(f"  Runs: {len(cc_data)}\n")
                f.write(f"  Avg Local Hit Ratio: {cc_data['hL'].mean():.4f} ± {cc_data['hL'].std():.4f}\n")
                f.write(f"  Avg P50 Latency: {cc_data['latency_p50'].mean():.2f} ± {cc_data['latency_p50'].std():.2f} μs\n")
                f.write(f"  Avg P95 Latency: {cc_data['latency_p95'].mean():.2f} ± {cc_data['latency_p95'].std():.2f} μs\n")
                f.write(f"  Avg Packet Loss: {cc_data['packet_loss'].mean():.4f} ± {cc_data['packet_loss'].std():.4f}\n")
                f.write(f"  Avg Throughput: {cc_data['throughput_mbps'].mean():.2f} ± {cc_data['throughput_mbps'].std():.2f} Mbps\n")
            
            # Recommendations
            f.write("\nRECOMMENDATIONS:\n")
            f.write("-" * 20 + "\n")
            
            # Find best performing scenario
            best_hit = summary_df.groupby('scenario')['hL'].mean().idxmax()
            best_latency = summary_df.groupby('scenario')['latency_p50'].mean().idxmin()
            best_throughput = summary_df.groupby('scenario')['throughput_mbps'].mean().idxmax()
            
            f.write(f"Best Hit Ratio Performance: Scenario {best_hit}\n")
            f.write(f"Best Latency Performance: Scenario {best_latency}\n")
            f.write(f"Best Throughput Performance: Scenario {best_throughput}\n")
            
            # Find best cache configuration
            best_cache_hit = summary_df.groupby('cache_config')['hL'].mean().idxmax()
            best_cache_latency = summary_df.groupby('cache_config')['latency_p50'].mean().idxmin()
            
            f.write(f"Best Cache Config for Hit Ratio: {best_cache_hit}\n")
            f.write(f"Best Cache Config for Latency: {best_cache_latency}\n")
            
            # Performance insights
            f.write(f"\nPERFORMANCE INSIGHTS:\n")
            f.write("-" * 20 + "\n")
            
            if best_hit == 4:
                f.write("✓ Full P2P scenarios show superior cache hit performance\n")
            if summary_df['hL'].mean() > 0.5:
                f.write("✓ Edge caching is effectively serving most requests locally\n")
            if summary_df['packet_loss'].mean() < 0.01:
                f.write("✓ Network conditions are stable with low packet loss\n")
            if summary_df['tasks_processed'].sum() > summary_df['tasks_dropped'].sum():
                f.write("✓ Edge computing capacity is sufficient for most tasks\n")
            
            f.write(f"\nTotal analysis time: {datetime.now() - self.start_time}\n")
        
        logging.info(f"Comprehensive report saved to {report_file}")

    def run_complete_analysis(self):
        """Run the complete realistic analysis pipeline"""
        try:
            self.run_batch_simulations()
            self.merge_and_analyze_results()
            self.generate_final_summary()
        except Exception as e:
            logging.error(f"Analysis failed: {e}")
            raise

    def generate_final_summary(self):
        """Generate final summary"""
        summary_file = self.OUT_DIR / "FINAL_SUMMARY.txt"
        
        with open(summary_file, 'w') as f:
            f.write("REALISTIC TIERED EDGE CACHING - FINAL SUMMARY\n")
            f.write("=" * 60 + "\n")
            f.write(f"Analysis completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total simulation time: {datetime.now() - self.start_time}\n")
            f.write(f"Successful runs: {self.completed_tasks}\n")
            f.write(f"Failed runs: {self.failed_tasks}\n")
            f.write(f"Success rate: {self.completed_tasks/(self.completed_tasks+self.failed_tasks)*100:.1f}%\n\n")
            
            f.write("FILES GENERATED:\n")
            f.write("-" * 20 + "\n")
            f.write("merged_summaries.csv - Summary data\n")
            f.write("merged_nodes.csv - Node-level data\n")
            f.write("merged_network.csv - Network metrics\n")
            f.write("detailed_analysis.json - Detailed analysis\n")
            f.write("comprehensive_report.txt - Statistical report\n")
            f.write("performance_comparison.png - Performance charts\n")
            f.write("network_analysis.png - Network analysis\n")
            f.write("node_analysis.png - Node-level analysis\n")
            
            f.write(f"\nResults directory: {self.OUT_DIR.absolute()}\n")
        
        logging.info(f"Final summary saved to {summary_file}")

if __name__ == "__main__":
    analyzer = RealisticBatchAnalytics()
    analyzer.run_complete_analysis()
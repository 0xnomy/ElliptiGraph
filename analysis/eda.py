import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def generate_eda(df: pd.DataFrame, edges_df: pd.DataFrame, output_path: str) -> None:
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("\n📊 Starting Exploratory Data Analysis...")
    
    # Set style
    sns.set_style("whitegrid")
    plt.rcParams['figure.figsize'] = (12, 6)
    
    # 1. Class Distribution
    fig, ax = plt.subplots(1, 1, figsize=(10, 5))
    class_counts = df['class_label'].value_counts()
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A']
    class_counts.plot(kind='bar', ax=ax, color=colors[:len(class_counts)])
    ax.set_title('Transaction Class Distribution', fontsize=14, fontweight='bold')
    ax.set_xlabel('Class')
    ax.set_ylabel('Count')
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
    plt.tight_layout()
    plt.savefig(output_path / 'class_distribution.png', dpi=150, bbox_inches='tight')
    logger.info("✓ Generated class distribution plot")
    plt.close()
    
    # 2. Degree Histogram (in-degree and out-degree)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    in_degree = edges_df['txId2'].value_counts()
    out_degree = edges_df['txId1'].value_counts()
    
    ax1.hist(in_degree.values, bins=50, color='#4ECDC4', edgecolor='black', alpha=0.7)
    ax1.set_title('In-Degree Distribution', fontsize=12, fontweight='bold')
    ax1.set_xlabel('In-Degree')
    ax1.set_ylabel('Frequency')
    ax1.set_yscale('log')
    
    ax2.hist(out_degree.values, bins=50, color='#FF6B6B', edgecolor='black', alpha=0.7)
    ax2.set_title('Out-Degree Distribution', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Out-Degree')
    ax2.set_ylabel('Frequency')
    ax2.set_yscale('log')
    
    plt.tight_layout()
    plt.savefig(output_path / 'degree_distribution.png', dpi=150, bbox_inches='tight')
    logger.info("✓ Generated degree distribution plot")
    plt.close()
    
    # 3. BTC Total / Time Step Trend
    if 'Time step' in df.columns and 'out_BTC_total' in df.columns:
        fig, ax = plt.subplots(figsize=(12, 5))
        btc_by_time = df.groupby('Time step')['out_BTC_total'].sum()
        ax.plot(btc_by_time.index, btc_by_time.values, linewidth=2, color='#45B7D1', marker='o', markersize=4)
        ax.set_title('Total BTC Value by Time Step', fontsize=14, fontweight='bold')
        ax.set_xlabel('Time Step')
        ax.set_ylabel('Total BTC')
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(output_path / 'btc_trend.png', dpi=150, bbox_inches='tight')
        logger.info("✓ Generated BTC trend plot")
        plt.close()
    
    # 4. Correlation Heatmap (first 20 features)
    feature_cols = [col for col in df.columns 
                   if col.startswith('Local_') or col.startswith('Aggregate_')][:20]
    
    if len(feature_cols) > 1:
        fig, ax = plt.subplots(figsize=(12, 10))
        corr_matrix = df[feature_cols].corr()
        sns.heatmap(corr_matrix, annot=False, cmap='coolwarm', center=0, ax=ax, cbar_kws={'label': 'Correlation'})
        ax.set_title('Feature Correlation Heatmap (First 20 Features)', fontsize=14, fontweight='bold')
        plt.tight_layout()
        plt.savefig(output_path / 'correlation_heatmap.png', dpi=150, bbox_inches='tight')
        logger.info("✓ Generated correlation heatmap")
        plt.close()
    
    # 5. Summary Statistics
    summary_file = output_path / 'summary_statistics.txt'
    with open(summary_file, 'w') as f:
        f.write("=" * 60 + "\n")
        f.write("TRANSACTION DATASET - SUMMARY STATISTICS\n")
        f.write("=" * 60 + "\n\n")
        
        f.write(f"Total Transactions: {len(df)}\n")
        f.write(f"Total Edges: {len(edges_df)}\n")
        f.write(f"Number of Features: {len(feature_cols)}\n")
        f.write(f"Time Steps: {df['Time step'].min()} to {df['Time step'].max()}\n\n")
        
        f.write("Class Distribution:\n")
        for class_label, count in df['class_label'].value_counts().items():
            pct = (count / len(df)) * 100
            f.write(f"  {class_label}: {count} ({pct:.2f}%)\n")
        
        f.write("\n" + "-" * 60 + "\n")
        f.write("Feature Statistics (Sample):\n")
        f.write("-" * 60 + "\n")
        f.write(df[feature_cols[:5]].describe().to_string())
        
    logger.info("✓ Generated summary statistics")
    logger.info(f"✅ EDA complete. Results saved to {output_path}")

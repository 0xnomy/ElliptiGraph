import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def load_dataset(dataset_path: str) -> tuple:
    dataset_path = Path(dataset_path)
    
    # Check if files exist
    classes_file = dataset_path / "txs_classes.csv"
    edges_file = dataset_path / "txs_edgelist.csv"
    features_file = dataset_path / "txs_features.csv"
    
    if not all([classes_file.exists(), edges_file.exists(), features_file.exists()]):
        missing = []
        if not classes_file.exists():
            missing.append("txs_classes.csv")
        if not edges_file.exists():
            missing.append("txs_edgelist.csv")
        if not features_file.exists():
            missing.append("txs_features.csv")
        raise FileNotFoundError(
            f"📂 Missing dataset files: {', '.join(missing)}. "
            f"Please place them in the /dataset folder."
        )
    
    try:
        logger.info("Loading dataset files...")
        classes_df = pd.read_csv(classes_file)
        edges_df = pd.read_csv(edges_file)
        features_df = pd.read_csv(features_file)
        
        logger.info(f"✓ Loaded {len(features_df)} transactions with features")
        logger.info(f"✓ Loaded {len(edges_df)} edges")
        logger.info(f"✓ Loaded {len(classes_df)} transaction classes")
        
        # Merge on txId
        merged_df = features_df.merge(classes_df, on='txId', how='left')
        
        return features_df, edges_df, classes_df, merged_df
        
    except Exception as e:
        raise RuntimeError(f"Error loading dataset: {str(e)}")


def preprocess_data(merged_df: pd.DataFrame, features_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("\n🔧 Starting preprocessing...")
    
    # Get feature columns (exclude txId, Time step, and class)
    feature_cols = [col for col in merged_df.columns 
                   if col not in ['txId', 'Time step', 'class']]
    
    # Handle missing class values (assign as 'Unknown' = 0)
    class_series = merged_df['class'].fillna(0).astype(int)
    
    # Handle missing values in features
    missing_count_before = merged_df[feature_cols].isnull().sum().sum()
    features_filled = merged_df[feature_cols].fillna(merged_df[feature_cols].mean())
    missing_count_after = features_filled.isnull().sum().sum()
    logger.info(f"✓ Filled {missing_count_before} missing values (now: {missing_count_after})")
    
    # Remove infinite values
    features_clean = features_filled.replace([np.inf, -np.inf], np.nan)
    features_clean = features_clean.fillna(features_clean.mean())
    
    # Normalize features using StandardScaler
    scaler = StandardScaler()
    features_normalized = pd.DataFrame(
        scaler.fit_transform(features_clean),
        columns=feature_cols,
        index=merged_df.index
    )
    logger.info(f"✓ Normalized {len(feature_cols)} features using StandardScaler")
    
    # Encode class labels efficiently
    class_mapping = {0: 'Unknown', 1: 'Licit', 2: 'Illicit', 3: 'Suspected'}
    class_label = class_series.map(class_mapping).fillna('Unknown')
    logger.info(f"✓ Encoded class labels")
    
    # Combine all columns at once to avoid fragmentation
    df = pd.concat([
        merged_df[['txId', 'Time step']],
        class_series.rename('class'),
        features_normalized,
        class_label.rename('class_label')
    ], axis=1)
    
    logger.info(f"✓ Preprocessing complete: {len(df)} transactions")
    
    return df


def save_processed_data(df: pd.DataFrame, output_path: str) -> None:
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    
    output_file = output_path / "processed_features.csv"
    df.to_csv(output_file, index=False)
    logger.info(f"✓ Saved processed features to {output_file}")


def get_feature_columns(df: pd.DataFrame) -> list:
    exclude_cols = {'txId', 'Time step', 'class', 'class_label'}
    return [col for col in df.columns if col not in exclude_cols]

import pandas as pd
import time
import logging
from typing import Tuple

logger = logging.getLogger(__name__)


class StreamingIngestor:
    
    def __init__(self, db_manager, edges_df: pd.DataFrame, processed_df: pd.DataFrame):
        self.db = db_manager
        self.edges_df = edges_df
        self.processed_df = processed_df
    
    def stream_by_time_step(self, sleep_seconds: float = 0.05, sample_size: int = None) -> Tuple[int, int]:
        print("\n⚡ Starting streaming ingestion...")

        time_steps = sorted(self.processed_df['Time step'].unique())

        if isinstance(sample_size, int):
            time_steps = time_steps[:sample_size]
            print(f"📊 Processing {len(time_steps)} time steps (sample)")
        else:
            print(f"📊 Processing {len(time_steps)} time steps (all)")

        print("─" * 60)

        total_tx_inserted = 0
        total_edges_inserted = 0

        try:
            for i, time_step in enumerate(time_steps):
                # Get transactions for this time step
                ts_data = self.processed_df[self.processed_df['Time step'] == time_step]

                # Insert transactions
                tx_inserted = self._insert_transactions(ts_data)
                total_tx_inserted += tx_inserted

                # Insert edges for transactions in this time step
                edges_inserted = self._insert_edges(ts_data['txId'].values)
                total_edges_inserted += edges_inserted

                # Print progress every few steps
                if (i + 1) % 5 == 0 or (i + 1) == len(time_steps):
                    print(
                        f"⚡ Step {i+1}/{len(time_steps)} (Time step {time_step}): "
                        f"+{tx_inserted} tx, +{edges_inserted} edges | "
                        f"Total: {total_tx_inserted:,} tx, {total_edges_inserted:,} edges"
                    )

                # Fast sleep to avoid long runs; user can adjust via parameter
                time.sleep(sleep_seconds)

        except KeyboardInterrupt:
            print("\n⚠️ Streaming interrupted by user")

        print("─" * 60)
        print(f"✅ Ingestion complete: {total_tx_inserted:,} tx, {total_edges_inserted:,} edges")

        return total_tx_inserted, total_edges_inserted
    
    def _insert_transactions(self, ts_data: pd.DataFrame) -> int:
        feature_cols = [col for col in ts_data.columns 
                       if col not in ['txId', 'Time step', 'class', 'class_label']]
        
        # Prepare batch data
        batch_transactions = []
        for _, row in ts_data.iterrows():
            features = {}
            for col in feature_cols:
                try:
                    features[col] = float(row[col])
                except (TypeError, ValueError):
                    features[col] = 0.0
            
            batch_transactions.append({
                '_key': str(row['txId']),
                'time_step': int(row['Time step']),
                'class': int(row['class']),
                'features': features
            })
        
        # Batch insert
        return self.db.batch_insert_transactions(batch_transactions)
    
    def _insert_edges(self, tx_ids: list) -> int:
        tx_ids_set = set(tx_ids)
        
        # Prepare batch edges
        batch_edges = []
        for _, edge in self.edges_df.iterrows():
            if edge['txId1'] in tx_ids_set or edge['txId2'] in tx_ids_set:
                batch_edges.append({
                    '_from': f'transactions/{edge["txId1"]}',
                    '_to': f'transactions/{edge["txId2"]}'
                })
        
        # Batch insert
        return self.db.batch_insert_edges(batch_edges)

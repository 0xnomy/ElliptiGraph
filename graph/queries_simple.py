import pandas as pd
from pathlib import Path
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)


class SimpleQueries:
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.results = {}
    
    def query_1_count_by_class(self) -> Dict:
        query = """
        FOR tx IN transactions
            COLLECT class = tx.class INTO group
            RETURN {
                class: class,
                count: LENGTH(group),
                class_name: class == 0 ? 'Unknown' :
                           class == 1 ? 'Licit' :
                           class == 2 ? 'Illicit' :
                           class == 3 ? 'Suspected' : 'Unknown'
            }
        """
        results = self.db.aql_query(query)
        self.results['query_1'] = {
            'name': 'Count Transactions per Class',
            'query': query,
            'results': results
        }
        return results
    
    def query_2_outgoing_edges(self, tx_id: str = None) -> Dict:
        if tx_id is None:
            sample_query = "FOR tx IN transactions LIMIT 1 RETURN tx._key"
            sample = self.db.aql_query(sample_query)
            if sample:
                tx_id = sample[0]
            else:
                return {'error': 'No transactions found'}
        
        query = """
        FOR edge IN tx_edges
            FILTER edge._from == @tx_id
            RETURN {
                from: edge._from,
                to: edge._to
            }
        """
        results = self.db.aql_query(query, {'tx_id': f'transactions/{tx_id}'})
        self.results['query_2'] = {
            'name': f'Outgoing Edges from Transaction {tx_id}',
            'query': query,
            'sample_tx_id': tx_id,
            'results': results
        }
        return results
    
    def query_3_avg_feature_by_class(self) -> Dict:
        query = """
        FOR tx IN transactions
            COLLECT class = tx.class INTO group
            RETURN {
                class: class,
                avg_time_step: AVG(group[*].tx.time_step),
                transaction_count: LENGTH(group)
            }
        """
        results = self.db.aql_query(query)
        self.results['query_3'] = {
            'name': 'Average Time Step by Class',
            'query': query,
            'results': results
        }
        return results
    
    def query_4_total_edges(self) -> Dict:
        query = """
        RETURN {
            total_edges: LENGTH(
                FOR edge IN tx_edges
                RETURN edge
            )
        }
        """
        results = self.db.aql_query(query)
        self.results['query_4'] = {
            'name': 'Total Number of Edges',
            'query': query,
            'results': results
        }
        return results
    
    def query_5_after_time_step(self, time_step: int = 10) -> Dict:
        query = """
        FOR tx IN transactions
            FILTER tx.time_step > @time_step
            COLLECT class = tx.class INTO group
            RETURN {
                class: class,
                count: LENGTH(group),
                min_time_step: MIN(group[*].tx.time_step),
                max_time_step: MAX(group[*].tx.time_step)
            }
        """
        results = self.db.aql_query(query, {'time_step': time_step})
        self.results['query_5'] = {
            'name': f'Transactions After Time Step {time_step}',
            'query': query,
            'time_step': time_step,
            'results': results
        }
        return results
    
    def execute_all(self) -> Dict:
        print("\n🔍 Executing 5 Simple Queries...")
        
        try:
            self.query_1_count_by_class()
            print("✅ Query 1: Count transactions per class")
        except Exception as e:
            print(f"❌ Query 1 failed: {e}")
        
        try:
            self.query_2_outgoing_edges()
            print("✅ Query 2: List outgoing edges")
        except Exception as e:
            print(f"❌ Query 2 failed: {e}")
        
        try:
            self.query_3_avg_feature_by_class()
            print("✅ Query 3: Average features by class")
        except Exception as e:
            print(f"❌ Query 3 failed: {e}")
        
        try:
            self.query_4_total_edges()
            print("✅ Query 4: Total edges")
        except Exception as e:
            print(f"❌ Query 4 failed: {e}")
        
        try:
            self.query_5_after_time_step()
            print("✅ Query 5: Transactions after time step")
        except Exception as e:
            print(f"❌ Query 5 failed: {e}")
        
        return self.results
    
    def save_results(self, output_path: str) -> None:
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)
        
        output_file = output_path / 'query_results_simple.csv'
        
        rows = []
        for query_name, query_data in self.results.items():
            query_num = query_name.split('_')[1]
            rows.append({
                'Query': query_num,
                'Name': query_data['name'],
                'Results_Count': len(query_data['results']),
                'Results_JSON': str(query_data['results'][:3]) if query_data['results'] else 'None'
            })
        
        df = pd.DataFrame(rows)
        df.to_csv(output_file, index=False)
        print(f"✅ Saved simple query results to {output_file}")

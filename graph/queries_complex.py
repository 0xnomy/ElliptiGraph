import pandas as pd
from pathlib import Path
import logging
from typing import Dict

logger = logging.getLogger(__name__)


class ComplexQueries:
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.results = {}
    
    def query_1_two_hop_neighbors(self, tx_id: str = None) -> Dict:
        if tx_id is None:
            sample_query = "FOR tx IN transactions LIMIT 1 RETURN tx._key"
            sample = self.db.aql_query(sample_query)
            if sample:
                tx_id = sample[0]
            else:
                return {'error': 'No transactions found'}
        
        query = """
        LET start_tx = @start_tx
        LET first_hop = (
            FOR edge1 IN tx_edges
            FILTER edge1._from == start_tx
            RETURN edge1._to
        )
        LET second_hop = (
            FOR hop1 IN first_hop
            FOR edge2 IN tx_edges
            FILTER edge2._from == hop1
            RETURN edge2._to
        )
        RETURN {
            source_tx: @start_tx,
            first_hop_count: LENGTH(first_hop),
            second_hop_count: LENGTH(second_hop),
            unique_second_hop: LENGTH(UNIQUE(second_hop))
        }
        """
        results = self.db.aql_query(query, {'start_tx': f'transactions/{tx_id}'})
        self.results['query_1'] = {
            'name': f'Two-Hop Neighbors from {tx_id}',
            'query': query,
            'results': results
        }
        return results
    
    def query_2_illicit_clusters(self) -> Dict:
        query = """
        FOR tx IN transactions
            FILTER tx.class == 2
            LET connected = (
                FOR edge IN tx_edges
                FILTER edge._from == tx._id OR edge._to == tx._id
                FOR neighbor IN transactions
                FILTER neighbor._id == (edge._from == tx._id ? edge._to : edge._from)
                RETURN neighbor
            )
            RETURN {
                illicit_tx: tx._key,
                time_step: tx.time_step,
                connected_count: LENGTH(connected),
                connected_classes: UNIQUE(connected[*].class)
            }
        """
        results = self.db.aql_query(query)
        self.results['query_2'] = {
            'name': 'Illicit Transaction Clusters',
            'query': query,
            'results': results
        }
        return results
    
    def query_3_temporal_patterns(self) -> Dict:
        query = """
        FOR tx IN transactions
            COLLECT time_step = tx.time_step, class = tx.class INTO group
            RETURN {
                time_step: time_step,
                class: class,
                transaction_count: LENGTH(group),
                edge_activity: LENGTH(
                    FOR edge IN tx_edges
                    FOR from_tx IN transactions
                    FILTER from_tx._id == edge._from AND from_tx.time_step == time_step
                    RETURN edge
                )
            }
        """
        results = self.db.aql_query(query)
        self.results['query_3'] = {
            'name': 'Temporal Transaction Patterns',
            'query': query,
            'results': results
        }
        return results
    
    def query_4_high_degree_nodes(self, min_degree: int = 5) -> Dict:
        query = """
        FOR tx IN transactions
            LET in_degree = LENGTH(
                FOR edge IN tx_edges
                FILTER edge._to == tx._id
                RETURN edge
            )
            LET out_degree = LENGTH(
                FOR edge IN tx_edges
                FILTER edge._from == tx._id
                RETURN edge
            )
            LET total_degree = in_degree + out_degree
            FILTER total_degree >= @min_degree
            RETURN {
                transaction: tx._key,
                class: tx.class,
                time_step: tx.time_step,
                in_degree: in_degree,
                out_degree: out_degree,
                total_degree: total_degree
            }
        """
        results = self.db.aql_query(query, {'min_degree': min_degree})
        self.results['query_4'] = {
            'name': f'High Degree Nodes (min degree: {min_degree})',
            'query': query,
            'results': results
        }
        return results
    
    def query_5_shortest_paths(self, from_tx: str = None, to_tx: str = None) -> Dict:
        if from_tx is None or to_tx is None:
            sample_query = "FOR tx IN transactions LIMIT 2 RETURN tx._key"
            sample = self.db.aql_query(sample_query)
            if len(sample) >= 2:
                from_tx = sample[0]
                to_tx = sample[1]
            else:
                return {'error': 'Not enough transactions found'}
        
        query = """
        FOR path IN K_SHORTEST_PATHS @from_tx TO @to_tx GRAPH 'tx_graph'
            OPTIONS {weightAttribute: null, defaultWeight: 1}
            LIMIT 3
            RETURN {
                path_length: LENGTH(path.edges),
                vertices: path.vertices[*]._key,
                vertex_classes: (
                    FOR v IN path.vertices
                    RETURN v.class
                )
            }
        """
        results = self.db.aql_query(query, {
            'from_tx': f'transactions/{from_tx}',
            'to_tx': f'transactions/{to_tx}'
        })
        self.results['query_5'] = {
            'name': f'Shortest Paths from {from_tx} to {to_tx}',
            'query': query,
            'results': results
        }
        return results
    
    def execute_all(self) -> Dict:
        print("\n🔬 Executing 5 Complex Queries...")
        
        try:
            self.query_1_two_hop_neighbors()
            print("✅ Query 1: Two-hop neighbors")
        except Exception as e:
            print(f"❌ Query 1 failed: {e}")
        
        try:
            self.query_2_illicit_clusters()
            print("✅ Query 2: Illicit clusters")
        except Exception as e:
            print(f"❌ Query 2 failed: {e}")
        
        try:
            self.query_3_temporal_patterns()
            print("✅ Query 3: Temporal patterns")
        except Exception as e:
            print(f"❌ Query 3 failed: {e}")
        
        try:
            self.query_4_high_degree_nodes()
            print("✅ Query 4: High degree nodes")
        except Exception as e:
            print(f"❌ Query 4 failed: {e}")
        
        try:
            self.query_5_shortest_paths()
            print("✅ Query 5: Shortest paths")
        except Exception as e:
            print(f"❌ Query 5 failed: {e}")
        
        return self.results
    
    def save_results(self, output_path: str) -> None:
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)
        
        output_file = output_path / 'query_results_complex.csv'
        
        rows = []
        for query_name, query_data in self.results.items():
            query_num = query_name.split('_')[1]
            rows.append({
                'Query': query_num,
                'Name': query_data['name'],
                'Results_Count': len(query_data['results']),
                'Results_JSON': str(query_data['results'][:2]) if query_data['results'] else 'None'
            })
        
        df = pd.DataFrame(rows)
        df.to_csv(output_file, index=False)
        print(f"✅ Saved complex query results to {output_file}")
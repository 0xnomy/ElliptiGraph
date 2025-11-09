import logging
from typing import Optional
from pyArango.connection import Connection

logger = logging.getLogger(__name__)


class ArangoDatabaseManager:
    
    def __init__(self, url: str, username: str, password: str):
        self.url = url
        self.username = username
        self.password = password
        self.db = None
        self.conn = None
        
    def connect(self) -> bool:
        try:
            print(f"🔌 Connecting to ArangoDB Docker container...")
            print(f"   📍 URL: {self.url}")
            print(f"   👤 User: {self.username}")
            
            self.conn = Connection(
                arangoURL=self.url,
                username=self.username,
                password=self.password
            )
            
            print("✅ Connected to ArangoDB Docker instance")
            return True
            
        except Exception as e:
            print(f"❌ Connection failed: {str(e)}")
            print("💡 Verify Docker container is running:")
            print("   docker ps | grep arangodb")
            return False
    
    def create_database(self, db_name: str) -> bool:
        try:
            if db_name in self.conn.databases:
                print(f"✅ Database '{db_name}' already exists")
            else:
                self.conn.createDatabase(name=db_name)
                print(f"✅ Created database '{db_name}'")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to create database: {str(e)}")
            return False
    
    def use_database(self, db_name: str) -> bool:
        try:
            self.db = self.conn[db_name]
            return True
        except Exception as e:
            print(f"❌ Failed to use database: {str(e)}")
            return False
    
    def create_graph_structure(self) -> bool:
        try:
            print("📦 Setting up graph structure...")
            
            # Create transactions collection
            if 'transactions' not in self.db.collections:
                self.db.createCollection(name='transactions')
                print("✅ Created 'transactions' collection")
            else:
                print("✅ 'transactions' collection exists")
            
            # Create tx_edges collection (edge type)
            if 'tx_edges' not in self.db.collections:
                self.db.createCollection(className='Edges', name='tx_edges')
                print("✅ Created 'tx_edges' collection")
            else:
                print("✅ 'tx_edges' collection exists")
            
            # Create graph
            if 'tx_graph' not in self.db.graphs:
                self.db.createGraph(name='tx_graph')
                graph = self.db.graphs['tx_graph']
                graph.createEdgeDefinition(
                    edgeCollection='tx_edges',
                    fromCollections=['transactions'],
                    toCollections=['transactions']
                )
                print("✅ Created graph 'tx_graph'")
            else:
                print("✅ Graph 'tx_graph' exists")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to create graph structure: {str(e)}")
            return False
    
    def batch_insert_transactions(self, transactions: list) -> int:
        try:
            collection = self.db['transactions']
            inserted = 0
            for tx in transactions:
                try:
                    collection.createDocument(tx)
                    inserted += 1
                except:
                    pass
            return inserted
        except Exception as e:
            print(f"Batch transaction insert failed: {e}")
            return 0
    
    def batch_insert_edges(self, edges: list) -> int:
        try:
            collection = self.db['tx_edges']
            inserted = 0
            for edge in edges:
                try:
                    collection.createDocument(edge)
                    inserted += 1
                except:
                    pass
            return inserted
        except Exception as e:
            print(f"Batch edge insert failed: {e}")
            return 0
    
    def aql_query(self, query: str, bind_vars: Optional[dict] = None) -> list:
        try:
            result = self.db.AQLQuery(query, bindVars=bind_vars or {}, rawResults=True)
            return list(result)
        except Exception as e:
            return []
    
    def get_collection_count(self, collection_name: str) -> int:
        try:
            return len(self.db[collection_name])
        except:
            return 0

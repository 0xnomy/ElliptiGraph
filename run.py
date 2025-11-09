import sys
import subprocess
from analysis.preprocessing import load_dataset, preprocess_data, save_processed_data
from analysis.eda import generate_eda
from graph.arango_setup import ArangoDatabaseManager
from ingestion.streaming_ingest import StreamingIngestor

def main():
    print("\n" + "═" * 70)
    print("🚀 ElliptiGraph - Bitcoin Transaction Analysis Dashboard")
    print("═" * 70)
    
    # Load dataset
    print("\n[1/5] 📂 Loading dataset...")
    try:
        features_df, edges_df, classes_df, merged_df = load_dataset("./dataset")
        print(f"      ✅ {len(features_df):,} transactions, {len(edges_df):,} edges")
    except Exception as e:
        print(f"      ❌ {e}")
        return
    
    # Preprocess data
    print("\n[2/5] 🔧 Preprocessing...")
    try:
        processed_df = preprocess_data(merged_df, features_df)
        save_processed_data(processed_df, "./output")
        print("      ✅ Data preprocessed")
    except Exception as e:
        print(f"      ❌ {e}")
        return
    
    # Generate EDA
    print("\n[3/5] 📊 Generating plots...")
    try:
        generate_eda(processed_df, edges_df, "./visualization/plots")
        print("      ✅ Plots saved")
    except Exception as e:
        print(f"      ⚠️  {e}")
    
    # Connect to ArangoDB
    print("\n[4/5] 🔌 Connecting to ArangoDB...")
    try:
        db = ArangoDatabaseManager("http://localhost:8529", "root", "root")
        if not db.connect():
            print("      ❌ Connection failed")
            print("      💡 Run: docker run -p 8529:8529 -e ARANGO_ROOT_PASSWORD=root arangodb/arangodb")
            return
        
        db.create_database("elliptic_graph")
        db.use_database("elliptic_graph")
        db.create_graph_structure()
        print("      ✅ ArangoDB ready")
    except Exception as e:
        print(f"      ❌ {e}")
        return
    
    # Ingest data
    print("\n[5/5] 📥 Ingesting to ArangoDB...")
    try:
        ingestor = StreamingIngestor(db, edges_df, processed_df)
        ingestor.stream_by_time_step(sleep_seconds=0.01, sample_size=None)
        print("      ✅ Ingestion complete")
    except Exception as e:
        print(f"      ⚠️  {e}")
    
    # Execute queries and save results
    print("\n[6/7] 🔍 Executing queries...")
    try:
        from graph.queries_simple import SimpleQueries
        from graph.queries_complex import ComplexQueries
        import json
        
        simple_q = SimpleQueries(db)
        complex_q = ComplexQueries(db)
        
        # Execute simple queries
        q1_results = simple_q.query_1_count_by_class()
        
        # Execute complex queries
        cq1_results = complex_q.query_1_two_hop_neighbors()
        cq2_results = complex_q.query_2_illicit_clusters()
        
        # Save results
        output_path = "./output"
        import pandas as pd
        
        if q1_results:
            pd.DataFrame(q1_results).to_csv(f"{output_path}/query_results_simple.csv", index=False)
        
        combined_complex = []
        if cq1_results:
            combined_complex.append({"Query": "Two-Hop Neighbors", "Result": str(cq1_results)})
        if cq2_results:
            for i, result in enumerate(cq2_results[:10]):
                combined_complex.append({"Query": "Illicit Clusters", "Index": i, "Result": str(result)})
        
        if combined_complex:
            pd.DataFrame(combined_complex).to_csv(f"{output_path}/query_results_complex.csv", index=False)
        
        print("      ✅ Query results saved")
    except Exception as e:
        print(f"      ⚠️  Query execution: {e}")
    
    # CRITICAL: Close DB connection to free resources before dashboard starts
    try:
        del db
        del ingestor
        print("\n[7/7] 🔌 Closing connections...")
        print("      ✅ Connections closed")
    except:
        pass
    
    # Force garbage collection to free memory
    import gc
    gc.collect()
    
    # Launch Dash Dashboard
    print("\n" + "═" * 70)
    print("🎨 Launching ElliptiGraph Dashboard")
    print("═" * 70)
    print("📍 URL: http://localhost:8050")
    print("✨ Modern Dash UI with live ArangoDB integration")
    print("🔍 Interactive query execution & network visualization")
    print("🔄 Press Ctrl+C to stop")
    print("═" * 70 + "\n")
    
    try:
        subprocess.run([
            sys.executable, 
            "visualization/dash_app.py"
        ])
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped")
    except Exception as e:
        print(f"\n⚠️ Dashboard launch error: {e}")
        print("💡 Try: python visualization/dash_app.py")

if __name__ == "__main__":
    main()

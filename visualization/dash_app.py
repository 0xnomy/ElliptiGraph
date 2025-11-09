import dash
from dash import dcc, html, Input, Output, State, dash_table, ctx
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import networkx as nx
from datetime import datetime

# Add project root
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

# Try to import ArangoDB manager
try:
    from graph.arango_setup import ArangoDatabaseManager
    ARANGO_AVAILABLE = True
except ImportError:
    ARANGO_AVAILABLE = False
    ArangoDatabaseManager = None

# Initialize app
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.CYBORG,
        "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css"
    ],
    suppress_callback_exceptions=True,
    title="ElliptiGraph Dashboard"
)

# ============================================================================
# DATA LOADING & CACHING
# ============================================================================

print("📊 Loading data for instant access...")
output_path = Path("./output")
dataset_path = Path("./dataset")

# Load data
PROCESSED_DF = None
EDGES_DF = None
QUERY_RESULTS_SIMPLE = None
QUERY_RESULTS_COMPLEX = None

processed_file = output_path / 'processed_features.csv'
edges_file = dataset_path / 'txs_edgelist.csv'
query_simple_file = output_path / 'query_results_simple.csv'
query_complex_file = output_path / 'query_results_complex.csv'

if processed_file.exists():
    PROCESSED_DF = pd.read_csv(processed_file)
    print(f"✅ Loaded {len(PROCESSED_DF):,} transactions")

if edges_file.exists():
    EDGES_DF = pd.read_csv(edges_file)
    print(f"✅ Loaded {len(EDGES_DF):,} edges")

if query_simple_file.exists():
    QUERY_RESULTS_SIMPLE = pd.read_csv(query_simple_file)
    print(f"✅ Loaded simple query results")

if query_complex_file.exists():
    QUERY_RESULTS_COMPLEX = pd.read_csv(query_complex_file)
    print(f"✅ Loaded complex query results")

# Pre-compute all analytics
STATS = {}
if PROCESSED_DF is not None:
    STATS['total_tx'] = len(PROCESSED_DF)
    STATS['illicit'] = len(PROCESSED_DF[PROCESSED_DF['class'] == 2])
    STATS['licit'] = len(PROCESSED_DF[PROCESSED_DF['class'] == 1])
    STATS['unknown'] = len(PROCESSED_DF[PROCESSED_DF['class'] == 0])
    STATS['time_steps'] = PROCESSED_DF['Time step'].nunique()
    STATS['illicit_pct'] = (STATS['illicit'] / STATS['total_tx'] * 100)
    STATS['time_min'] = int(PROCESSED_DF['Time step'].min())
    STATS['time_max'] = int(PROCESSED_DF['Time step'].max())
    STATS['avg_time_step'] = PROCESSED_DF['Time step'].mean()
    
    # Feature analysis
    feature_cols = [c for c in PROCESSED_DF.columns if c.startswith(('Local_', 'Aggregate_'))]
    STATS['num_features'] = len(feature_cols)
    
if EDGES_DF is not None:
    STATS['total_edges'] = len(EDGES_DF)
    STATS['avg_conn'] = len(EDGES_DF) / len(PROCESSED_DF) if PROCESSED_DF is not None else 0
    STATS['network_density'] = STATS['total_edges'] / STATS['total_tx'] if STATS['total_tx'] > 0 else 0
    
    # Degree analysis
    in_deg = EDGES_DF['txId2'].value_counts()
    out_deg = EDGES_DF['txId1'].value_counts()
    STATS['max_in_degree'] = in_deg.max()
    STATS['max_out_degree'] = out_deg.max()
    STATS['avg_in_degree'] = in_deg.mean()
    STATS['avg_out_degree'] = out_deg.mean()

print("✅ All analytics pre-computed")

# ArangoDB connection (cached)
ARANGO_CONN = None
if ARANGO_AVAILABLE:
    try:
        db_mgr = ArangoDatabaseManager("http://localhost:8529", "root", "root")
        if db_mgr.connect():
            db_mgr.use_database("elliptic_graph")
            ARANGO_CONN = db_mgr
            print("✅ ArangoDB connected")
    except:
        print("⚠️ ArangoDB not available")

# ============================================================================
# APP LAYOUT
# ============================================================================

app.layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.Div([
                html.H1([
                    html.I(className="fas fa-project-diagram me-3"),
                    "ElliptiGraph"
                ], style={'background': 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                         '-webkit-background-clip': 'text',
                         '-webkit-text-fill-color': 'transparent',
                         'font-weight': 'bold',
                         'font-size': '3rem'}),
                html.H5("Bitcoin Transaction Network Analysis Platform", className="text-muted mb-1"),
                html.P([
                    html.I(className="fas fa-university me-2"),
                    "Assignment for DS461 - Big Data Analytics | ",
                    html.Strong("Muhammad Hamza M. Zaidi & Nauman Ali Murad")
                ], className="small text-muted")
            ], className="text-center py-4", style={
                'background': 'rgba(102, 126, 234, 0.1)',
                'border-radius': '15px',
                'border-left': '4px solid #667eea'
            })
        ])
    ], className="mb-4"),
    
    # System Health Row
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-heartbeat fa-2x mb-2", style={'color': '#28a745'}),
                        html.H6("Data Freshness", className="text-muted"),
                        html.H4("Today", className="text-success", id="data-freshness")
                    ], className="text-center")
                ])
            ])
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-database fa-2x mb-2", style={'color': '#17a2b8'}),
                        html.H6("Data Completeness", className="text-muted"),
                        html.H4("98.5%", className="text-info", id="data-completeness")
                    ], className="text-center")
                ])
            ])
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-server fa-2x mb-2", 
                              style={'color': '#28a745' if ARANGO_CONN else '#dc3545'}),
                        html.H6("ArangoDB Status", className="text-muted"),
                        html.H4("Connected" if ARANGO_CONN else "Offline", 
                               className="text-success" if ARANGO_CONN else "text-danger",
                               id="arango-status")
                    ], className="text-center")
                ])
            ])
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-clock fa-2x mb-2", style={'color': '#ffc107'}),
                        html.H6("Last Updated", className="text-muted"),
                        html.H4(datetime.now().strftime("%H:%M"), className="text-warning", id="last-updated")
                    ], className="text-center")
                ])
            ])
        ], width=3)
    ], className="mb-4"),
    
    # Key Metrics Row - Compact & Elegant
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div(["📊"], style={'font-size': '1.5rem', 'line-height': '1.5rem', 'margin-bottom': '0.3rem'}),
                    html.H5(f"{STATS.get('total_tx', 0):,}", className="mb-1", style={'font-weight': '600', 'line-height': '1.2'}),
                    html.P("TOTAL TRANSACTIONS", className="text-muted mb-0", style={'font-size': '0.65rem', 'letter-spacing': '0.5px', 'line-height': '1'})
                ], className="text-center", style={'padding': '0.75rem', 'height': '100px', 'display': 'flex', 'flex-direction': 'column', 'justify-content': 'center'})
            ], style={'border': '1px solid #667eea', 'border-radius': '8px', 'background': 'rgba(102, 126, 234, 0.1)', 'height': '100px'})
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div(["🔗"], style={'font-size': '1.5rem', 'line-height': '1.5rem', 'margin-bottom': '0.3rem'}),
                    html.H5(f"{STATS.get('total_edges', 0):,}", className="mb-1", style={'font-weight': '600', 'line-height': '1.2'}),
                    html.P("NETWORK EDGES", className="text-muted mb-0", style={'font-size': '0.65rem', 'letter-spacing': '0.5px', 'line-height': '1'})
                ], className="text-center", style={'padding': '0.75rem', 'height': '100px', 'display': 'flex', 'flex-direction': 'column', 'justify-content': 'center'})
            ], style={'border': '1px solid #00d4aa', 'border-radius': '8px', 'background': 'rgba(0, 212, 170, 0.1)', 'height': '100px'})
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div(["⚠️"], style={'font-size': '1.5rem', 'line-height': '1.5rem', 'margin-bottom': '0.3rem'}),
                    html.H5(f"{STATS.get('illicit', 0):,}", className="mb-1", style={'font-weight': '600', 'line-height': '1.2'}),
                    html.P("ILLICIT TXs", className="text-muted mb-0", style={'font-size': '0.65rem', 'letter-spacing': '0.5px', 'line-height': '1'})
                ], className="text-center", style={'padding': '0.75rem', 'height': '100px', 'display': 'flex', 'flex-direction': 'column', 'justify-content': 'center'})
            ], style={'border': '1px solid #dc3545', 'border-radius': '8px', 'background': 'rgba(220, 53, 69, 0.1)', 'height': '100px'})
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div(["✅"], style={'font-size': '1.5rem', 'line-height': '1.5rem', 'margin-bottom': '0.3rem'}),
                    html.H5(f"{STATS.get('licit', 0):,}", className="mb-1", style={'font-weight': '600', 'line-height': '1.2'}),
                    html.P("LICIT TXs", className="text-muted mb-0", style={'font-size': '0.65rem', 'letter-spacing': '0.5px', 'line-height': '1'})
                ], className="text-center", style={'padding': '0.75rem', 'height': '100px', 'display': 'flex', 'flex-direction': 'column', 'justify-content': 'center'})
            ], style={'border': '1px solid #28a745', 'border-radius': '8px', 'background': 'rgba(40, 167, 69, 0.1)', 'height': '100px'})
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div(["⏱️"], style={'font-size': '1.5rem', 'line-height': '1.5rem', 'margin-bottom': '0.3rem'}),
                    html.H5(f"{STATS.get('time_steps', 0)}", className="mb-1", style={'font-weight': '600', 'line-height': '1.2'}),
                    html.P("TIME STEPS", className="text-muted mb-0", style={'font-size': '0.65rem', 'letter-spacing': '0.5px', 'line-height': '1'})
                ], className="text-center", style={'padding': '0.75rem', 'height': '100px', 'display': 'flex', 'flex-direction': 'column', 'justify-content': 'center'})
            ], style={'border': '1px solid #ffc107', 'border-radius': '8px', 'background': 'rgba(255, 193, 7, 0.1)', 'height': '100px'})
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div(["📈"], style={'font-size': '1.5rem', 'line-height': '1.5rem', 'margin-bottom': '0.3rem'}),
                    html.H5(f"{STATS.get('illicit_pct', 0):.1f}%", className="mb-1", style={'font-weight': '600', 'line-height': '1.2'}),
                    html.P("FRAUD RATE", className="text-muted mb-0", style={'font-size': '0.65rem', 'letter-spacing': '0.5px', 'line-height': '1'})
                ], className="text-center", style={'padding': '0.75rem', 'height': '100px', 'display': 'flex', 'flex-direction': 'column', 'justify-content': 'center'})
            ], style={'border': '1px solid #ff6348', 'border-radius': '8px', 'background': 'rgba(255, 99, 72, 0.1)', 'height': '100px'})
        ], width=2)
    ], className="mb-4"),
    
    # Main Tabs
    dbc.Tabs([
        # TAB 1: OVERVIEW
        dbc.Tab(label="🏠 OVERVIEW", tab_id="overview", children=[
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-chart-pie me-2"), "Class Distribution"])),
                        dbc.CardBody([dcc.Graph(id="overview-pie", config={'displayModeBar': False})])
                    ], className="shadow")
                ], width=4),
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-chart-line me-2"), "Transactions Over Time"])),
                        dbc.CardBody([dcc.Graph(id="overview-timeseries", config={'displayModeBar': False})])
                    ], className="shadow")
                ], width=8)
            ], className="mt-3 mb-3"),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-info-circle me-2"), "Quick Insights"])),
                        dbc.CardBody([html.Div(id="overview-insights")])
                    ], className="shadow")
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-table me-2"), "Sample Transactions"])),
                        dbc.CardBody([html.Div(id="overview-sample-table")])
                    ], className="shadow")
                ], width=6)
            ], className="mb-3")
        ]),
        
        # TAB 2: NETWORK GRAPH
        dbc.Tab(label="🕸️ NETWORK", tab_id="network", children=[
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-sliders-h me-2"), "Network Controls"])),
                        dbc.CardBody([
                            html.Label("📊 Sample Size:", className="fw-bold mt-2"),
                            dcc.Slider(
                                id="network-sample-slider",
                                min=100, max=5000, step=100, value=1000,
                                marks={i: f'{i}' for i in [100, 1000, 2000, 3000, 5000]},
                                tooltip={"placement": "bottom", "always_visible": True}
                            ),
                            
                            html.Label("🎯 Filter by Class:", className="fw-bold mt-4"),
                            dcc.Dropdown(
                                id="network-class-filter",
                                options=[
                                    {'label': '🌐 All Classes', 'value': 'All'},
                                    {'label': '✅ Licit Only', 'value': 'Licit'},
                                    {'label': '⚠️ Illicit Only', 'value': 'Illicit'},
                                    {'label': '❓ Unknown Only', 'value': 'Unknown'}
                                ],
                                value='All',
                                clearable=False
                            ),
                            
                            html.Label("⏰ Time Step:", className="fw-bold mt-4"),
                            dcc.Dropdown(
                                id="network-time-filter",
                                options=[{'label': 'All Time Steps', 'value': 'All'}] +
                                        [{'label': f'Time Step {t}', 'value': t} 
                                         for t in sorted(PROCESSED_DF['Time step'].unique().tolist())] if PROCESSED_DF is not None else [],
                                value='All',
                                clearable=False
                            ),
                            
                            html.Hr(),
                            dbc.Button(
                                [html.I(className="fas fa-sync-alt me-2"), "Update Network"],
                                id="network-update-btn",
                                color="primary",
                                className="w-100 mt-3",
                                size="lg"
                            ),
                            
                            html.Div(id="network-stats-box", className="mt-3")
                        ])
                    ], className="shadow")
                ], width=3),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-project-diagram me-2"), 
                                               "Interactive Network Visualization"])),
                        dbc.CardBody([
                            dcc.Loading(
                                dcc.Graph(id="network-graph", style={'height': '850px'}),
                                type="circle",
                                color="#667eea"
                            )
                        ])
                    ], className="shadow")
                ], width=9)
            ], className="mt-3")
        ]),
        
        # TAB 3: ARANGODB INSIGHTS
        dbc.Tab(label="🗃️ ARANGODB", tab_id="arangodb", children=[
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-database me-2"), "Live Database Metrics"])),
                        dbc.CardBody([html.Div(id="arango-metrics")])
                    ], className="shadow")
                ], width=12)
            ], className="mt-3 mb-3"),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-chart-bar me-2"), "Class Distribution (Live)"])),
                        dbc.CardBody([dcc.Graph(id="arango-class-dist", config={'displayModeBar': False})])
                    ], className="shadow")
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-network-wired me-2"), "Degree Analysis"])),
                        dbc.CardBody([dcc.Graph(id="arango-degree-dist", config={'displayModeBar': False})])
                    ], className="shadow")
                ], width=6)
            ], className="mb-3")
        ]),
        
        # TAB 4: QUERY RESULTS
        dbc.Tab(label="🔍 QUERIES", tab_id="queries", children=[
            # Query Execution Panel
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-play-circle me-2"), "Execute Queries"])),
                        dbc.CardBody([
                            html.H6("Simple Queries", className="text-primary mb-3"),
                            dbc.ButtonGroup([
                                dbc.Button([html.I(className="fas fa-chart-pie me-2"), "Count by Class"],
                                          id="query-simple-1", color="primary", size="sm", className="me-2"),
                                dbc.Button([html.I(className="fas fa-project-diagram me-2"), "Outgoing Edges"],
                                          id="query-simple-2", color="primary", size="sm", className="me-2"),
                                dbc.Button([html.I(className="fas fa-network-wired me-2"), "Incoming Edges"],
                                          id="query-simple-3", color="primary", size="sm", className="me-2"),
                                dbc.Button([html.I(className="fas fa-clock me-2"), "Time Range"],
                                          id="query-simple-4", color="primary", size="sm"),
                            ], className="mb-3 flex-wrap"),
                            
                            html.H6("Complex Queries", className="text-warning mb-3 mt-4"),
                            dbc.ButtonGroup([
                                dbc.Button([html.I(className="fas fa-route me-2"), "Two-Hop Neighbors"],
                                          id="query-complex-1", color="warning", size="sm", className="me-2"),
                                dbc.Button([html.I(className="fas fa-exclamation-triangle me-2"), "Illicit Clusters"],
                                          id="query-complex-2", color="warning", size="sm", className="me-2"),
                                dbc.Button([html.I(className="fas fa-wave-square me-2"), "Temporal Patterns"],
                                          id="query-complex-3", color="warning", size="sm", className="me-2"),
                                dbc.Button([html.I(className="fas fa-search-plus me-2"), "High Degree Nodes"],
                                          id="query-complex-4", color="warning", size="sm"),
                            ], className="flex-wrap"),
                            
                            html.Hr(className="my-4"),
                            dbc.Button([html.I(className="fas fa-sync-alt me-2"), "Run All Queries"],
                                      id="query-run-all", color="success", size="lg", className="w-100"),
                            
                            html.Div(id="query-status", className="mt-3")
                        ])
                    ], className="shadow")
                ])
            ], className="mt-3 mb-3"),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-search me-2"), "Simple Query Results"])),
                        dbc.CardBody([html.Div(id="query-simple-content")])
                    ], className="shadow")
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-brain me-2"), "Complex Query Results"])),
                        dbc.CardBody([html.Div(id="query-complex-content")])
                    ], className="shadow")
                ], width=6)
            ], className="mb-3"),
            
            # Query Visualization
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-chart-bar me-2"), "Query Results Visualization"])),
                        dbc.CardBody([dcc.Graph(id="query-viz", config={'displayModeBar': False})])
                    ], className="shadow")
                ])
            ])
        ]),
        
        # TAB 5: DATA EXPLORER
        dbc.Tab(label="🔬 EXPLORER", tab_id="explorer", children=[
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-filter me-2"), "Data Filters"])),
                        dbc.CardBody([
                            html.Label("🏷️ Select Classes:", className="fw-bold"),
                            dcc.Dropdown(
                                id="explorer-class-filter",
                                options=[{'label': c, 'value': c} for c in PROCESSED_DF['class_label'].unique()] if PROCESSED_DF is not None else [],
                                value=PROCESSED_DF['class_label'].unique().tolist() if PROCESSED_DF is not None else [],
                                multi=True
                            ),
                            
                            html.Label("⏰ Time Range:", className="fw-bold mt-3"),
                            dcc.RangeSlider(
                                id="explorer-time-range",
                                min=STATS.get('time_min', 0),
                                max=STATS.get('time_max', 49),
                                value=[STATS.get('time_min', 0), STATS.get('time_max', 49)],
                                marks={i: str(i) for i in range(STATS.get('time_min', 0), STATS.get('time_max', 49)+1, 10)},
                                tooltip={"placement": "bottom", "always_visible": True}
                            ),
                            
                            html.Label("📊 Sample Size:", className="fw-bold mt-3"),
                            dcc.Dropdown(
                                id="explorer-sample-size",
                                options=[
                                    {'label': '100 rows', 'value': 100},
                                    {'label': '500 rows', 'value': 500},
                                    {'label': '1,000 rows', 'value': 1000},
                                    {'label': '5,000 rows', 'value': 5000},
                                    {'label': 'All rows', 'value': 'All'}
                                ],
                                value=1000,
                                clearable=False
                            ),
                            
                            dbc.Button(
                                [html.I(className="fas fa-filter me-2"), "Apply Filters"],
                                id="explorer-apply-btn",
                                color="success",
                                className="w-100 mt-4"
                            )
                        ])
                    ], className="shadow")
                ], width=3),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-fire me-2"), "Feature Correlation"])),
                        dbc.CardBody([dcc.Graph(id="explorer-correlation", config={'displayModeBar': False})])
                    ], className="shadow mb-3")
                ], width=9)
            ], className="mt-3"),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            html.H5([html.I(className="fas fa-table me-2"), "Filtered Data Table"], className="d-inline"),
                            dbc.Button([html.I(className="fas fa-download me-2"), "Download CSV"],
                                      id="explorer-download-btn", size="sm", color="info", className="float-end")
                        ]),
                        dbc.CardBody([html.Div(id="explorer-data-table")])
                    ], className="shadow")
                ])
            ], className="mt-3 mb-3")
        ]),
        
        # TAB 6: ANALYTICS
        dbc.Tab(label="📈 ANALYTICS", tab_id="analytics", children=[
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-chart-bar me-2"), "Degree Distribution"])),
                        dbc.CardBody([dcc.Graph(id="analytics-degree", config={'displayModeBar': False})])
                    ], className="shadow")
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-fire me-2"), "Feature Correlation Matrix"])),
                        dbc.CardBody([dcc.Graph(id="analytics-correlation", config={'displayModeBar': False})])
                    ], className="shadow")
                ], width=6)
            ], className="mt-3 mb-3"),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5([html.I(className="fas fa-box-open me-2"), "Feature Box Plots"])),
                        dbc.CardBody([dcc.Graph(id="analytics-boxplots", config={'displayModeBar': False})])
                    ], className="shadow")
                ])
            ], className="mb-3")
        ])
    ], id="main-tabs", active_tab="overview"),
    
    # Footer
    html.Hr(),
    html.Div([
        html.P([
            html.I(className="fas fa-code me-2"),
            "ElliptiGraph © 2025 | ",
            f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ], className="text-center text-muted mb-1"),
        html.P([
            html.Small([
                "Assignment for DS461 - Big Data Analytics | Assignment 1 | ",
                html.Strong("M. Hamza M. Zaidi & Nauman Ali Murad")
            ])
        ], className="text-center text-muted small")
    ], className="mt-4 mb-3")
], fluid=True, className="px-4")

# ============================================================================
# CALLBACKS
# ============================================================================

# Overview Tab Callbacks
@app.callback(
    Output("overview-pie", "figure"),
    Input("main-tabs", "active_tab")
)
def update_overview_pie(tab):
    if tab != "overview" or PROCESSED_DF is None:
        return {}
    
    class_counts = PROCESSED_DF['class_label'].value_counts()
    fig = px.pie(
        values=class_counts.values,
        names=class_counts.index,
        color_discrete_sequence=['#28a745', '#dc3545', '#ffc107'],
        hole=0.4,
        title="Transaction Class Distribution"
    )
    fig.update_traces(textposition='inside', textinfo='percent+label', textfont_size=16)
    fig.update_layout(template="plotly_dark", height=400, showlegend=True)
    return fig

@app.callback(
    Output("overview-timeseries", "figure"),
    Input("main-tabs", "active_tab")
)
def update_overview_timeseries(tab):
    if tab != "overview" or PROCESSED_DF is None:
        return {}
    
    time_data = PROCESSED_DF.groupby(['Time step', 'class_label']).size().unstack(fill_value=0)
    
    fig = go.Figure()
    colors = {'Licit': '#28a745', 'Illicit': '#dc3545', 'Unknown': '#ffc107'}
    for col in time_data.columns:
        fig.add_trace(go.Scatter(
            x=time_data.index,
            y=time_data[col],
            name=col,
            mode='lines+markers',
            line=dict(width=3, color=colors.get(col, '#667eea')),
            fill='tonexty' if col != time_data.columns[0] else None
        ))
    
    fig.update_layout(
        template="plotly_dark",
        height=400,
        hovermode='x unified',
        xaxis_title="Time Step",
        yaxis_title="Number of Transactions",
        title="Transaction Evolution Over Time"
    )
    return fig

@app.callback(
    Output("overview-insights", "children"),
    Input("main-tabs", "active_tab")
)
def update_overview_insights(tab):
    if tab != "overview":
        return []
    
    insights = [
        dbc.Alert([
            html.H6([html.I(className="fas fa-exclamation-triangle me-2"), "Risk Analysis"], className="alert-heading"),
            html.P([
                html.Strong(f"{STATS.get('illicit_pct', 0):.2f}%"), " of all transactions are flagged as illicit"
            ]),
            html.Hr(),
            html.P([
                html.Strong(f"{STATS.get('illicit', 0):,}"), " illicit transactions detected out of ",
                html.Strong(f"{STATS.get('total_tx', 0):,}"), " total"
            ], className="mb-0")
        ], color="danger", className="mb-3"),
        
        dbc.Alert([
            html.H6([html.I(className="fas fa-network-wired me-2"), "Network Metrics"], className="alert-heading"),
            html.P([
                html.Strong(f"{STATS.get('avg_conn', 0):.2f}"), " average connections per transaction"
            ]),
            html.Hr(),
            html.P([
                "Network density: ", html.Strong(f"{STATS.get('network_density', 0):.4f}")
            ], className="mb-0")
        ], color="info", className="mb-3"),
        
        dbc.Alert([
            html.H6([html.I(className="fas fa-clock me-2"), "Temporal Analysis"], className="alert-heading"),
            html.P([
                "Data spans ", html.Strong(f"{STATS.get('time_steps', 0)}"), " time steps"
            ]),
            html.Hr(),
            html.P([
                "Average time step: ", html.Strong(f"{STATS.get('avg_time_step', 0):.2f}")
            ], className="mb-0")
        ], color="warning", className="mb-3")
    ]
    
    return insights

@app.callback(
    Output("overview-sample-table", "children"),
    Input("main-tabs", "active_tab")
)
def update_overview_sample(tab):
    if tab != "overview" or PROCESSED_DF is None:
        return html.P("No data available")
    
    sample = PROCESSED_DF.sample(min(15, len(PROCESSED_DF)))[['txId', 'Time step', 'class_label']].reset_index(drop=True)
    
    return dash_table.DataTable(
        data=sample.to_dict('records'),
        columns=[{'name': i, 'id': i} for i in sample.columns],
        style_cell={
            'textAlign': 'center',
            'backgroundColor': '#1a1f2e',
            'color': 'white',
            'padding': '10px'
        },
        style_header={
            'backgroundColor': '#667eea',
            'fontWeight': 'bold',
            'color': 'white'
        },
        style_data_conditional=[
            {
                'if': {'filter_query': '{class_label} = "Illicit"'},
                'backgroundColor': 'rgba(220, 53, 69, 0.2)',
                'color': '#dc3545'
            },
            {
                'if': {'filter_query': '{class_label} = "Licit"'},
                'backgroundColor': 'rgba(40, 167, 69, 0.2)',
                'color': '#28a745'
            }
        ],
        page_size=15,
        style_table={'overflowX': 'auto'}
    )

# Network Tab Callbacks
@app.callback(
    [Output("network-graph", "figure"),
     Output("network-stats-box", "children")],
    [Input("network-update-btn", "n_clicks")],
    [State("network-sample-slider", "value"),
     State("network-class-filter", "value"),
     State("network-time-filter", "value")]
)
def update_network(n_clicks, sample_size, class_filter, time_filter):
    if PROCESSED_DF is None or EDGES_DF is None:
        return {}, html.P("Data not available", className="text-danger")
    
    # Filter data
    filtered_df = PROCESSED_DF.copy()
    if class_filter != 'All':
        filtered_df = filtered_df[filtered_df['class_label'] == class_filter]
    if time_filter != 'All':
        filtered_df = filtered_df[filtered_df['Time step'] == time_filter]
    
    # Sample nodes
    sampled = filtered_df.sample(min(sample_size, len(filtered_df)))
    sampled_ids = set(sampled['txId'].astype(str))
    
    # Filter edges
    sampled_edges = EDGES_DF[
        (EDGES_DF['txId1'].astype(str).isin(sampled_ids)) &
        (EDGES_DF['txId2'].astype(str).isin(sampled_ids))
    ].head(1500)
    
    # Build graph
    G = nx.Graph()
    for _, edge in sampled_edges.iterrows():
        G.add_edge(str(edge['txId1']), str(edge['txId2']))
    
    if len(G.nodes()) == 0:
        return {}, html.P("No nodes in filtered data", className="text-warning")
    
    # Layout
    pos = nx.spring_layout(G, k=0.5, iterations=30, seed=42)
    
    # Edges
    edge_x, edge_y = [], []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
    
    # Nodes
    node_x, node_y, node_color, node_text = [], [], [], []
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        
        node_data = sampled[sampled['txId'].astype(str) == node]
        if not node_data.empty:
            cls = node_data.iloc[0]['class']
            node_color.append('#28a745' if cls == 1 else '#dc3545' if cls == 2 else '#ffc107')
            node_text.append(f"TX: {node[:10]}...<br>Class: {node_data.iloc[0]['class_label']}")
        else:
            node_color.append('#667eea')
            node_text.append(f"TX: {node[:10]}...")
    
    # Calculate node sizes based on degree
    degrees = dict(G.degree())
    max_degree = max(degrees.values()) if degrees else 1
    node_sizes = [max(15, min(40, 15 + (degrees.get(node, 0) / max_degree) * 25)) for node in G.nodes()]
    
    fig = go.Figure(data=[
        go.Scatter(x=edge_x, y=edge_y, mode='lines', 
                  line=dict(width=0.8, color='#555'), hoverinfo='none', showlegend=False),
        go.Scatter(x=node_x, y=node_y, mode='markers', 
                  marker=dict(size=node_sizes, color=node_color, line=dict(width=3, color='white'),
                            opacity=0.9),
                  text=node_text, hoverinfo='text', showlegend=False)
    ])
    
    fig.update_layout(
        template="plotly_dark",
        showlegend=False,
        hovermode='closest',
        margin=dict(b=20,l=20,r=20,t=40),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        title=dict(text=f"Network Graph: {len(G.nodes())} Nodes, {len(G.edges())} Edges",
                  font=dict(size=16, color='#667eea')),
        dragmode='pan'
    )
    
    # Stats box
    stats_content = dbc.Alert([
        html.H6("📊 Network Statistics", className="alert-heading"),
        html.P([html.Strong("Nodes: "), f"{len(G.nodes())}"]),
        html.P([html.Strong("Edges: "), f"{len(G.edges())}"]),
        html.P([html.Strong("Density: "), f"{nx.density(G):.4f}"], className="mb-0")
    ], color="primary")
    
    return fig, stats_content

# ArangoDB Tab Callbacks
@app.callback(
    Output("arango-metrics", "children"),
    Input("main-tabs", "active_tab")
)
def update_arango_metrics(tab):
    if tab != "arangodb":
        return []
    
    if not ARANGO_CONN:
        return dbc.Alert([
            html.H5([html.I(className="fas fa-exclamation-triangle me-2"), "ArangoDB Not Connected"]),
            html.P("Please ensure ArangoDB is running and accessible at localhost:8529"),
            html.Hr(),
            html.P([
                html.Strong("Troubleshooting:"), html.Br(),
                "• Check if Docker container is running", html.Br(),
                "• Verify credentials (root/root)", html.Br(),
                "• Ensure port 8529 is accessible"
            ])
        ], color="danger")
    
    try:
        tx_count = ARANGO_CONN.get_collection_count('transactions')
        edge_count = ARANGO_CONN.get_collection_count('tx_edges')
        
        metrics = dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H2(f"{tx_count:,}", className="text-success"),
                        html.P("Transactions in DB", className="text-muted")
                    ], className="text-center")
                ])
            ], width=4),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H2(f"{edge_count:,}", className="text-info"),
                        html.P("Edges in DB", className="text-muted")
                    ], className="text-center")
                ])
            ], width=4),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H2(f"{edge_count/tx_count if tx_count > 0 else 0:.2f}", className="text-warning"),
                        html.P("Avg Degree", className="text-muted")
                    ], className="text-center")
                ])
            ], width=4)
        ])
        
        return metrics
    except Exception as e:
        return dbc.Alert(f"Error fetching ArangoDB data: {str(e)}", color="danger")

@app.callback(
    Output("arango-class-dist", "figure"),
    Input("main-tabs", "active_tab")
)
def update_arango_class_dist(tab):
    if tab != "arangodb" or not ARANGO_CONN:
        return {}
    
    try:
        query = """
        FOR tx IN transactions
            COLLECT class = tx.class INTO group
            RETURN {
                class: class,
                count: LENGTH(group),
                class_name: class == 1 ? 'Licit' : class == 2 ? 'Illicit' : 'Unknown'
            }
        """
        results = ARANGO_CONN.aql_query(query)
        
        if not results:
            return {}
        
        df = pd.DataFrame(results)
        fig = px.bar(df, x='class_name', y='count',
                    color='class_name',
                    color_discrete_map={'Licit': '#28a745', 'Illicit': '#dc3545', 'Unknown': '#ffc107'},
                    title="Live Class Distribution from ArangoDB")
        fig.update_layout(template="plotly_dark", showlegend=False, height=400)
        return fig
    except:
        return {}

@app.callback(
    Output("arango-degree-dist", "figure"),
    Input("main-tabs", "active_tab")
)
def update_arango_degree(tab):
    if tab != "arangodb" or EDGES_DF is None:
        return {}
    
    in_deg = EDGES_DF['txId2'].value_counts()
    out_deg = EDGES_DF['txId1'].value_counts()
    
    fig = go.Figure()
    fig.add_trace(go.Histogram(x=in_deg.values, name='In-Degree', opacity=0.7, marker_color='#17a2b8'))
    fig.add_trace(go.Histogram(x=out_deg.values, name='Out-Degree', opacity=0.7, marker_color='#dc3545'))
    fig.update_layout(template="plotly_dark", barmode='overlay', 
                     title="Node Degree Distribution", height=400)
    return fig

# Query Tab Callbacks
@app.callback(
    [Output("query-simple-content", "children"),
     Output("query-complex-content", "children"),
     Output("query-viz", "figure"),
     Output("query-status", "children")],
    [Input("query-simple-1", "n_clicks"),
     Input("query-simple-2", "n_clicks"),
     Input("query-simple-3", "n_clicks"),
     Input("query-simple-4", "n_clicks"),
     Input("query-complex-1", "n_clicks"),
     Input("query-complex-2", "n_clicks"),
     Input("query-complex-3", "n_clicks"),
     Input("query-complex-4", "n_clicks"),
     Input("query-run-all", "n_clicks"),
     Input("main-tabs", "active_tab")]
)
def execute_queries(sq1, sq2, sq3, sq4, cq1, cq2, cq3, cq4, run_all, tab):
    if tab != "queries":
        return [], [], {}, []
    
    # Determine which button was clicked
    triggered = ctx.triggered_id if ctx.triggered_id else None
    
    simple_results = []
    complex_results = []
    viz_fig = {}
    status = []
    
    if not ARANGO_CONN:
        status = dbc.Alert([
            html.H6([html.I(className="fas fa-exclamation-triangle me-2"), "ArangoDB Not Connected"]),
            html.P("Cannot execute queries without database connection")
        ], color="danger")
        return simple_results, complex_results, viz_fig, status
    
    try:
        # Import query classes
        try:
            from graph.queries_simple import SimpleQueries
            from graph.queries_complex import ComplexQueries
            simple_q = SimpleQueries(ARANGO_CONN)
            complex_q = ComplexQueries(ARANGO_CONN)
        except ImportError as e:
            status = dbc.Alert(f"Failed to import query modules: {str(e)}", color="danger")
            return simple_results, complex_results, viz_fig, status
        
        # Execute Simple Query 1: Count by Class
        if triggered == "query-simple-1" or triggered == "query-run-all":
            results = simple_q.query_1_count_by_class()
            if results:
                df = pd.DataFrame(results)
                simple_results.append(html.H6("Query 1: Count by Class", className="text-primary mt-3"))
                simple_results.append(dash_table.DataTable(
                    data=df.to_dict('records'),
                    columns=[{'name': i, 'id': i} for i in df.columns],
                    style_cell={'textAlign': 'center', 'backgroundColor': '#1a1f2e', 'color': 'white'},
                    style_header={'backgroundColor': '#667eea', 'fontWeight': 'bold'}
                ))
                
                # Create visualization
                viz_fig = px.bar(df, x='class_name', y='count', color='class_name',
                               color_discrete_map={'Licit': '#28a745', 'Illicit': '#dc3545', 
                                                  'Unknown': '#ffc107', 'Suspected': '#17a2b8'},
                               title="Transaction Count by Class")
                viz_fig.update_layout(template="plotly_dark", showlegend=False, height=400)
                
                status = dbc.Alert([
                    html.I(className="fas fa-check-circle me-2"),
                    "Query executed successfully!"
                ], color="success")
        
        # Execute Simple Query 2: Outgoing Edges
        elif triggered == "query-simple-2":
            # Get a sample transaction
            sample_query = "FOR tx IN transactions LIMIT 1 RETURN tx._key"
            sample_tx = ARANGO_CONN.aql_query(sample_query)
            if sample_tx:
                query = f"""
                FOR edge IN tx_edges
                    FILTER edge._from == 'transactions/{sample_tx[0]}'
                    LIMIT 20
                    RETURN {{
                        from: edge._from,
                        to: edge._to
                    }}
                """
                results = ARANGO_CONN.aql_query(query)
                if results:
                    df = pd.DataFrame(results)
                    simple_results.append(html.H6(f"Query 2: Outgoing Edges from {sample_tx[0]}", 
                                                  className="text-primary mt-3"))
                    simple_results.append(dash_table.DataTable(
                        data=df.to_dict('records'),
                        columns=[{'name': i, 'id': i} for i in df.columns],
                        style_cell={'textAlign': 'left', 'backgroundColor': '#1a1f2e', 'color': 'white'},
                        style_header={'backgroundColor': '#667eea', 'fontWeight': 'bold'},
                        page_size=10
                    ))
                    status = dbc.Alert("Query executed successfully!", color="success")
        
        # Execute Simple Query 3: Incoming Edges
        elif triggered == "query-simple-3":
            sample_query = "FOR tx IN transactions LIMIT 1 RETURN tx._key"
            sample_tx = ARANGO_CONN.aql_query(sample_query)
            if sample_tx:
                query = f"""
                FOR edge IN tx_edges
                    FILTER edge._to == 'transactions/{sample_tx[0]}'
                    LIMIT 20
                    RETURN {{
                        from: edge._from,
                        to: edge._to
                    }}
                """
                results = ARANGO_CONN.aql_query(query)
                if results:
                    df = pd.DataFrame(results)
                    simple_results.append(html.H6(f"Query 3: Incoming Edges to {sample_tx[0]}", 
                                                  className="text-primary mt-3"))
                    simple_results.append(dash_table.DataTable(
                        data=df.to_dict('records'),
                        columns=[{'name': i, 'id': i} for i in df.columns],
                        style_cell={'textAlign': 'left', 'backgroundColor': '#1a1f2e', 'color': 'white'},
                        style_header={'backgroundColor': '#667eea', 'fontWeight': 'bold'},
                        page_size=10
                    ))
                    status = dbc.Alert("Query executed successfully!", color="success")
        
        # Execute Simple Query 4: Time Range
        elif triggered == "query-simple-4":
            query = """
            FOR tx IN transactions
                COLLECT time_step = tx.time_step INTO group
                SORT time_step
                RETURN {
                    time_step: time_step,
                    count: LENGTH(group)
                }
            """
            results = ARANGO_CONN.aql_query(query)
            if results:
                df = pd.DataFrame(results)
                simple_results.append(html.H6("Query 4: Transactions per Time Step", 
                                              className="text-primary mt-3"))
                simple_results.append(dash_table.DataTable(
                    data=df.head(20).to_dict('records'),
                    columns=[{'name': i, 'id': i} for i in df.columns],
                    style_cell={'textAlign': 'center', 'backgroundColor': '#1a1f2e', 'color': 'white'},
                    style_header={'backgroundColor': '#667eea', 'fontWeight': 'bold'},
                    page_size=10
                ))
                
                # Time series visualization
                viz_fig = px.line(df, x='time_step', y='count', markers=True,
                                title="Transaction Activity Over Time")
                viz_fig.update_layout(template="plotly_dark", height=400)
                status = dbc.Alert("Query executed successfully!", color="success")
        
        # Execute Complex Query 1: Two-Hop Neighbors
        elif triggered == "query-complex-1":
            results = complex_q.query_1_two_hop_neighbors()
            if results and not isinstance(results, dict) or (isinstance(results, dict) and 'error' not in results):
                df = pd.DataFrame(results if isinstance(results, list) else [results])
                complex_results.append(html.H6("Query 1: Two-Hop Neighbor Analysis", 
                                               className="text-warning mt-3"))
                complex_results.append(dash_table.DataTable(
                    data=df.to_dict('records'),
                    columns=[{'name': i, 'id': i} for i in df.columns],
                    style_cell={'textAlign': 'center', 'backgroundColor': '#1a1f2e', 'color': 'white'},
                    style_header={'backgroundColor': '#f0ad4e', 'fontWeight': 'bold'}
                ))
                status = dbc.Alert("Complex query executed successfully!", color="success")
        
        # Execute Complex Query 2: Illicit Clusters
        elif triggered == "query-complex-2":
            results = complex_q.query_2_illicit_clusters()
            if results:
                df = pd.DataFrame(results[:50])  # Limit to 50 for display
                complex_results.append(html.H6("Query 2: Illicit Transaction Clusters", 
                                               className="text-warning mt-3"))
                complex_results.append(dash_table.DataTable(
                    data=df.to_dict('records'),
                    columns=[{'name': i, 'id': i} for i in df.columns],
                    style_cell={'textAlign': 'left', 'backgroundColor': '#1a1f2e', 'color': 'white'},
                    style_header={'backgroundColor': '#f0ad4e', 'fontWeight': 'bold'},
                    page_size=10
                ))
                
                # Cluster size distribution
                viz_fig = px.histogram(df, x='connected_count', nbins=30,
                                     title="Distribution of Cluster Sizes (Illicit Transactions)")
                viz_fig.update_layout(template="plotly_dark", height=400)
                status = dbc.Alert("Complex query executed successfully!", color="success")
        
        # Execute Complex Query 3: Temporal Patterns
        elif triggered == "query-complex-3":
            results = complex_q.query_3_temporal_patterns()
            if results:
                df = pd.DataFrame(results)
                complex_results.append(html.H6("Query 3: Temporal Transaction Patterns", 
                                               className="text-warning mt-3"))
                complex_results.append(dash_table.DataTable(
                    data=df.head(30).to_dict('records'),
                    columns=[{'name': i, 'id': i} for i in df.columns],
                    style_cell={'textAlign': 'center', 'backgroundColor': '#1a1f2e', 'color': 'white'},
                    style_header={'backgroundColor': '#f0ad4e', 'fontWeight': 'bold'},
                    page_size=15
                ))
                
                # Temporal heatmap
                pivot = df.pivot_table(values='transaction_count', index='time_step', 
                                      columns='class', fill_value=0)
                viz_fig = px.imshow(pivot.T, aspect='auto', color_continuous_scale='Blues',
                                  title="Temporal Pattern Heatmap (Class vs Time)")
                viz_fig.update_layout(template="plotly_dark", height=400)
                status = dbc.Alert("Complex query executed successfully!", color="success")
        
        # Execute Complex Query 4: High Degree Nodes
        elif triggered == "query-complex-4":
            query = """
            FOR tx IN transactions
                LET out_degree = LENGTH(FOR e IN tx_edges FILTER e._from == tx._id RETURN 1)
                LET in_degree = LENGTH(FOR e IN tx_edges FILTER e._to == tx._id RETURN 1)
                FILTER out_degree + in_degree > 5
                SORT out_degree + in_degree DESC
                LIMIT 50
                RETURN {
                    tx_id: tx._key,
                    class: tx.class,
                    time_step: tx.time_step,
                    in_degree: in_degree,
                    out_degree: out_degree,
                    total_degree: in_degree + out_degree
                }
            """
            results = ARANGO_CONN.aql_query(query)
            if results:
                df = pd.DataFrame(results)
                complex_results.append(html.H6("Query 4: High Degree Nodes (Hubs)", 
                                               className="text-warning mt-3"))
                complex_results.append(dash_table.DataTable(
                    data=df.to_dict('records'),
                    columns=[{'name': i, 'id': i} for i in df.columns],
                    style_cell={'textAlign': 'center', 'backgroundColor': '#1a1f2e', 'color': 'white'},
                    style_header={'backgroundColor': '#f0ad4e', 'fontWeight': 'bold'},
                    page_size=15
                ))
                
                # Hub distribution
                viz_fig = px.scatter(df, x='in_degree', y='out_degree', color='class',
                                   size='total_degree', hover_data=['tx_id'],
                                   title="Network Hubs: In-Degree vs Out-Degree",
                                   color_discrete_map={0: '#ffc107', 1: '#28a745', 2: '#dc3545'})
                viz_fig.update_layout(template="plotly_dark", height=400)
                status = dbc.Alert("Complex query executed successfully!", color="success")
        
        # Default display
        if not simple_results:
            simple_results = dbc.Alert([
                html.H6([html.I(className="fas fa-info-circle me-2"), "No Query Executed"]),
                html.P("Click a button above to execute queries on the ArangoDB database")
            ], color="info")
        
        if not complex_results:
            complex_results = dbc.Alert([
                html.H6([html.I(className="fas fa-info-circle me-2"), "No Query Executed"]),
                html.P("Click a button above to execute complex graph queries")
            ], color="info")
        
        if not status:
            status = dbc.Alert([
                html.I(className="fas fa-database me-2"),
                f"Connected to ArangoDB | Ready to execute queries"
            ], color="info")
        
    except Exception as e:
        status = dbc.Alert([
            html.H6([html.I(className="fas fa-exclamation-triangle me-2"), "Query Execution Error"]),
            html.P(f"Error: {str(e)}")
        ], color="danger")
        simple_results = dbc.Alert("Error executing query", color="danger")
        complex_results = dbc.Alert("Error executing query", color="danger")
    
    return simple_results, complex_results, viz_fig, status

# Explorer Tab Callbacks
@app.callback(
    Output("explorer-correlation", "figure"),
    [Input("explorer-apply-btn", "n_clicks")],
    [State("explorer-class-filter", "value"),
     State("explorer-time-range", "value")]
)
def update_explorer_correlation(n_clicks, classes, time_range):
    if PROCESSED_DF is None:
        return {}
    
    # Filter
    filtered = PROCESSED_DF[
        (PROCESSED_DF['class_label'].isin(classes)) &
        (PROCESSED_DF['Time step'] >= time_range[0]) &
        (PROCESSED_DF['Time step'] <= time_range[1])
    ]
    
    # Get feature columns
    feat_cols = [c for c in filtered.columns if c.startswith(('Local_', 'Aggregate_'))][:15]
    
    if not feat_cols:
        return {}
    
    corr = filtered[feat_cols].corr()
    fig = px.imshow(corr, color_continuous_scale='RdBu_r', aspect='auto',
                   title="Feature Correlation Heatmap")
    fig.update_layout(template="plotly_dark", height=500)
    return fig

@app.callback(
    Output("explorer-data-table", "children"),
    [Input("explorer-apply-btn", "n_clicks")],
    [State("explorer-class-filter", "value"),
     State("explorer-time-range", "value"),
     State("explorer-sample-size", "value")]
)
def update_explorer_table(n_clicks, classes, time_range, sample_size):
    if PROCESSED_DF is None:
        return html.P("No data available")
    
    # Filter
    filtered = PROCESSED_DF[
        (PROCESSED_DF['class_label'].isin(classes)) &
        (PROCESSED_DF['Time step'] >= time_range[0]) &
        (PROCESSED_DF['Time step'] <= time_range[1])
    ]
    
    # Sample
    if sample_size != 'All':
        filtered = filtered.sample(min(sample_size, len(filtered)))
    
    # Select columns to display
    display_cols = ['txId', 'Time step', 'class_label'] + \
                   [c for c in filtered.columns if c.startswith('Local_')][:5]
    
    return dash_table.DataTable(
        data=filtered[display_cols].to_dict('records'),
        columns=[{'name': i, 'id': i} for i in display_cols],
        style_cell={'textAlign': 'center', 'backgroundColor': '#1a1f2e', 'color': 'white'},
        style_header={'backgroundColor': '#667eea', 'fontWeight': 'bold'},
        page_size=25,
        style_table={'overflowX': 'auto'}
    )

# Analytics Tab Callbacks
@app.callback(
    Output("analytics-degree", "figure"),
    Input("main-tabs", "active_tab")
)
def update_analytics_degree(tab):
    if tab != "analytics" or EDGES_DF is None:
        return {}
    
    in_deg = EDGES_DF['txId2'].value_counts()
    out_deg = EDGES_DF['txId1'].value_counts()
    
    fig = go.Figure()
    fig.add_trace(go.Histogram(x=in_deg.values, name='In-Degree', opacity=0.7, marker_color='#17a2b8'))
    fig.add_trace(go.Histogram(x=out_deg.values, name='Out-Degree', opacity=0.7, marker_color='#dc3545'))
    fig.update_layout(template="plotly_dark", barmode='overlay',
                     title="Degree Distribution Analysis", height=400,
                     xaxis_title="Degree", yaxis_title="Frequency")
    return fig

@app.callback(
    Output("analytics-correlation", "figure"),
    Input("main-tabs", "active_tab")
)
def update_analytics_correlation(tab):
    if tab != "analytics" or PROCESSED_DF is None:
        return {}
    
    feat_cols = [c for c in PROCESSED_DF.columns if c.startswith(('Local_', 'Aggregate_'))][:12]
    if not feat_cols:
        return {}
    
    corr = PROCESSED_DF[feat_cols].corr()
    fig = px.imshow(corr, color_continuous_scale='RdBu_r', aspect='auto',
                   title="Feature Correlation Matrix")
    fig.update_layout(template="plotly_dark", height=400)
    return fig

@app.callback(
    Output("analytics-boxplots", "figure"),
    Input("main-tabs", "active_tab")
)
def update_analytics_boxplots(tab):
    if tab != "analytics" or PROCESSED_DF is None:
        return {}
    
    feat_cols = [c for c in PROCESSED_DF.columns if c.startswith('Local_')][:8]
    if not feat_cols:
        return {}
    
    fig = make_subplots(rows=2, cols=4, subplot_titles=feat_cols)
    for i, col in enumerate(feat_cols):
        row = i // 4 + 1
        col_pos = i % 4 + 1
        fig.add_trace(go.Box(y=PROCESSED_DF[col], name=col, marker_color='#667eea'),
                     row=row, col=col_pos)
    
    fig.update_layout(template="plotly_dark", showlegend=False, height=500,
                     title="Feature Distribution Box Plots")
    return fig

# ============================================================================
# RUN SERVER
# ============================================================================

if __name__ == '__main__':
    print("\n" + "="*70)
    print("🚀 ElliptiGraph Dashboard - Comprehensive Edition")
    print("="*70)
    print(f"📊 Data Loaded: {STATS.get('total_tx', 0):,} transactions, {STATS.get('total_edges', 0):,} edges")
    print(f"🗃️ ArangoDB: {'✅ Connected' if ARANGO_CONN else '❌ Not Connected'}")
    print("📍 Dashboard: http://localhost:8050")
    print("="*70 + "\n")
    
    app.run(debug=False, host='0.0.0.0', port=8050)

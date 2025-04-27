import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import glob

# Initialize the Dash app
app = dash.Dash(__name__)

# Read all CSV files
csv_files = glob.glob("stream_input/credit_chunk_*.csv")
df_list = [pd.read_csv(file) for file in csv_files]
df = pd.concat(df_list, ignore_index=True)

# Calculate some basic statistics
total_customers = len(df)
avg_credit_limit = df['LIMIT_BAL'].mean()
default_rate = df['default payment next month'].mean() * 100

# App layout
app.layout = html.Div([
    html.H1("Credit Data Dashboard", style={'textAlign': 'center', 'color': '#1f77b4'}),
    
    # Summary Cards
    html.Div([
        html.Div([
            html.H3("Total Customers"),
            html.H2(f"{total_customers:,}")
        ], className="card"),
        html.Div([
            html.H3("Avg Credit Limit"),
            html.H2(f"${avg_credit_limit:,.2f}")
        ], className="card"),
        html.Div([
            html.H3("Default Rate"),
            html.H2(f"{default_rate:.1f}%")
        ], className="card")
    ], style={'display': 'flex', 'justifyContent': 'space-around', 'margin': '20px'}),
    
    # Dropdown for selecting visualization
    dcc.Dropdown(
        id='visualization-type',
        options=[
            {'label': 'Credit Limit Distribution', 'value': 'credit_dist'},
            {'label': 'Age vs Credit Limit', 'value': 'age_credit'},
            {'label': 'Default by Education', 'value': 'edu_default'},
            {'label': 'Payment Status Over Time', 'value': 'pay_status'}
        ],
        value='credit_dist',
        style={'width': '50%', 'margin': '0 auto 20px'}
    ),
    
    # Graph container
    dcc.Graph(id='main-graph'),
    
    # Additional styling
], style={'fontFamily': 'Arial, sans-serif', 'maxWidth': '1200px', 'margin': '0 auto'})

# Callback to update graph based on dropdown selection
@app.callback(
    Output('main-graph', 'figure'),
    Input('visualization-type', 'value')
)
def update_graph(viz_type):
    if viz_type == 'credit_dist':
        fig = px.histogram(df, x='LIMIT_BAL', nbins=50, title='Credit Limit Distribution',
                         color_discrete_sequence=['#1f77b4'])
        fig.update_layout(xaxis_title="Credit Limit", yaxis_title="Count")
        
    elif viz_type == 'age_credit':
        fig = px.scatter(df, x='AGE', y='LIMIT_BAL', color='default payment next month',
                        title='Age vs Credit Limit (Colored by Default Status)',
                        color_continuous_scale='RdBu')
        fig.update_layout(xaxis_title="Age", yaxis_title="Credit Limit")
        
    elif viz_type == 'edu_default':
        edu_default = df.groupby('EDUCATION')['default payment next month'].mean().reset_index()
        fig = px.bar(edu_default, x='EDUCATION', y='default payment next month',
                    title='Default Rate by Education Level',
                    color_discrete_sequence=['#1f77b4'])
        fig.update_layout(xaxis_title="Education Level", yaxis_title="Default Rate")
        
    else:  # pay_status
        pay_cols = ['PAY_0', 'PAY_2', 'PAY_3', 'PAY_4', 'PAY_5', 'PAY_6']
        pay_data = df[pay_cols].mean().reset_index()
        fig = px.line(pay_data, x='index', y=0, title='Average Payment Status Over Time',
                     color_discrete_sequence=['#1f77b4'])
        fig.update_layout(xaxis_title="Payment Period", yaxis_title="Average Status")
    
    # Update layout for better appearance
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(size=12),
        showlegend=True
    )
    return fig

# Add some CSS
app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

# Run the app
if __name__ == '__main__':
    app.run(debug=True)  # Changed from run_server() to run()

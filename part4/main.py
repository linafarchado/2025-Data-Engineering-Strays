import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
from pyspark.sql import SparkSession

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css'])

# Create a Spark session
spark = SparkSession.builder.appName("HDFSCSVReader").getOrCreate()

# Define the base path
base_path = "hdfs://localhost:9000/user/lina/"

# Function to read CSV files from HDFS using Spark
def read_hdfs_csv(file_path):
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df.toPandas()

# Read all CSV files
animal_type_df = read_hdfs_csv(base_path + "animaltype.csv")
avg_injury_df = read_hdfs_csv(base_path + "avgInjury.csv")
geo_analysis_df = read_hdfs_csv(base_path + "geoAnalysis.csv")
top_animals_df = read_hdfs_csv(base_path + "topAnimals.csv")
animal_density_df = read_hdfs_csv(base_path + "animalDensity.csv")
injury_severity_df = read_hdfs_csv(base_path + "injurySeverity.csv")

# Layout of the app
app.layout = html.Div([
    html.H1("Drone Data Analysis Dashboard", style={'textAlign': 'center', 'color': '#2C3E50', 'fontFamily': 'Arial'}),

    dcc.Tabs([
        dcc.Tab(label="Geographical Analysis", children=[
            html.Div([
                html.Div([
                    dcc.Graph(id='animal-density-map')
                ], className="twelve columns"),
            ], className="row"),
            html.Div([
                dcc.Graph(id='quadrant-analysis-sunburst')
            ], className="row"),
        ]),

        dcc.Tab(label="Distributions", children=[
            html.Div([
                html.Div([
                    dcc.Graph(id='animal-type-sunburst')
                ], className="six columns"),
                html.Div([
                    dcc.Graph(id='injury-severity-pie')
                ], className="six columns"),
            ], className="row"),
        ]),

        dcc.Tab(label="Injury Analysis", children=[
            html.Div([
                html.Div([
                    dcc.Graph(id='injury-index-bar')
                ], className="six columns"),
                html.Div([
                    dcc.Graph(id='top-animals-injury-bar')
                ], className="six columns"),
            ], className="row"),
        ]),

    ])
])

# Callbacks for each graph
@app.callback(Output('animal-density-map', 'figure'), Input('animal-density-map', 'id'))
def update_animal_density_map(id):
    fig = px.scatter_mapbox(animal_density_df, lat='latitude', lon='longitude', hover_name='animalType',
                            hover_data=['count'], color='animalType', zoom=3, height=600,
                            mapbox_style="carto-positron")
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, title='Animal Density Map')
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    return fig

@app.callback(Output('animal-type-sunburst', 'figure'), Input('animal-type-sunburst', 'id'))
def update_animal_type_sunburst(id):
    fig = px.sunburst(animal_type_df, path=['animalType'], values='count',
                      title='Animal Type Distribution',
                      color='count', color_continuous_scale='Viridis')
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    fig.update_traces(textinfo="label+percent entry")
    return fig


@app.callback(Output('injury-index-bar', 'figure'), Input('injury-index-bar', 'id'))
def update_injury_index_bar(id):
    fig = px.bar(avg_injury_df, x='animalType', y='avgInjuryIndex',
                 title='Average Injury Index by Animal Type',
                 color='animalType', color_discrete_sequence=px.colors.qualitative.Pastel)

    fig.update_layout(
        title_font=dict(size=20),
        title_x=0.5,
        yaxis=dict(visible=False),
        showlegend=False
    )

    # Add text annotations for average values on top of each bar
    for i, row in avg_injury_df.iterrows():
        fig.add_annotation(
            x=row['animalType'],
            y=row['avgInjuryIndex'],
            text=f"{row['avgInjuryIndex']:.2f}",
            showarrow=False,
            yshift=10,  # Adjust this value to position the text above the bar
            font=dict(size=12)
        )

    return fig

@app.callback(Output('injury-severity-pie', 'figure'), Input('injury-severity-pie', 'id'))
def update_injury_severity_pie(id):
    fig = px.pie(injury_severity_df, values='count', names='severityCategory',
                 title='Injury Severity Distribution',
                 color_discrete_sequence=px.colors.sequential.RdBu)
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    fig.update_traces(textinfo="label+percent")
    return fig

@app.callback(Output('quadrant-analysis-sunburst', 'figure'), Input('quadrant-analysis-sunburst', 'id'))
def update_quadrant_analysis_sunburst(id):
    fig = px.sunburst(geo_analysis_df, path=['quadrant'], values='count',
                      title='Incidents by Quadrant',
                      color='count', color_continuous_scale='Viridis')
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    fig.update_traces(textinfo="label+percent entry")
    return fig

@app.callback(Output('top-animals-injury-bar', 'figure'), Input('top-animals-injury-bar', 'id'))
def update_top_animals_injury_bar(id):
    # Sort the dataframe in descending order of totalInjuryIndex
    sorted_df = top_animals_df.sort_values('totalInjuryIndex', ascending=True)

    fig = px.bar(sorted_df, x='totalInjuryIndex', y='animalType',
                 title='Top Animals by Total Injury Index',
                 color='animalType', orientation='h',
                 color_discrete_sequence=px.colors.qualitative.Pastel)

    fig.update_layout(
        title_font=dict(size=20),
        title_x=0.5,
        xaxis=dict(visible=False, showticklabels=False),
        yaxis=dict(title=''),
        showlegend=True,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        height=400,
        margin=dict(l=100, r=20, t=50, b=20)
    )

    fig.update_traces(textposition='inside', insidetextanchor='middle')

    return fig

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)

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
high_severity_df = read_hdfs_csv(base_path + "highSeverity.csv")
time_analysis_df = read_hdfs_csv(base_path + "timeAnalysis.csv")
top_animals_df = read_hdfs_csv(base_path + "topAnimals.csv")
median_injury_df = read_hdfs_csv(base_path + "medianInjury.csv")
incidents_by_day_df = read_hdfs_csv(base_path + "incidentsByDayOfWeek.csv")
incidents_by_geo_df = read_hdfs_csv(base_path + "incidentsByGeoArea.csv")
correlations_df = read_hdfs_csv(base_path + "correlations.csv")
seasonal_analysis_df = read_hdfs_csv(base_path + "seasonalAnalysis.csv")
animal_density_df = read_hdfs_csv(base_path + "animalDensity.csv")
injury_severity_df = read_hdfs_csv(base_path + "injurySeverity.csv")

# Layout of the app
app.layout = html.Div([
    html.H1("Drone Data Analysis Dashboard", style={'textAlign': 'center', 'color': '#2C3E50', 'fontFamily': 'Arial'}),

    dcc.Tabs([
        dcc.Tab(label="Animal Distribution", children=[
            html.Div([
                html.Div([
                    dcc.Graph(id='animal-density-map')
                ], className="twelve columns"),
            ], className="row"),
            html.Div([
                html.Div([
                    dcc.Graph(id='animal-type-sunburst')
                ], className="six columns"),
                html.Div([
                    dcc.Graph(id='animal-type-treemap')
                ], className="six columns"),
            ], className="row"),
        ]),

        dcc.Tab(label="Injury Analysis", children=[
            html.Div([
                html.Div([
                    dcc.Graph(id='injury-index-violin')
                ], className="six columns"),
                html.Div([
                    dcc.Graph(id='injury-severity-pie')
                ], className="six columns"),
            ], className="row"),
            html.Div([
                dcc.Graph(id='injury-distribution-ridgeline')
            ], className="row"),
        ]),

        dcc.Tab(label="Temporal Analysis", children=[
            html.Div([
                html.Div([
                    dcc.Graph(id='incidents-by-hour-area')
                ], className="six columns"),
                html.Div([
                    dcc.Graph(id='incidents-by-day-polar')
                ], className="six columns"),
            ], className="row"),
            html.Div([
                dcc.Graph(id='seasonal-analysis-radar')
            ], className="row"),
        ]),

        dcc.Tab(label="Geographical Analysis", children=[
            html.Div([
                dcc.Graph(id='geo-analysis-bubble')
            ], className="row"),
            html.Div([
                dcc.Graph(id='quadrant-analysis-sunburst')
            ], className="row"),
        ]),

        dcc.Tab(label="Severity Analysis", children=[
            html.Div([
                html.Div([
                    dcc.Graph(id='high-severity-parallel')
                ], className="six columns"),
                html.Div([
                    dcc.Graph(id='top-animals-injury-funnel')
                ], className="six columns"),
            ], className="row"),
        ]),
        dcc.Tab(label="Location and Time Analysis", children=[
            html.Div([
                html.Div([
                    dcc.Graph(id='geo-analysis-pie')
                ], className="six columns"),
                html.Div([
                    dcc.Graph(id='seasonal-bar-chart')
                ], className="six columns"),
            ], className="row"),
            html.Div([
                html.Div([
                    dcc.Graph(id='hourly-incidents-line')
                ], className="six columns"),
                html.Div([
                    dcc.Graph(id='animal-density-scatter')
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

@app.callback(Output('animal-type-treemap', 'figure'), Input('animal-type-treemap', 'id'))
def update_animal_type_treemap(id):
    fig = px.treemap(animal_type_df, path=['animalType'], values='count',
                     title='Animal Type Hierarchy',
                     color='count', color_continuous_scale='Viridis')
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    fig.update_traces(textinfo="label+percent entry")
    return fig

@app.callback(Output('injury-index-violin', 'figure'), Input('injury-index-violin', 'id'))
def update_injury_index_violin(id):
    fig = px.violin(avg_injury_df, x='animalType', y='avgInjuryIndex', box=True, points="all",
                    title='Injury Index Distribution by Animal Type',
                    color='animalType', color_discrete_sequence=px.colors.qualitative.Pastel)
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    fig.update_traces(meanline_visible=True)
    return fig

@app.callback(Output('injury-severity-pie', 'figure'), Input('injury-severity-pie', 'id'))
def update_injury_severity_pie(id):
    fig = px.pie(injury_severity_df, values='count', names='severityCategory',
                 title='Injury Severity Distribution',
                 color_discrete_sequence=px.colors.sequential.RdBu)
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    fig.update_traces(textinfo="label+percent")
    return fig

@app.callback(Output('injury-distribution-ridgeline', 'figure'), Input('injury-distribution-ridgeline', 'id'))
def update_injury_distribution_ridgeline(id):
    fig = go.Figure()
    for animal in avg_injury_df['animalType'].unique():
        animal_data = avg_injury_df[avg_injury_df['animalType'] == animal]
        fig.add_trace(go.Violin(x=animal_data['avgInjuryIndex'], name=animal, side='positive', orientation='h'))
    fig.update_layout(title='Distribution of Average Injury Index by Animal Type',
                      title_font=dict(size=20), title_x=0.5,
                      xaxis_title='Average Injury Index',
                      yaxis_title='Animal Type')
    return fig

@app.callback(Output('incidents-by-hour-area', 'figure'), Input('incidents-by-hour-area', 'id'))
def update_incidents_by_hour_area(id):
    fig = px.area(time_analysis_df, x='hour', y='count',
                  title='Incidents by Hour of Day',
                  color_discrete_sequence=['#FFA07A'])
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    fig.update_traces(line_shape='spline')
    return fig

@app.callback(Output('incidents-by-day-polar', 'figure'), Input('incidents-by-day-polar', 'id'))
def update_incidents_by_day_polar(id):
    fig = px.bar_polar(incidents_by_day_df, r='count', theta='dayOfWeek',
                       title='Incidents by Day of the Week',
                       color='count', color_continuous_scale='Viridis')
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    return fig

@app.callback(Output('seasonal-analysis-radar', 'figure'), Input('seasonal-analysis-radar', 'id'))
def update_seasonal_analysis_radar(id):
    fig = px.line_polar(seasonal_analysis_df, r='count', theta='season', line_close=True,
                        title='Seasonal Analysis of Incidents',
                        color_discrete_sequence=px.colors.sequential.Plasma)
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    return fig

@app.callback(Output('geo-analysis-bubble', 'figure'), Input('geo-analysis-bubble', 'id'))
def update_geo_analysis_bubble(id):
    fig = px.scatter(incidents_by_geo_df, x='lonGroup', y='latGroup', size='count', color='count',
                     title='Geographical Distribution of Incidents',
                     color_continuous_scale='Viridis', size_max=50)
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    fig.update_traces(marker=dict(sizemode='diameter'))
    return fig

@app.callback(Output('quadrant-analysis-sunburst', 'figure'), Input('quadrant-analysis-sunburst', 'id'))
def update_quadrant_analysis_sunburst(id):
    fig = px.sunburst(geo_analysis_df, path=['quadrant'], values='count',
                      title='Incidents by Quadrant',
                      color='count', color_continuous_scale='Viridis')
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    fig.update_traces(textinfo="label+percent entry")
    return fig

@app.callback(Output('high-severity-parallel', 'figure'), Input('high-severity-parallel', 'id'))
def update_high_severity_parallel(id):
    fig = px.parallel_coordinates(high_severity_df, color="injuryIndex",
                                  dimensions=['id', 'injuryIndex'],
                                  title='High Severity Incidents Parallel Coordinates',
                                  color_continuous_scale='Viridis')
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    return fig

@app.callback(Output('top-animals-injury-funnel', 'figure'), Input('top-animals-injury-funnel', 'id'))
def update_top_animals_injury_funnel(id):
    fig = px.funnel(top_animals_df, x='totalInjuryIndex', y='animalType',
                    title='Top Animals by Total Injury Index',
                    color='animalType', color_discrete_sequence=px.colors.qualitative.Safe)
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    return fig

@app.callback(Output('geo-analysis-pie', 'figure'), Input('geo-analysis-pie', 'id'))
def update_geo_analysis_pie(id):
    fig = px.pie(geo_analysis_df, values='count', names='quadrant',
                 title='Incident Distribution by Quadrant',
                 color_discrete_sequence=px.colors.qualitative.Pastel)
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    fig.update_traces(textinfo="label+percent")
    return fig

@app.callback(Output('seasonal-bar-chart', 'figure'), Input('seasonal-bar-chart', 'id'))
def update_seasonal_bar_chart(id):
    fig = px.bar(seasonal_analysis_df, x='season', y='count',
                 title='Seasonal Analysis of Incidents',
                 color='avgInjuryIndex', text='count',
                 color_continuous_scale="Viridis")
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    fig.update_traces(marker=dict(line=dict(color='#333', width=2)))
    return fig

@app.callback(Output('hourly-incidents-line', 'figure'), Input('hourly-incidents-line', 'id'))
def update_hourly_incidents_line(id):
    fig = px.line(time_analysis_df, x='hour', y='count',
                  title='Incidents by Hour of Day',
                  labels={'count': 'Number of Incidents', 'hour': 'Hour of Day'},
                  line_shape="spline", render_mode="svg",
                  color_discrete_sequence=px.colors.sequential.Viridis)
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    return fig

@app.callback(Output('animal-density-scatter', 'figure'), Input('animal-density-scatter', 'id'))
def update_animal_density_scatter(id):
    fig = px.scatter(animal_density_df, x='longitude', y='latitude',
                     size='count', color='animalType',
                     hover_data=['animalType', 'count'],
                     title='Animal Density Distribution',
                     color_discrete_sequence=px.colors.qualitative.Safe)
    fig.update_layout(title_font=dict(size=20), title_x=0.5)
    fig.update_traces(marker=dict(line=dict(color='#333', width=2)))
    return fig

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)

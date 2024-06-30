from py4j.java_gateway import JavaGateway
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
import numpy as np

# Connect to the Py4J gateway
gateway = JavaGateway()

# Create a Spark session
spark = SparkSession.builder.appName("HDFSCSVReader").getOrCreate()

# Define the base path
base_path = "hdfs://localhost:9000/user/lina/"

# Function to read CSV files from HDFS
def read_hdfs_csv(file_path):
    df = spark.read.csv(file_path, header=True, inferSchema=True).toPandas()
    print(f"Columns in {file_path}: {df.columns.tolist()}")
    return df

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

# Set a consistent style for all plots
plt.style.use('seaborn')

# Animal type distribution: Horizontal bar chart
plt.figure(figsize=(12, 8))
animal_type_df = animal_type_df.sort_values('count', ascending=True)
plt.barh(animal_type_df['animalType'], animal_type_df['count'])
plt.title('Animal Type Distribution', fontsize=16)
plt.xlabel('Count', fontsize=12)
plt.ylabel('Animal Type', fontsize=12)
plt.tight_layout()
plt.savefig('animal_type_distribution.png')
plt.close()

"""
#Correlation between Injury Index and Latitude: Scatter plot
plt.figure(figsize=(12, 6))
sns.regplot(x='longitude', y='injuryIndex', data=parquet_df, line_kws={'color': 'red'})
plt.title('Correlation between Injury Index and Longitude', fontsize=16)
plt.xlabel('Longitude', fontsize=12)
plt.ylabel('Injury Index', fontsize=12)
plt.tight_layout()
plt.savefig('correlation_injury_longitude.png')
plt.close()
"""

# Average injury index by animal type: Lollipop chart
avg_injury_column = 'avgInjuryIndex' if 'avgInjuryIndex' in avg_injury_df.columns else 'avg(injuryIndex)'
plt.figure(figsize=(12, 8))
avg_injury_df = avg_injury_df.sort_values(avg_injury_column, ascending=False)
plt.stem(range(len(avg_injury_df)), avg_injury_df[avg_injury_column].to_numpy(), linefmt='grey', markerfmt='D', bottom=0)
plt.xticks(range(len(avg_injury_df)), avg_injury_df['animalType'], rotation=45, ha='right')
plt.title('Average Injury Index by Animal Type', fontsize=16)
plt.xlabel('Animal Type', fontsize=12)
plt.ylabel('Average Injury Index', fontsize=12)
plt.tight_layout()
plt.savefig('avg_injury_by_animal.png')
plt.close()

# Geographical analysis: Pie chart
plt.figure(figsize=(10, 10))
plt.pie(geo_analysis_df['count'], labels=geo_analysis_df['quadrant'], autopct='%1.1f%%', startangle=90)
plt.title('Incidents by Quadrant', fontsize=16)
plt.axis('equal')
plt.tight_layout()
plt.savefig('incidents_by_quadrant.png')
plt.close()

"""
# Time-based analysis: Line plot

plt.figure(figsize=(12, 6))
plt.plot(time_analysis_df['hour'], time_analysis_df['count'], marker='o')
plt.title('Incidents by Hour of Day', fontsize=16)
plt.xlabel('Hour of Day', fontsize=12)
plt.ylabel('Number of Incidents', fontsize=12)
plt.xticks(range(0, 24))
plt.tight_layout()
plt.savefig('incidents_by_hour.png')
plt.close()"""

# High severity incidents: Scatter plot with color gradient
plt.figure(figsize=(12, 8))
scatter = plt.scatter(high_severity_df['id'], high_severity_df['injuryIndex'],
                      c=high_severity_df['injuryIndex'], cmap='YlOrRd', alpha=0.6)
plt.grid(False)
plt.colorbar(scatter, label='Injury Index')
plt.title('High Severity Incidents', fontsize=16)
plt.xlabel('Incident ID', fontsize=12)
plt.ylabel('Injury Index', fontsize=12)
plt.tight_layout()
plt.savefig('high_severity_incidents.png')
plt.close()

# Top 3 animals by total injury index: Horizontal bar chart
plt.figure(figsize=(12, 8))
top_animals_df = top_animals_df.sort_values('totalInjuryIndex', ascending=True)
plt.barh(top_animals_df['animalType'], top_animals_df['totalInjuryIndex'])
plt.title('Top 3 Animals by Total Injury Index', fontsize=16)
plt.xlabel('Total Injury Index', fontsize=12)
plt.ylabel('Animal Type', fontsize=12)
plt.tight_layout()
plt.savefig('top_animals_by_injury.png')
plt.close()

# Median injury index by animal type: Horizontal bar chart
plt.figure(figsize=(12, 8))
median_injury_df = median_injury_df.sort_values('medianInjuryIndex', ascending=True)
plt.barh(median_injury_df['animalType'], median_injury_df['medianInjuryIndex'])
plt.title('Median Injury Index by Animal Type', fontsize=16)
plt.xlabel('Median Injury Index', fontsize=12)
plt.ylabel('Animal Type', fontsize=12)
plt.tight_layout()
plt.savefig('median_injury_by_animal.png')
plt.close()

# Incidents by day of the week: Horizontal bar chart
plt.figure(figsize=(12, 8))
incidents_by_day_df = incidents_by_day_df.sort_values('count', ascending=True)
plt.barh(incidents_by_day_df['dayOfWeek'], incidents_by_day_df['count'])
plt.title('Incidents by Day of the Week', fontsize=16)
plt.xlabel('Count', fontsize=12)
plt.ylabel('Day of the Week', fontsize=12)
plt.tight_layout()
plt.savefig('incidents_by_day_of_week.png')
plt.close()

# Density of incidents by geographical area: Heatmap
plt.figure(figsize=(12, 8))
heatmap_data = incidents_by_geo_df.pivot(index='latGroup', columns='lonGroup', values='count')
sns.heatmap(heatmap_data, cmap='YlOrRd', annot=True, fmt='g')
plt.title('Density of Incidents by Geographical Area', fontsize=16)
plt.xlabel('Longitude Group', fontsize=12)
plt.ylabel('Latitude Group', fontsize=12)
plt.tight_layout()
plt.savefig('density_of_incidents_by_geo_area.png')
plt.close()

# Correlation between Injury Index and Latitude/Longitude: Bar chart
plt.figure(figsize=(10, 6))
plt.bar(correlations_df['correlationType'], correlations_df['value'], color='skyblue')
plt.title('Correlation of Injury Index with Latitude and Longitude', fontsize=16)
plt.xlabel('Correlation Type', fontsize=12)
plt.ylabel('Correlation Value', fontsize=12)
plt.tight_layout()
plt.savefig('correlation_injury_index.png')
plt.close()


print("All plots have been generated and saved.")

# Stop the Spark session
spark.stop()

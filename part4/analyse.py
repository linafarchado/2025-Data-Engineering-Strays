import pandas as pd
import matplotlib.pyplot as plt
from subprocess import run

# id, timestamp, latitude, longitude, injuryIndex, animalType

df = pd.read_csv("/2025-Data-Engineering-Strays/part4/analysis_result.csv")

# number of animals by type
plt.figure(figsize=(10, 6))
df.plot(kind='bar', x='animalType', y='count', legend=False)
plt.title('Number of animals by type')
plt.xlabel('Animal type
plt.ylabel('Number')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('animal_count.png')

# average injury index by animal type
plt.figure(figsize=(10, 6))
df.plot(kind='bar', x='animalType', y='avgInjuryIndex', legend=False)
plt.title('Average injury index by animal type')
plt.xlabel('Animal type')
plt.ylabel('Injury index')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('avg_injury_index.png')

# heatmap of number of animals by sector
animalCounts = pd.read_csv("/user/lina/analysis/animalCounts/part-*.csv", names=["animalType", "count"])
avgInjuryByAnimal = pd.read_csv("/user/lina/analysis/avgInjuryByAnimal/part-*.csv", names=["animalType", "avgInjury"])
sectorAnalysis = pd.read_csv("/user/lina/analysis/sectorAnalysis/part-*.csv", names=["sector", "animalCount", "avgInjury"])

# Heatmap : Number of animals by sector
plt.figure(figsize=(12, 8))
sectorData = sectorAnalysis.pivot("latitude", "longitude", "animalCount").fillna(0)
plt.imshow(sectorData, cmap="YlOrRd", interpolation="nearest")
plt.colorbar(label="Number of animals")
plt.title("Heatmap : Number of animals by sector")
plt.xlabel("Longitude")")
plt.ylabel("Latitude")
plt.savefig("sectorHeatmap.png")
plt.close()

# Scatter plot : Average injury index by sector
plt.figure(figsize=(12, 8))
lat, lon = zip(*[map(float, sector.split("_")) for sector in sectorAnalysis["sector"]])
plt.scatter(lon, lat, c=sectorAnalysis["avgInjury"], cmap="viridis", alpha=0.7, s=sectorAnalysis["animalCount"]*10)
plt.colorbar(label="Average injury index")
plt.title("Scatter plot : Average injury index by sector")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.savefig("sectorScatter.png")
plt.close()

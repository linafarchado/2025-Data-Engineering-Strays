# Project Launch Instructions

This guide provides step-by-step instructions on how to launch the project. Follow these steps to set up and run the project successfully.

## Prerequisites

Make sure you have the following software installed on your system:

- **park**
- **sbt**
- **Python**

## Python Packages

Ensure you have the following Python packages installed. You can install them using `pip`:

```bash
pip install dash pandas plotly pyspark matplotlib seaborn numpy
```

## Step-by-Step Instructions

### 1. Prepare the Data

If you already have the data in your storage, you can continue to the next steps. If not, follow the data preparation instructions provided in the RADME of parts 1 and 3.

### 2. Run the Scala Analysis

Navigate to the project directory ```part4/``` and use sbt to run the Scala analysis. This will generate the required `.csv` files.

```bash
sbt run
```

### Expected Output Structure

The analysis will create the following directories and files:

```
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/animalDensity.csv
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/animalSeasonData.csv
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/animaltype.csv
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/avgInjury.csv
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/correlations.csv
drwxr-xr-x   - flo supergroup          0 2024-06-28 19:33 /user/flo/dronedata
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/geoAnalysis.csv
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/highSeverity.csv
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/hourlyDailyData.csv
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/incidentsByDayOfWeek.csv
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/incidentsByGeoArea.csv
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/injurySeverity.csv
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/locationData.csv
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/medianInjury.csv
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/seasonalAnalysis.csv
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/seasonalData.csv
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/timeAnalysis.csv
drwxr-xr-x   - flo supergroup          0 2024-07-01 17:03 /user/flo/topAnimals.csv
```

Note: The `.csv` directories contain the actual `.csv` results of the Scala analysis.

### 3. Launch the Python Dashboard

After the Scala analysis has been completed and the `.csv` files are generated, you can launch the Python dashboard.

Navigate to the project directory and run:

```bash
python3 main.py
```

### 4. Access the Dashboard

Open your web browser and go to:

```
http://127.0.0.1:8050
```

You will be able to see all the generated graphics and interact with the dashboard.

## Conclusion

By following these steps, you will be able to successfully set up and run the project, generating the necessary data and visualizing it through a Python dashboard.
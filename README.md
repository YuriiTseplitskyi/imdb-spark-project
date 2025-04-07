# IMDB Spark Project

A data analysis project using Apache Spark to process and analyze IMDB data.

## Project Overview

This project provides analysis of IMDB data using PySpark. It includes predefined schemas for all IMDB datasets and utility functions for data exploration and analysis.

## Getting Started

### 1. Clone the repository

```bash
git clone <repository-url>
cd imdb_spark_project
```

### 2. Download IMDB datasets

Create the data directory structure:

```bash
mkdir -p data/imdb
```

#### For Windows PowerShell:

```powershell
Invoke-WebRequest -Uri "https://datasets.imdbws.com/title.basics.tsv.gz" -OutFile "$HOME/imdb_spark_project/data/imdb/title.basics.tsv.gz"
Invoke-WebRequest -Uri "https://datasets.imdbws.com/title.ratings.tsv.gz" -OutFile "$HOME/imdb_spark_project/data/imdb/title.ratings.tsv.gz"
Invoke-WebRequest -Uri "https://datasets.imdbws.com/title.crew.tsv.gz" -OutFile "$HOME/imdb_spark_project/data/imdb/title.crew.tsv.gz"
Invoke-WebRequest -Uri "https://datasets.imdbws.com/title.akas.tsv.gz" -OutFile "$HOME/imdb_spark_project/data/imdb/title.akas.tsv.gz"
Invoke-WebRequest -Uri "https://datasets.imdbws.com/title.episode.tsv.gz" -OutFile "$HOME/imdb_spark_project/data/imdb/title.episode.tsv.gz"
Invoke-WebRequest -Uri "https://datasets.imdbws.com/title.principals.tsv.gz" -OutFile "$HOME/imdb_spark_project/data/imdb/title.principals.tsv.gz"
Invoke-WebRequest -Uri "https://datasets.imdbws.com/name.basics.tsv.gz" -OutFile "$HOME/imdb_spark_project/data/imdb/name.basics.tsv.gz"
```

#### For Linux/macOS:

```bash
wget -P $HOME/imdb_spark_project/data/imdb https://datasets.imdbws.com/title.basics.tsv.gz
wget -P $HOME/imdb_spark_project/data/imdb https://datasets.imdbws.com/title.ratings.tsv.gz
wget -P $HOME/imdb_spark_project/data/imdb https://datasets.imdbws.com/title.crew.tsv.gz
wget -P $HOME/imdb_spark_project/data/imdb https://datasets.imdbws.com/title.akas.tsv.gz
wget -P $HOME/imdb_spark_project/data/imdb https://datasets.imdbws.com/title.episode.tsv.gz
wget -P $HOME/imdb_spark_project/data/imdb https://datasets.imdbws.com/title.principals.tsv.gz
wget -P $HOME/imdb_spark_project/data/imdb https://datasets.imdbws.com/name.basics.tsv.gz
```

### 3. Run with Docker

You can also run the project using Docker:

1. Switch to Linux containers:
    ```bash
    docker switch linux
    ```

1. Build the Docker image:
   ```bash
   docker build -t imdb_spark_project .
   ```

2. Run the container:
   ```bash
   docker run -it --rm -p 8888:8888 imdb_spark_project
   ```

3. Open the Jupyter Notebook URL provided in the console.

## Project Structure

- `schemas.py`: Contains schema definitions for all IMDB datasets and creates PySpark DataFrames
- `tools.py`: Utility functions for data analysis
- `notebooks/`: Jupyter notebooks for analysis
- `data/imdb/`: Directory for IMDB dataset files
- `data/results/`: Directory for analysis results

## Available Datasets

- `title_basics`: Basic information for titles
- `title_ratings`: Ratings information for titles
- `title_crew`: Directors and writers for titles
- `title_akas`: Alternative titles
- `title_episode`: Episode information for TV series
- `title_principals`: Principal cast/crew for titles
- `name_basics`: Information about people (actors, directors, etc.)
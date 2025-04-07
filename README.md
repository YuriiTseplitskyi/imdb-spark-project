# 🎬 IMDB Spark Project

A data analysis project using **Apache Spark** and **PySpark** to process and analyze data from the IMDB dataset.

## 📌 Overview

This project provides tools to explore and analyze the IMDB dataset using **PySpark**, with schemas and utilities for data transformation, querying, and visualization.

Features:
- Predefined schemas for all IMDB datasets
- Utility functions for efficient analysis
- Interactive exploration with Jupyter Notebooks
- Dockerized environment for easy setup and reproducibility


## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone <repository-url>
cd imdb_spark_project
```

### 2. Run with Docker

To build and run the Docker container:
   ```bash
   docker compose up --build
   ```

For subsequent runs:
   ```bash
   docker compose up
   ```
### 3. Open Jupiter Notebook editor locally
After the Docker container starts, a URL for the Jupyter Notebook editor will appear in your terminal. The link will begin with ```http://localhost:8888/lab?token=``` followed by a unique authentication token. To access the editor, either:
- Hold CTRL and click the URL (or CMD+click on Mac), or
- Copy the entire URL (including the token) and paste it into your web browser
### 4. Create directory for IMDB datasets
1. After Jupiter Notebook editor is opened, you need to open a terminal there and enter ```code``` directory:
   ```bash
   cd code
   ```
2. Create the data directory structure:
   ```bash
   mkdir -p data/imdb
   ```
3. Execute cell in the ```dataset_loader.ipynb``` file to load the dataset.
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
# Use the official Jupyter PySpark notebook image as the base
FROM jupyter/pyspark-notebook:latest

# Set the working directory inside the container
WORKDIR /home/jovyan/work

# Copy the local 'notebooks' and 'data' directories into the container
COPY notebooks /home/jovyan/work/notebooks
COPY data /home/jovyan/work/data

# Set environment variables to configure Jupyter Notebook
ENV JUPYTER_ENABLE_LAB="no"
ENV NOTEBOOK_DIR="/home/jovyan/work/notebooks"

# Expose the port that Jupyter Notebook will run on
EXPOSE 8888

#! commands
# docker build -t imdb_spark_project .
# docker run -it --rm -p 8888:8888 imdb_spark_project

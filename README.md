# Pharmaceutical Companies Big Data Manipulation Project

## Project Overview

This project focuses on the manipulation and analysis of large datasets related to pharmaceutical companies, including clinical trial data from multiple years. The main objective is to perform data cleaning, preprocessing, and analysis using big data technologies like Apache Spark, providing insights that can support decision-making in the pharmaceutical industry.

## Repository Contents

- **Pharmaceutical_Project_RDD_Notebook.ipynb**  
  - **Description**: This notebook handles file operations, such as unzipping datasets, and prepares them for further analysis.
  - **Key Functions**:
    - `unzipper`: A utility function designed to unzip data files from a specified directory to another location within the Databricks File System (DBFS). This function is particularly useful for managing multiple compressed datasets.
  - **Libraries Used**: `pandas`, `matplotlib`, `seaborn`, `PySpark`, `csv`, `os`.

- **Pharmaceutical_Project_DataFrame_Notebook.ipynb**  
  - **Description**: This notebook focuses on data cleaning and preprocessing using PySpark DataFrames, a crucial step in preparing the data for analysis.
  - **Key Functions**:
    - `preprocessorDF`: A comprehensive data cleaning function that:
      - Reads the input file into an RDD.
      - Handles different delimiters (`|`, `\t`) in the dataset.
      - Replaces missing values with `NA`.
      - Converts the cleaned RDD to a PySpark DataFrame.
  - **Data Files Processed**:
    - `clinicaltrial_2020.csv`
    - `clinicaltrial_2021.csv`
    - `clinicaltrial_2023.csv`
    - `pharma.csv`
  - **Libraries Used**: `pandas`, `matplotlib`, `seaborn`, `PySpark`.

## Getting Started

To run these notebooks, follow these steps:

1. **Clone the repository** to your local machine or a cloud-based environment like Databricks.
2. **Install the required libraries**:
   ```bash
   pip install pandas matplotlib seaborn pyspark

# Data Sources
The data used in this project includes clinical trial data from multiple years (2020, 2021, 2023) and other pharmaceutical datasets. These datasets are expected to be in CSV format and may require unzipping and preprocessing before analysis.

# Key Features
- Flexible Data Processing: The use of PySpark allows for handling large datasets efficiently.
- Automated Data Cleaning: The preprocessorDF function provides a robust method for cleaning and preparing the data for analysis.
- Visualization Support: Built-in support for visualizing data trends and patterns using matplotlib and seaborn.
# Usage Notes
- Ensure that your environment is properly configured to run PySpark.
- Some functions (e.g., unzipper) may need modifications based on the file paths and environment settings.

### Description

The Car Crash Analysis Project is a data analysis tool for examining car crash data. It provides various analytics functions to extract valuable insights from the data, such as identifying accident trends, demographic statistics, and vehicle-related information.

## Table of Contents
Installation
Usage
Project Structure



<!-- Installation -->
To use this project, follow these steps:

Clone the repository to your local machine:

bash
git clone https://github.com/abakashbws/car-crash-analysis.git

<!-- Install the required dependencies: -->
pip install -r requirements.txt
Set up Spark (if not already installed) and configure it according to your environment.

Make sure you have the necessary data files and update the configuration file (config/config.json) with the correct paths to your data files.

<!-- Usage -->
To run the Car Crash Analysis tool, execute the following command:
python main.py
This will execute the analysis and display the results in the console.

You can customize the analytics functions and configurations in the analysis.py and config.json files to suit your specific analysis requirements.

Project Structure
The project is organized as follows:

Car_crash: The main package containing the analysis and utility modules.
config: Configuration files, including config.json for specifying data paths.
data: Data files used for analysis.
main.py: The main entry point for running the analysis.
spark_setup.py: Configuration and setup for Spark.
utils.py: Utility functions used for data validation and JSON parsing.
Car_crash/analysis.py: Contains the analytics classes and methods.
Car_crash/__init__.py: Initializes the package and sets up logging.
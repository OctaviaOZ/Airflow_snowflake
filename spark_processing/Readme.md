# Data Engineer Assignment

This repository contains the code for a data processing task as part of a data engineer assignment. 

## Solution description

In the provided solution, two distinct approaches have been employed—a comprehensive Jupyter notebook and a meticulously crafted Python script. The Jupyter notebook, upon initial inspection, offers an in-depth exploration of IBAN validation methods. However, it quickly became apparent that the dataset exhibits an artificial nature, thus presenting limitations for the application of advanced algorithms. Consequently, a more straightforward regular expression was adopted for IBAN validation.

The underlying methodology of the solution relies on certain assumptions regarding the structure of IBANs. It assumes the presence of four fundamental components within each IBAN: a country code, a bank code, an account number, and the final two digits representing subaccounts. In consideration of these assumptions, the decision was made to exclude subaccounts from consideration. Instead, the combination of the bank code and account number is treated as a unique identifier, denoted as the 'id' for each customer. This forms the basis for a grouping mechanism designed to categorize customers based on these identifiers.


## Code Overview

## Jupyter Notebook `research_data_task.ipynb`:

Code sections:

### IBAN Validation Class:
Defines a class Iban for IBAN validation, with methods for checking IBAN validity and computing check digits.
It has a class-level constant IBAN_ALPHABET for character-to-digit mapping.

### Spark Session and Schema Setup:
Initializes a SparkSession for processing data.
Defines a schema for the CSV file containing 'id', 'name', and 'iban' fields.

### Data Loading and Cleaning:
Loads the CSV data into a DataFrame, removing spaces from the 'iban' column.

### IBAN Validation:
Implements IBAN validation using a regular expression.
Creates a new column 'is_valid' indicating the validity of the 'iban_clean' column.

### Entity Resolution:
Filters rows with valid IBANs.
Extracts 'bank_account' and 'subaccount' from 'iban_clean'.
Groups by 'bank_account' and aggregates related data.

### Display and Output:
Shows intermediate and final DataFrames.
Displays the schema.


## Python Code `data_enginneer_assigment.py`:

An implementation of the data processing tasks mentioned in the task description. Here's an overview of the code:

### CounterpartyDataProcessor Class:
Initializes with input and output paths, creates a SparkSession, and defines the data schema.

### validate_iban Method:
Validates IBAN format using a regular expression.

### resolve_entities Method:
Filters valid IBANs, extracts 'bank_account' and 'subaccount', and aggregates data by 'bank_account'.

### process Method:
Loads the input CSV data.
Cleans and validates IBANs.
Resolves entities and writes the output to a new CSV file.
Logs errors and completion messages.

### Command-Line Interface:
Parses command-line arguments for input and output paths.
Creates an instance of CounterpartyDataProcessor and processes the data.

## Usage

To run the code, you can use the following command:

```python data_engineer_assignment.py input_file.csv output_file.csv```

Replace `input_file.csv` and `output_file.csv` with the actual file paths for your input and output files, respectively.

## Dependencies:
hadoop-3.1.2
pyspark==3.2.4
Spark 3.2.4
JAVA_VERSION="1.8.0_381"
import csv
import pandas as pd
import logging

def identify_delimiter(file_path):
    # Open the file and read the first 3 lines
    with open(file_path, 'r') as file:
        lines = [file.readline() for _ in range(3)]

    # Join the first 3 lines to form a sample text
    sample = ''.join(lines)

    # Common delimiters to check
    delimiters = [',', ';', '\t', '|']
    delimiter_counts = {delimiter: sample.count(delimiter) for delimiter in delimiters}

    # Find the delimiter with the highest count
    identified_delimiter = max(delimiter_counts, key=delimiter_counts.get)

    return identified_delimiter


def read_and_infer(file_path):
    # Identify the delimiter
    delimiter = identify_delimiter(file_path)
    logging.info(f"Identified delimiter: {delimiter}")

    # Use pandas to read the file with the identified delimiter
    df = pd.read_csv(file_path, delimiter=delimiter)

    # Get the header (column names)
    columns = [col.replace(" ","_").upper() for col in df.columns.tolist()]
    logging.info(f"Columns: {columns}")

    # Infer the data types of each column
    data_types = df.dtypes
    logging.info(f"Data Types:{data_types}")

    return delimiter, columns, data_types
import logging
import os
import subprocess
import yaml
import pandas as pd
import datetime 
import gc
import re

def read_config_file(filepath):
    with open(filepath, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)
            
def read_csv_file(filepath, delimiter):
    try:
        df = pd.read_csv(filepath, delimiter=delimiter)
        return df
    except Exception as exc:
        logging.error(exc)
        return None

def standardize_column_names(df):
    df.columns = [col.strip().replace(' ', '_').lower() for col in df.columns]
    return df

def validate_columns(df, yaml_filepath):
    try:
        with open(yaml_filepath, 'r') as yaml_file:
            schema = yaml.safe_load(yaml_file)
        expected_columns = schema['columns']
        if list(df.columns) != expected_columns:
            raise ValueError("Column names or order do not match the schema.")
        if len(df.columns) != len(expected_columns):
            raise ValueError("Number of columns do not match the schema.")
    except Exception as exc:
        logging.error(exc)
        return False
    return True

def write_gz_file(df, output_filepath, delimiter):
    try:
        with gzip.open(output_filepath, 'wt') as gz_file:
            df.to_csv(gz_file, sep=delimiter, index=False)
    except Exception as exc:
        logging.error(exc)

def summary(df, filepath):
    summary = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'file_size': os.path.getsize(filepath)
    }
    return summary


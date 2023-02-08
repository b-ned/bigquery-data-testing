# import json
# from google.cloud import bigquery
# import pandas as pd


class BigQueryUniquenessTest:
    def __init__(self, client, table_name, column_name):
        self.table_name = table_name
        self.column_name = column_name
        self.client = client
        self.status = None
        self.name = f"{self.table_name}-{self.column_name}-unique"
        self.labels = {
            "dest_table": table_name,
            "column_name": column_name,
        }

    def run_test(self):
        query = f"SELECT COUNT(DISTINCT {self.column_name}) as unique_count, COUNT(*) as total_count FROM {self.table_name}"
        results = self.client.query(query).to_dataframe()
        unique_count = results.iloc[0]["unique_count"]
        total_count = results.iloc[0]["total_count"]
        if unique_count == total_count:
            print(f"Test passed")
            self.status = "success"
        else:
            print(f"Test failed")
            self.status = "failed"


class BigQueryNullTest:
    def __init__(self, client, table_name, column_name):
        self.table_name = table_name
        self.column_name = column_name
        self.client = client
        self.status = None
        self.name = f"{self.table_name}-{self.column_name}-notnull"
        self.labels = {
            "dest_table": table_name,
            "column_name": column_name,
        }

    def run_test(self):
        query = f"SELECT COUNT(*) as total_count, COUNTIF({self.column_name} IS NULL) as null_count FROM {self.table_name}"
        query_job = self.client.query(query)
        results = query_job.result()
        row = results.to_dataframe().iloc[0]
        if row["null_count"] == 0:
            print(f"Test passed")
            self.status = "success"
        else:
            print(f"Test failed")
            self.status = "failed"


class BigQueryReferentialIntegrityTest:
    def __init__(self, primary_table, primary_key, foreign_table, foreign_key, client):
        self.primary_table = primary_table
        self.primary_key = primary_key
        self.foreign_table = foreign_table
        self.foreign_key = foreign_key
        self.client = client
        self.status = None
        self.name = f"{self.primary_table}-{self.primary_key}-ref-int"
        self.labels = {
            "primary_table": primary_table,
            "primary_key": primary_key,
            "foreign_table": foreign_table,
            "foreign_key": foreign_key,
        }

    def run_test(self):
        query = f"SELECT COUNT(*) as total_rows FROM {self.primary_table} p LEFT JOIN {self.foreign_table} f ON p.{self.primary_key} = f.{self.foreign_key} WHERE f.{self.foreign_key} IS NULL"
        query_job = self.client.query(query)
        results = query_job.result()
        row = results.fetchone()
        if row.total_rows == 0:
            print(f"Test passed")
            self.status = "success"
        else:
            print(f"Test failed")
            self.status = "failed"


class BigQueryNullRatioTest:
    def __init__(self, client, table_name, column_name, threshold = 0.2):
        self.client = client
        self.table_name = table_name
        self.column_name = column_name
        self.threshold = threshold
        self.status = None
        self.name = f"{self.table_name}-{self.column_name}-null-ratio"
        self.labels = {
            "dest_table": table_name,
            "column_name": column_name,
            "threshold": threshold,
        }

    def run_test(self):
        query = f"SELECT {self.column_name}, COUNT(*) as total_count, SUM(CASE WHEN {self.column_name} IS NULL THEN 1 ELSE 0 END) as null_count FROM {self.table_name} GROUP BY {self.column_name}"
        query_job = self.client.query(query)
        results = query_job.to_dataframe()
        ratio = results["null_count"].sum() / results["total_count"].sum()
        if self.threshold and ratio > self.threshold:
            print(f"Test failed")
            self.status = "failed"
        else:
            print(f"Test passed")
            self.status = "success"

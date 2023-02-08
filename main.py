
from google.cloud import bigquery
import datetime
import pandas as pd
import json
from test_config import test_configs

# BigQuery Project Configuration
test_log_table = 'mydataset.test_results'
project="enter_your_project_here"

# Entry Point Cloud Function
def run_all_tests(requests):
    # Set up client
    client = bigquery.Client(project)

    # Create an empty list to store test results
    test_results = []

    # Run tests
    for test_config in test_configs:
        test_class = test_config['class']
        test_instance = test_class(client, **test_config['test_params'])
        test_start_time = datetime.datetime.now()
        test_instance.run_test()
        test_end_time = datetime.datetime.now()
        test_results.append({
            'name': test_instance.name,
            'status': test_instance.status,
            'start_time': test_start_time,
            'end_time': test_end_time,
            'labels': test_instance.labels,
        })

    # Convert test results to a DataFrame
    test_results_df = pd.DataFrame(test_results)
    test_results_df['labels'] = test_results_df['labels'].apply(json.dumps)

    # Write test results to a BigQuery table
    test_results_df.to_gbq(test_log_table, project_id=project, if_exists='append')
    return 'Successfully ran all tests'

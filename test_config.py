from test_classes import BigQueryUniquenessTest

test_configs = [
    {
        'class': BigQueryUniquenessTest,
        'test_params':
            {
                'table_name': 'mydataset.transaction_fact',
                'column_name': 'transaction_id'
            }
    },
    {
        'class': BigQueryUniquenessTest,
        'test_params':
            {
                'table_name': 'mydataset.transaction_fact',
                'column_name': 'transaction_date'
            }
    },
]

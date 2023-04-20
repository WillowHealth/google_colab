from google.cloud import bigquery
from google.cloud import firestore
from datetime import datetime

def firestore_to_bigquery(event):
    # Set up the Firestore client
    firestore_client = firestore.Client()
    print("firestore to bigquery running")
    # Set up the BigQuery client
    bigquery_client = bigquery.Client()

    # Set up the dataset and table IDs for BigQuery
    dataset_id = 'firestore_export'
    table_id = 'yassine_historical_data_once'

    # Set up the BigQuery table schema
    table_schema = [
        bigquery.SchemaField('userid', 'STRING'),
        bigquery.SchemaField('date', 'DATE'),
        bigquery.SchemaField('hour', 'INTEGER'),
        bigquery.SchemaField('data', 'STRING')
    ]
    # Create a BigQuery table object
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=table_schema)

    # Create the table in BigQuery with the specified schema
    table = bigquery_client.create_table(table)

    # Write the rows to the BigQuery table
    # Query Firestore for all the documents in the collection
    collection_ref = firestore_client.collection('New_Sample_Users_RealTime')
    docs = collection_ref.list_documents()

    # Create a list to hold the rows to be inserted into BigQuery
    rows = []

    # Iterate over the documents in the collection and create a row for each document
    for doc in docs:
        print(doc)
        subcollection_ref = doc.collection('dates')
        subdocs = subcollection_ref.list_documents()
        for subdoc in subdocs:
            subsubcollection_ref = subdoc.collection('hours')
            subsubdocs = subsubcollection_ref.list_documents()
            for subsubdoc in subsubdocs:
                hourvalue=subsubdoc.id
                print(hourvalue)
                hourvalue=hourvalue.split(':')
                print(hourvalue)
                hourvalue=hourvalue[0]
                print(hourvalue)

                row = {
                    'userid': doc.id,
                    'date': datetime.strptime(subdoc.id, '%Y-%m-%d').date(),
                    'hour': int(hourvalue),
                    'data': str(subsubdoc.get().to_dict())
                }
                print(row)
                errors = bigquery_client.insert_rows(table, [row])
                if errors != []:
                    print(f'Error inserting rows into BigQuery table: {errors}')
                else:
                    print(f'Successfully inserted {len(rows)} rows into BigQuery table.')

  
    if len(rows)>0:
        print("case 1")
    else: 
        print("case 2")

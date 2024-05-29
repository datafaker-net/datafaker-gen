## DataFaker Gen BigQuery Sink

The BigQuery Sink allows for the generation and sending of data to Google BigQuery.
This sink can generate and send documents either individually or in batches in one message. It depends on `batchSize` property specified in `output.yaml` configuration.

## How to run 

### Configure BigQuery Sink

All configuration is in `output.yaml` file.
```bash
sinks:
  bigquery:
    batchsize: 10
    project_id: _project_id_
    dataset: datafaker
    table: users
    service_account: path_to_service_account.json
    create_table_if_not_exists: true
    max_outstanding_elements_count: 100
    max_outstanding_request_bytes: 10000
    keep_alive_time_in_seconds: 60
    keep_alive_timeout_in_seconds: 60
```

### Run

### How to configure Credentials

How to add credentials to your environment, so that the DataFaker Gen can authenticate with Google BigQuery.

There are three ways to authenticate with Google BigQuery:
  
1. Add `SERVICE_ACCOUNT_SECRET` to your environment. This property should contain content of the service account key file in JSON format.
2. Use the `service_account` property in the `output.yaml` file. This property should contain the path to the service account key file.
3. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of the JSON key file that contains your service account key. This variable only applies to your current shell session. To set it for future sessions, you will need to modify your shell's configuration file.
4. Use the `gcloud` command-line tool to authenticate. You can use the `gcloud auth application-default login` command to authenticate with Google Cloud Platform services.

```bash
 bin/datafaker_gen -f json -n 10000 -sink bigquery 
```

## TODO
 
 - [ ] Add support for more data types (date, timestamp, array, etc.)
 - [ ] Add support for more configuration options (create table, etc.)

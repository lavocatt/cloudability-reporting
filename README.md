# Program to export data from cloudability

This program fetches the data from cloudability and can export it to different
sources.

* First of all print it on screen
* Export it to a csv file
* Export it to a parquet file

It has also the possibility to upload the data to a s3 bucket.

## usage:

```
python main.py --token-command "pass cloudability_secret" --days 7 --bucket-name cloudability-test-storage --aws-access-key-id "$KEY_ID" --aws-secret-access-key "$KEY_SECRET" --region-name us-east-1 parquet --filename /tmp/something
```

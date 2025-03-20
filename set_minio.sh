mc alias set local http://localhost:9000 admin password
mc mb --ignore-existing local/spark-data
mc policy set public local/spark-data
mc cp --recursive /batches/* local/spark-data/raw
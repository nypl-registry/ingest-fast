# ingest-fast
[![travis](https://travis-ci.org/nypl-registry/ingest-fast.svg)](https://travis-ci.org/nypl-registry/ingest-fast/)

The FAST data ingest process, it builds the fast lookup table in the registry-ingest database and then modifies the viaf lookup table with FAST ids for Events/Corporate/Personal names.

These methods require you have the data downloaded and prepared before executing.

For the inital ingest of clusters and persist data you need to run [download_data.sh](download_data.sh):
```
chmod +x download_data.sh
./download_data.sh 
```



The ingest process is found in `ingest`. These can be run mannualy but would normally be called from the [dispatch module](https://github.com/nypl-registry/dispatch) as a job.


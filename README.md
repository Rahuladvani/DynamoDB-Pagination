This is a example application to implement different forms of pagination in DynamoDB. In-depth explanation of the storage layer of this repository can be found in <>. 

To run this project DynamoDB needs to be installed locally and run on port `8000`.

For local installation of DynamoDB follow - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html

To visualize data in DynamoDB and manage tables download NoSQL Workbench - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/workbench.settingup.html

For creating the stats table and global index, run `sh scripts/soccer_app_create_table.sh`. The script also inserts a sample record/item in the table based on the JSON defined under `scripts/stats_app_insert.json`

Once DynamoDB is installed locally, and the stats table is created, run the project using

`go run main.go`
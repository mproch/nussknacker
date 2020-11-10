Setup for simple performance tests on demo cluster. 
Scripts in avroloader generate test data and feed them to demo Kafka.
- ```schema1.avsc``` - schema for events, it has >20 additional fields to simulate broad records
- ```generate.sc``` - ammonite script which producers 10M records (as JSON) for given schema and prints them to stdout
- ```runproducer.sh``` - script which register test schema and sends data from stdin to given topic.
- To run full load: ```amm generate.sc | ./runproducer.sh transactionstopic```

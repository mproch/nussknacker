#!/bin/bash

TOPIC=$1
SCHEMA=schema1.avsc

echo "{\"schema\": " > /tmp/$SCHEMA
cat $SCHEMA | jq -Rs . >> /tmp/$SCHEMA
echo "}" >> /tmp/$SCHEMA
ID=`curl -q POST -v -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d@/tmp/$SCHEMA \
  http://localhost:3082/subjects/$TOPIC-value/versions | jq .id`

echo "Schema: ${ID}"

docker run --entrypoint=kafka-avro-console-producer -i  \
  --link=nussknacker_schemaregistry:schemaregistry --link=nussknacker_kafka:kafka --net docker_default \
  confluentinc/cp-schema-registry:latest \
  --topic $TOPIC \
  --bootstrap-server kafka:9092 \
  --property value.schema.id=$ID \
  --property schema.registry.url=http://schemaregistry:8081

#!/bin/sh

export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc

genavro(){
	echo generating avro...
	cat sample.d/sample.jsonl |
		json2avrows |
		cat > sample.d/sample.avro
}

test -f ./sample.d/sample.avro || genavro

export ENV_TAG_NAME=tags
export ENV_BODY_NAME=body

cat sample.d/sample.avro |
	./avro-log-tag-ipv4 |
	rq -aJ |
	jq -c

#!/bin/sh

DB_NAME="AMPds2"
TARGET="mock_data.txt"
PREFIX="mock_"

split --additional-suffix=.txt -l 5000 -a 3 ${TARGET} ${PREFIX}
for x in ${PREFIX}*.txt; do
	sed -i '1i # DDL\nCREATE DATABASE '"$DB_NAME"'\n\n# DML\n# CONTEXT-DATABASE: '"$DB_NAME"'\n# CONTEXT-RETENTION-POLICY: autogen\n' ${x}
	influx -import -path=${x} -precision=s
done

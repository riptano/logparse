#!/bin/sh
SCHEMA_PATH=`dirname $0`
cqlsh -f $SCHEMA_PATH/systemlog.cql
dsetool create_core logparse.systemlog schema=$SCHEMA_PATH/schema.xml solrconfig=$SCHEMA_PATH/solrconfig.xml

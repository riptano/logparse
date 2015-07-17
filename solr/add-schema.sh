#!/bin/sh

# options parser sets the following variables:
# HOST - server hostname
# SCHEME  - (http|https)
# CERT_FILE - client certificate file
# AUTH_OPTS - Additional security options for curl
cd `dirname $0`
. ./set-solr-options.sh $*

cqlsh -f ../systemlog.cql

SOLRCONFIG_URL="$SCHEME://$HOST:8983/solr/resource/logparse.systemlog/solrconfig.xml"
SOLRCONFIG=solrconfig.xml

echo "Posting $SOLRCONFIG to $SOLRCONFIG_URL..."
curl -s $AUTH_OPTS $CLIENT_CERT_FILE $CERT_FILE --data-binary @$SOLRCONFIG -H 'Content-type:text/xml; charset=utf-8' $SOLRCONFIG_URL
echo "Posted $SOLRCONFIG to $SOLRCONFIG_URL"

SCHEMA_URL="$SCHEME://$HOST:8983/solr/resource/logparse.systemlog/schema.xml"
SCHEMA=schema.xml

echo "Posting $SCHEMA to $SCHEMA_URL..."
curl -s $AUTH_OPTS $CLIENT_CERT_FILE $CERT_FILE --data-binary @$SCHEMA -H 'Content-type:text/xml; charset=utf-8' $SCHEMA_URL
echo "Posted $SCHEMA to $SCHEMA_URL"

CREATE_URL="$SCHEME://$HOST:8983/solr/admin/cores?action=CREATE&name=logparse.systemlog"

echo "Creating index..."
curl -s $AUTH_OPTS $CLIENT_CERT_FILE $CERT_FILE  $CREATE_URL 
echo "Created index."

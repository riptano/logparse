#!/bin/sh

###############################################################################
# Common options parsing for Solr demo scripts. Recognises a number of options
# related to DSE security settings. 
# Use the -a option to enable Kerberos authentication 
# Kerberos authentication requires request  URLs to use the correct fqdn for 
# the host.Where `hostname -f` returns this correctly, use the -h option to 
# provide correct hostname. 
# 
# To enable SSL encryption of HTTP requests, use the -e option and supply
# a path to a valid client certificate file (curl expects these in pem format)
# When require_client_auth is set to true in cassandra.yaml, use the -E option
# to specify the client certificate file. The option value is passed onto curl
# verbatim, so if the cert file requires a password see the curl docs for how
# to specify it.
# Use the -k option to disable cert chain checking
############################################################################### 

SCHEME="http"
HOST=`hostname -f`
USERNAME="user"
PASSWORD="pass"
AUTH_OPTS=""

CREDENTIALS_FLAG=0
KRB_FLAG=0

while getopts ":ae:h:ku:p:E: " opt; do
  case $opt in
    a)
      # Additional options for GSSAPI on secure DSE nodes. The user & password are required, but
      # ignored by Curl's authentication.
      AUTH_OPTS="--negotiate -b .cookiejar.txt -c .cookiejar.txt" 
      KRB_FLAG=1
      ;;
    h)
      HOST=$OPTARG;
      ;;
    e)
      SCHEME="https"
      CERT_FILE="$CERT_FILE --cacert $OPTARG"
      ;;
    k)
      CERT_FILE="-k $CERT_FILE"
      ;;
    E)
      CLIENT_CERT_FILE="--cert $OPTARG"
      ;; 
    p)
      PASSWORD=$OPTARG
      CREDENTIALS_FLAG=`expr $CREDENTIALS_FLAG + 1`
      ;;
    u)
      USERNAME=$OPTARG
      CREDENTIALS_FLAG=`expr $CREDENTIALS_FLAG + 1`
      ;;
    :)
      "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
    \?)
      ## ignore unknown options as they may be specific to a particular demo
      ;;
  esac
done

if [ $CREDENTIALS_FLAG -gt 0 -a  $CREDENTIALS_FLAG -lt 2 ]
then
  echo "Please supply both a username and password" >&2
  exit 1;
fi 

# if kerberos is enabled, we need to supply a set of credentials, 
# (it doesn't matter what they are, so we can use the defaults).
# if a username & password were supplied we'll use those, either
# for SPNEGO or HTTP Basic auth
if [ $KRB_FLAG -eq 1 -o $CREDENTIALS_FLAG -gt 0 ] 
then
  AUTH_OPTS="${AUTH_OPTS} -u${USERNAME}:${PASSWORD}"
fi

export HOST
export SCHEME
export CERT_FILE
export CLIENT_CERT_FILE
export AUTH_OPTS

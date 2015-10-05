# Cassandra system.log parser

This is a rule-based Cassandra `system.log` parser.  The [systemlog.py](systemlog.py) file contains
a set of rules that define how to parse the system.log. The `parse_log` generator applies
these rules and yields a dictionary containing a single event from the log at a time.
It takes as input another generator which should yield one line from the log at a time.
This works perfectly with `fileinput` from the python standard library.

## log_to_json

The [log_to_json](log_to_json) script parses system.log and outputs events in JSON format with one
event per line.  It takes a list of log files on the command line and parses them.
If no arguments are supplied it will attempt to parse stdin. This can be used to parse
a live log file by piping from tail: `tail -f /var/log/cassandra/system.log | log_to_json`.

## cassandra_ingest

The [cassandra_ingest](cassandra_ingest) script parses system.log and inserts each event into the
logparse.systemlog table defined in [systemlog.cql](systemlog.cql). It takes a list of log files
on the command line and parses them.  If no arguments are supplied it will attempt 
to parse stdin. This can be used to parse a live log file by piping from tail: 
`tail -f /var/log/cassandra/system.log | cassandra_ingest`.

The systemlog table contains a standard set of fields that are common to each event,
as well as a set of collections of various types, and fields that do not exist in 
the table are automatically inserted into the appropriate collection depending on
the type of data it contains. This allows event-specific fields to be saved 
automatically.  The naming convention of these collections allows them to be treated 
as dynamic fields when the table is indexed in DSE's implementation of Solr.

The [cassandra_store.py](cassandra_store.py) file contains the code to save the log data into Cassandra.
It requires the [DataStax Python Driver](https://github.com/datastax/python-driver).
It should be installed by running `pip install cassandra-driver`. The script currently assumes
that Cassandra is running on `localhost` on the default CQL port 9042.

## Solr indexing

The logparse.systemlog table can be indexed using the Solr implementation from 
[DataStax Enterprise](http://docs.datastax.com/en/datastax_enterprise/4.7//datastax_enterprise/newFeatures.html).
A [schema.xml](solr/schema.xml) and [solrconfig.xml](solr/solrconfig.xml) are provided
in the [solr](solr) directory along with the [add-schema.sh](solr/add-schema.sh) script 
which will upload the Solr schema to DSE.  

Once indexed in Solr, the log events can be subsequently analyzed and visualized using 
[Banana](https://github.com/LucidWorks/banana).  Banana is a port of Kibana 3.0 to Solr.
Several pre-made dashboards are saved in json format in the [banana](banana) subdirectory. 
These can be loaded using the Banana web UI.

Setup Instructions:

1. Clone https://github.com/LucidWorks/banana to $DSE_HOME/resources/banana.
   Make sure you've checked out the release branch (should be the default).
   If you want, you can `rm -rf .git` at this point to save space.
   
2. Edit resources/banana/src/config.js and:
   - change `solr_core` to the core you're most frequently going to work with (only a 
     convenience, you can pick a different one later on the settings for each dashboard.
   - change `banana_index` to `banana.dashboards` (can be anything you want, but modify step 
     3 accordingly). Not strictly necessary if you don't want to save dashboards to solr.

3. Post the banana schema from `resources/banana/resources/banana-int-solr-4.5/banana-int/conf`
   - Use the `solrconfig.xml` from this project instead of the one provided by banana
   - Name the core the same name specified above in step 2.
   - Not strictly necessary if you don't want to save dashboards to solr.

   ```
   curl --data-binary @solrconfig.xml -H 'Content-type:text/xml; charset=utf-8' "http://localhost:8983/solr/resource/banana.dashboards/solrconfig.xml"
   curl --data-binary @schema.xml -H 'Content-type:text/xml; charset=utf-8' "http://localhost:8983/solr/resource/banana.dashboards/schema.xml"
   curl -X POST -H 'Content-type:text/xml; charset=utf-8' "http://localhost:8983/solr/admin/cores?action=CREATE&name=banana.dashboards"
   ```

4. Edit resources/tomcat/conf/server.xml and add the following inside the <Host> tags:

   ```
   <Context docBase="../../banana/src" path="/banana" />
   ```
   
5. If you've previously started DSE, remove `resources/tomcat/work` and restart.

6. Start DSE and go to http://localhost:8983/banana


## Definining Rules

The rules governing the parsing of `system.log` are defined in the [systemlog.py](systemlog.py) file.
These are specified using a Domain-Specific Language defined by the functions in [rules.py](rules.py).
The rules are explained by docstrings in rules.py, and existing rules found in systemlog.py
can be used as examples. The DSL can also be used to specify a completely new set of rules for a different
type of log file.

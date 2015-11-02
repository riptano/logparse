# Cassandra system.log parser

This rule-based log parser uses regular expressions to match various messages logged 
by Cassandra and extract any useful information they contain into separate fields.  
Additional transformations can be applied to each of the captured values, and then
a dictionary containing the resulting values on each line is returned.  The dictionaries
can be output in json format or inserted into a storage backend.

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

## Rule-Based Message Parser
To reduce the tedium of defining parsers for many different messages, I created a simple 
DSL using Python function objects.  The function objects can be called like normal 
functions, but they are created by a constructor which allows you to define the specific
behavior of the resulting function when it is called.  

The function objects themselves are defined in `rules.py`, and the rules specific to the
Cassandra system.log are defined in `systemlog.py`.  In the future, I may add additional
sets of rules for Spark executor logs, and OpsCenter daemon and agent logs. These rules 
can be used to create parsers for your own application logs as well. 

A minimal set of two rules is defined as follows:

```
capture_message = switch((
    case('CassandraDaemon'),
        rule(
            capture(r'Heap size: (?P<heap_used>[0-9]*)/(?P<total_heap>[0-9]*)'),
            convert(int, 'heap_used', 'total_heap'),
            update(event_product='cassandra', event_category='startup', event_type='heap_size')),
            
        rule(
            capture(r'Classpath: (?P<classpath>.*)'),
            convert(split(':'), 'classpath'),
            update(event_product='cassandra', event_category='startup', event_type='classpath'))))
```

The `switch(cases)` constructor takes a tuple of cases and rules. It was necessary to use
an actual tuple instead of argument unpacking because the number of rules exceeds the 
maximum number of parameters supported by a Python function call. The constructor returns
a function that we assign the name `capture_message`.  This function accepts two parameters:
the first determines which group of rules will be applied, and the second is the string
that the selected group of rules will be applied to until a match is found. The function
returns the value returned by the first matching rule. If no rules match, None is returned.  

Rules are grouped using the `case(*keys)` constructor. The keys specified in the case
constructor will be used by the switch function to determine which group of rules to
execute.  The keys in a case constructor apply to all of the rules that follow until the
next case constructor is encountered.

Rules are defined using `rule(source, *transformations)` where source is a function that
is expected to take a string and return a dictionary of fields extracted from the string 
if it matches, or None if it doesn't. Unless None is returned, the rule will then pass 
the resulting dictionary into the transformation functions in the specified order, each
of which is expected to manipulate the dictionary in some way by adding, removing, or 
overwriting fields. 

Currently the only source defined is `capture(*regexes)`, which takes a list of regular expressions
to apply against the input string.  Each of the regular expressions will be applied
until the first match is found, and then the match's groupdict will be returned. If no 
matches are found, None is returned.

Several transformations are provided:

- `convert(function, *field_names)` will iterate over the k/v pairs in a dictionary and 
apply the specified function to convert the specified fields to a different type or 
perform some some other transformation on the string value.  The function can be a simple 
type conversion such as int or float, or it can be a user-defined function or the 
constructor for a function object.  The field names are just one or more strings 
specifying the dictionary field that the conversion should be applied to. The convert 
function will iterate over the fields specified and apply the conversion function to 
each, replacing the value of the field with the result.

- `update(**fields)` simply adds the specified key-value pairs to the dictionary. This can
be used to tag the event with a category or type based on the regular expression that has
matched it.

- `default(**fields)` is the same as update, but it will only set the key/value pairs for
fields that do not already exist within the dictionary.

`systemlog.py` defines a capture_line rule to match the overall log line of the format:

```
level [thread] date sourcefile:sourceline - message
```

This rule then passes the sourcefile and message fields to the capture_message function 
defined above, which chooses a group of rules based on the sourcefile, then applies them 
to the message until a match is found.

These rules are wrapped by a `parse_log` generator that iterates over a sequence of log lines
and yields a dictionary for each event within the log. This has special handling for
exceptions which can follow on separate lines after the main line of an error message.

In order to test the rules, I created a simple front-end called `log_to_json`, which reads
one or more system.log files (or stdin) and converts each event into a json representation
with one event per line. 

## Cassandra Storage Backend

The Cassandra storage backend is designed to store the data generated by the log parser in a 
flexible schema. Any provided key/value pairs that match the name of a field in the table
will be inserted into the corresponding field. Any pairs that do not match a field in the table
will instead be inserted into a set of generic map fields based on the type of the value. 
The table has a map for each common data type, including boolean (b_), date (d_), integer (i_),
float (f_), string (s_), and list (l_).  

The required fields on the table are shown below, and additional fields can be added as desired.

```
create table generic (
    id timeuuid primary key,
    b_ map<text, boolean>,
    d_ map<text, timestamp>,
    i_ map<text, bigint>,
    f_ map<text, double>,
    s_ map<text, text>,
    l_ map<text, text>
);
```

The `genericize` function in `cassandra_store.py` handles the transformation of arbitrary
dictionaries into the format of the parameters expected by the Cassandra Python Driver.  
Prior to insertion, any nested dictionaries are flattened by combining the key paths using 
underscores. For example `{'a': {'b': 'c'}, 'd': {'e': 'f', 'g': {'h': 'i'}}}` becomes 
`{'a_b': 'c', 'd_e': 'f', 'd_g_h': 'i'}`. Since lists can't be nested within a map in 
Cassandra, lists are actually expressed as a string where each element of the list 
separated by a newline. Anything else will be coerced to JSON and inserted into the string map.
Any fields that are present in the table but not provided in the dictionary will be set to None.

The `CassandraStore` class connects to the cassandra cluster using the DataStax Python Driver
and handles automatic preparation and caching of insert statements.  It provides an `insert` method 
to insert a single record into a specified table, either synchronously or asynchronously. 
To maximize throughput without overloading the cluster, it provides a `slurp` method that will
concurrently insert records provided by a generator while maintaining an optimal number of inflight
queries.

## Solr indexing

The Cassandra table containing parsed log entries can be indexed using the Solr implementation from 
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

4. Edit resources/tomcat/conf/server.xml and add the following inside the `<Host>` tags:

   ```
   <Context docBase="../../banana/src" path="/banana" />
   ```
   
5. If you've previously started DSE, remove `resources/tomcat/work` and restart.

6. Start DSE and go to http://localhost:8983/banana

# Cassandra system.log parser

This is a rule-based Cassandra `system.log` parser.  It 

## Using the parser

The parser is encapsulated in the SystemLog class within the `cassandra.py` module. To
use, pass a list of filenames to the SystemLog constructor:

```
import cassandra
log = cassandra.SystemLog('system.log')
```

If no filenames are specified, SystemLog will default to using the filenames specified
on the command line, and if none are specified, it will attempt to parse stdin.

Once the parser finishes parsing the log file, the log object will contain a few
dictionaries.  `lines` is a list of dictionaries containing all of the log lines, broken 
out into fields.

`sessions` is a list of dictionaries separating the log into individual sessions (restarts).
Each session contains various keys containing various categories of information:

- 'environment': environmental information such as jvm, heap size, and classpath
- 'version': versions of all components included in DSE/Cassandra
- 'garbage_collections': garbage collection pauses
- 'heap_warnings': heap full warnings
- 'flushes': memtable flushes with various statistics
- 'compactions': compactions with various statistics

## Examples

The `example.ipynb` file contains an IPython notebook containing several examples of
analysis that can be done on a log file once it has been parsed.  This includes graphing
and histogramming garbage collections, compactions, and flushes, querying version and
environmental information.

To run the examples, you need IPython, pandas, and associated dependencies.  Please
refer to [Diving into Open Data with IPython Notebook & Pandas](http://nbviewer.ipython.org/github/jvns/talks/blob/master/pyconca2013/pistes-cyclables.ipynb)
for installation instructions and a basic overview of IPython and pandas.

## Defining new rules

Rules can be added to the SystemLog class by defining action method with a `@group` and
`@regex` decorator.  Here are a few example rules:

```
@group(line_rules)
@regex(r'(?P<level>[A-Z]{4,5}) \[(?P<thread>[^\]]*)\] (?P<date>.{10} .{12}) (?P<source_file>[^ ]*) \(line (?P<source_lineno>[0-9]*)\) (?P<message>.*)')
def message_line(self, line_fields, extra_fields):
    'Parse main message line'
    line_fields['level'] = line_fields['level'].strip()
    line_fields['date'] = datetime.datetime.strptime(line_fields['date'], '%Y-%m-%d %H:%M:%S,%f')
    if extra_fields is not None:
        line_fields.update(extra_fields)
    for rule in self.message_rules[line_fields['source_file'][:-5]]:
        if rule(self, line_fields['message'], line_fields):
            break
    else:
        self.unknown_messages.append(line_fields)
    if line_fields['level'] == 'ERROR':
        self.append_session('errors', line_fields)
    elif line_fields['level'] == 'WARN':
        self.append_session('warnings', line_fields)
    self.lines.append(line_fields)

@group(message_rules['GCInspector'])
@regex(r'GC for (?P<type>[A-Za-z]*): (?P<duration>[0-9]*) ms for (?P<collections>[0-9]*) collections, (?P<used>[0-9]*) used; max is (?P<max>[0-9]*)')
def garbage_collection(self, message_fields, line_fields):
    message_fields['date'] = line_fields['date']
    convert(message_fields, ('duration', 'collections', 'used', 'max'), int)
    self.append_session('garbage_collections', message_fields)
```

The `@group` decorator specifies the group in which the rule should be registered.

- Rules that parse an entire line should be registered in the `line_rules` list.
- Rules that parse specific messages should be registered in the appropriate 
`message_rules['ClassName']` list.  ClassName should be the name of the class that logs
a particular message.

The `@regex` decorator takes a regular expression string containing named capture groups.
The regular expression will be evaluated against each line or message, and if it matches,
the action method will be called with a dictionary containing the captured fields.

Please refer to the [Regular Expression HOWTO](http://docs.python.org/2/howto/regex.html)
for more information on regular expressions.

In the action method, you should do any desired field manipulation such as type 
conversions, then save the data to the appropriate location within the SystemLog class.
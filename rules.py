'''
A set of functions defining a domain-specific language that specifies a set of rules for
parsing the lines in a log file.
'''

import re
from datetime import datetime
from collections import defaultdict

def switch(children):
    '''
    Tries multiple rules in the specified order until one returns a value other than None.
    Returns the result of the first successful rule.  Can be configured to run only a 
    subset of the rules using an optional case value.
    
    Constructor expects to be passed one or more case and rule objects. The case objects are
    used to group the rules.
    '''
    rules = defaultdict(list)
    keys = None
    for child in children:
        if isinstance(child, case):
            keys = child.keys
        else:
            for key in keys:
                rules[key].append(child)
    def inner_switch(key, data):
        if key in rules:
            for rule in rules[key]:
                result = rule(data)
                if result is not None:
                    return result
        return None
    return inner_switch

class case:
    '''
    Specifies an alternative for a switch rule.

    Constructor expects to be passed one or more strings. At least one of the strings in
    the case must match the case value passed to the switch for the case to be selected.
    '''
    def __init__(self, *keys):
        self.keys = keys

def rule(source, *transforms):
    '''
    Executes the condition, and optionally one or more actions. If the condition returns None,
    the rule returns None immediately.  If the condition returns something other than None, the
    rule executes each action in order, passing the result of the condition into each.
    
    Constructor expects the first parameter to be a function that takes a string as input. The
    condition should return None to indicate failure or something else to indicate success.
    The remaining parameters should be functions that act on the result of the condition.  
    '''
    def inner_rule(string):
        fields = source(string)
        if fields is not None:
            for transform in transforms:
                transform(fields)
        return fields
    return inner_rule

def capture(*regex_strings):
    '''
    Matches the input string against one or more regular expressions and returns a dictionary of
    values captured by regex's named capture groups. Returns None if none of the regular expressions
    match the input string. Returns an empty dict if the regular expression doesn't contains any
    named capture groups.

    Constructor expects a list of one or more regular expression strings.
    '''
    regexes = []
    for regex in regex_strings:
        regexes.append(re.compile(regex))
    def inner_capture(string):
        for regex in regexes:
            capture =  regex.match(string)
            if capture:
                return capture.groupdict()
        return None
    return inner_capture

def convert(func, *field_names):
    '''
    Applies the specified conversion function against each of the named fields in the input dictionary.
    The value of each field is passed to the conversion function and is replaced by the value returned
    by the conversion function.

    Constructor expects a conversion function followed by fields specifying one or more field names.
    The conversion function should take a string as input and return a converted value.
    '''
    def inner_convert(fields):
        for field_name in field_names:
            if field_name in fields and fields[field_name] is not None:
                fields[field_name] = func(fields[field_name])
    return inner_convert

def update(**extras):
    '''
    Updates the specified fields in the input dictionary with the specified values.

    Constructor expects a set of named parameters specifying key value pairs to be set in the input
    dictionary.
    '''
    return lambda fields: fields.update(extras)

def default(**defaults):
    '''
    Updates the specified fields in the input dictionary with the specified values only if the field
    does not already exist.

    Constructor expects a set of named parameters specifying key value pairs to be set in the input
    dictionary.
    '''
    def inner_default(fields):
        for key, value in defaults.iteritems():
            if key not in fields:
                fields[key] = value
    return inner_default

def strip(string):
    '''
    Strips the leading and trailing whitespace from the supplied string and returns the result.
    '''
    return string.strip()

def date(format):
    '''
    Parses the supplied date and returns the resulting datetime value.

    Constructor expects a date string supported by datetime.strptime.
    '''
    return lambda date: datetime.strptime(date, format)

def split(delimiter):
    '''
    Splits the supplied string and returns the resulting list.

    Constructor expects a string to use as the delimiter.
    '''
    return lambda string: string.split(delimiter)

def percent(value):
    '''
    Converts the supplied string to a floating point and multiplies it by 100.
    '''
    return float(value) * 100

def int_with_commas(value):
    '''
    Removes any commas from the input string and converts the result to an int.
    '''
    return int(value.replace(',', ''))

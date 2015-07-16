'''
A set of functions defining a domain-specific language that specifies a set of rules for
parsing the lines in a log file.
'''

import re
from datetime import datetime

def switch(*cases):
    '''
    Selects between one or more alternatives based on the case value and applies the selected 
    alternative to the data value, returning the results from the expected case. Returns None
    if none of the cases are selected.
    
    Constructor expects to be passed one or more case objects.
    '''
    casedict = {}
    for case in cases:
        for condition in case.conditions:
            if condition not in casedict:
                casedict[condition] = case.action
            else:
                casedict[condition] = first(casedict[condition], case.action)

    def inner_switch(case, data):
        if case in casedict:
            return casedict[case](data)
        return None
    return inner_switch

class case:
    '''
    Specifies an alternative for a switch rule.  
    
    Constructor expects to be passed one or more strings, followed by an action rule that takes 
    a string as input and returns None or a dictionary of values. At least one of the strings in
    the case must match the case value passed to the switch for the case to be selected.
    '''
    def __init__(self, *params):
        self.conditions = params[:-1]
        self.action = params[-1]

def first(*rules):
    '''
    Applies a list of rules to the input value and stops after the first successful rule, returning
    its value. Returns None if none of the rules are successful.
    
    Constructor expects a list of one or more rules to apply as parameters.  A successful rule
    is expected to return a dictionary of values. An unsuccessful rule is expected to return None.
    '''
    def inner_first(string):
        for rule in rules:
            fields = rule(string)
            if fields is not None:
                return fields
        return None
    return inner_first

def pipeline(source, *transforms):
    '''
    Forms a pipeline of one source and one or more transformations.  Returns the dictionary created
    by the source rule after all the specified transformations have been applied. No transformations
    will be executed if the source rule returns None.
    
    Constructor expects the first parameter to be a rule that takes a string as input. If the source
    rule successfully matches the input, it should returns a dictionary of values extracted from the
    input string.  If it does not match, it should return None.  The remaining parameters specify 
    transformation rules that modify the dictionary in place.  
    '''
    def inner_pipeline(string):
        fields = source(string)
        if fields is not None:
            for transform in transforms:
                transform(fields)
        return fields
    return inner_pipeline

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

'''
A set of functions defining a domain-specific language that specifies a set of rules for
parsing the lines in a log file.
'''

import re
from datetime import datetime
from collections import defaultdict

class switch:
    '''
    Tries multiple rules in the specified order until one returns a value other than None.
    Returns the result of the first successful rule.  Can be configured to run only a 
    subset of the rules using an optional case value.
    '''
    
    def __init__(self, children):
        '''
        Constructor expects to be passed one or more case and rule objects. The case objects are
        used to group the rules.
        '''
        
        self.rules = defaultdict(list)
        keys = None
        for child in children:
            if isinstance(child, case):
                keys = child.keys
            else:
                for key in keys:
                    self.rules[key].append(child)

    def __call__(self, key, data):
        if key in self.rules:
            for rule in self.rules[key]:
                result = rule(data)
                if result is not None:
                    return result
        return None

class case:
    "Specifies an alternative for a switch rule."

    def __init__(self, *keys):
        '''
        Constructor expects to be passed one or more strings. At least one of the strings in
        the case must match the case value passed to the switch for the case to be selected.
        '''
        self.keys = keys

class rule:
    '''
    Executes the condition, and optionally one or more actions. If the condition returns None,
    the rule returns None immediately.  If the condition returns something other than None, the
    rule executes each action in order, passing the result of the condition into each.
    '''


    def __init__(self, source, *transforms):
        ''' 
        Constructor expects the first parameter to be a function that takes a string as input. The
        condition should return None to indicate failure or something else to indicate success.
        The remaining parameters should be functions that act on the result of the condition.  
        '''
        self.source = source
        self.transforms = transforms

    def __call__(self, string):
        fields = self.source(string)
        if fields is not None:
            for transform in self.transforms:
                transform(fields)
        return fields

class capture:
    '''
    Matches the input string against one or more regular expressions and returns a dictionary of
    values captured by regex's named capture groups. Returns None if none of the regular expressions
    match the input string. Returns an empty dict if the regular expression doesn't contains any
    named capture groups.
    '''

    def __init__(self, *regex_strings):
        "Constructor expects a list of one or more regular expression strings."
        self.regexes = []
        for regex in regex_strings:
            self.regexes.append(re.compile(regex))

    def __call__(self, string):
        for regex in self.regexes:
            capture =  regex.match(string)
            if capture:
                return capture.groupdict()
        return None

class convert:
    '''
    Applies the specified conversion function against each of the named fields in the input dictionary.
    The value of each field is passed to the conversion function and is replaced by the value returned
    by the conversion function.
    '''

    def __init__(self, func, *field_names):
        '''
        Constructor expects a conversion function followed by fields specifying one or more field names.
        The conversion function should take a string as input and return a converted value.
        '''
        self.func = func
        self.field_names = field_names

    def __call__(self, fields):
        for field_name in self.field_names:
            if field_name in fields and fields[field_name] is not None:
                fields[field_name] = self.func(fields[field_name])

class update:
    "Updates the specified fields in the input dictionary with the specified values."

    def __init__(self, **extras):
        '''
        Constructor expects a set of named parameters specifying key value pairs to be set in the input
        dictionary.
        '''
        self.extras = extras

    def __call__(self, fields):
        fields.update(self.extras)

class default:
    '''
    Updates the specified fields in the input dictionary with the specified values only if the field
    does not already exist.
    '''

    def __init__(self, **defaults):
        '''
        Constructor expects a set of named parameters specifying key value pairs to be set in the input
        dictionary.
        '''
        self.defaults = defaults

    def __call__(self, fields):
        for key, value in self.defaults.iteritems():
            if key not in fields:
                fields[key] = value

def strip(string):
    "Strips the leading and trailing whitespace from the supplied string and returns the result."
    return string.strip()

class date:
    "Parses the supplied date and returns the resulting datetime value."

    def __init__(self, format):
        "Constructor expects a date string supported by datetime.strptime."
        self.format = format

    def __call__(self, date):
        return datetime.strptime(date, self.format)

class split:
    "Splits the supplied string and returns the resulting list."

    def __init__(self, delimiter):
        "Constructor expects a string to use as the delimiter."
        self.delimiter = delimiter

    def __call__(self, string):
        return string.split(self.delimiter)

def percent(value):
    "Converts the supplied string to a floating point and multiplies it by 100."
    return float(value) * 100

def int_with_commas(value):
    "Removes any commas from the input string and converts the result to an int."
    return int(value.replace(',', ''))

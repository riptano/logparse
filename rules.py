import re
from datetime import datetime

def switch(*cases):
    casedict = {}
    for case in cases:
        for condition in case.conditions:
            casedict[condition] = case.action
    def inner_switch(case, data):
        if case in casedict:
            return casedict[case](data)
        return None
    return inner_switch

class case:
    def __init__(self, *params):
        self.conditions = params[:-1]
        self.action = params[-1]

def first(*rules):
    def inner_first(string):
        for rule in rules:
            fields = rule(string)
            if fields is not None:
                return fields
        return None
    return inner_first

def pipeline(source, *transforms):
    def inner_pipeline(string):
        fields = source(string)
        if fields is not None:
            for transform in transforms:
                transform(fields)
        return fields
    return inner_pipeline

def capture(*regex_strings):
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
    def inner_convert(fields):
        for field_name in field_names:
            if field_name in fields and fields[field_name] is not None:
                fields[field_name] = func(fields[field_name])
    return inner_convert

def update(**extras):
    return lambda fields: fields.update(extras)

def default(**defaults):
    def inner_default(fields):
        for key, value in defaults.iteritems():
            if key not in fields:
                fields[key] = value
    return inner_default

def field(key):
    return lambda fields: fields[key]

def strip(string):
    return string.strip()

def date(format):
    return lambda date: datetime.strptime(date, format)

def split(delimiter):
    return lambda string: string.split(delimiter)

def percent(value):
    return float(value) * 100

def int_with_commas(value):
    return int(value.replace(',', ''))

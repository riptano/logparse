import re
import inspect
from collections import defaultdict

class Rule(object):
	rule_group = None
	rule_groups = ()
	regex = None
	
	def __init__(self, ruleset, target):
		self.regex = re.compile(self.regex)
		self.ruleset = ruleset
		self.target = target
		if self.rule_groups == () and self.rule_group is not None:
			self.rule_groups = (self.rule_group,)
			
	def action(self, fields):
		pass
		
	def parse(self, name, value):
		return getattr(self, 'parse_' + name, lambda x: x)(value)
		
	def apply(self, input, fields=None):
		match = self.regex.match(input)
		if match is not None:
			if fields is None:
				fields = {}
			fields.update({k: self.parse(k, v) for k, v in match.groupdict().iteritems()})
			self.action(fields)
			return True
		else:
			return False


class RuleSet(object):
	def __init__(self, target):
		self.target = target
		self.all_rules = []
		self.rules_by_group = defaultdict(list)
		for child_name in dir(self):
			child = getattr(self, child_name)
			if inspect.isclass(child) and issubclass(child, Rule):
				rule = child(self, target)
				self.all_rules.append(rule)
				for rule_group in rule.rule_groups:
					self.rules_by_group[rule_group].append(rule)
		
	def apply(self, input, group=None, fields=None):
		for rule in self.all_rules if group is None else self.rules_by_group[group]:
			if rule.apply(input, fields):
				return True
		return False
import yaml


class Config(object):
	"""docstring for Config"""
	def __init__(self, namespace="bjec"):
		super(Config, self).__init__()
		self.namespace = namespace
		self.config_dict = dict()
		self.User = ModuleConfig(self, ["User"])

	def __getitem__(self, key):
		if not isinstance(key, str):
			if isinstance(key, type):
				key = key.__module__ + "." + key.__name__
			else:
				key = key.__module__ + "." + key.__class__.__name__

		key_parts = key.split(".")

		if key_parts[0] == self.namespace:
			key_parts = key_parts[1:]

		return ModuleConfig(self, key_parts)

	def read_yaml(self, path):
		with open(path) as f:
			config = yaml.load(f)

		self.config_dict.update(config)


class ModuleConfig(object):
	"""docstring for ModuleConfig"""
	def __init__(self, config, key_parts):
		super(ModuleConfig, self).__init__()
		self.config = config
		self.key_parts = key_parts

	def __getitem__(self, key):
		config_elm = self.config.config_dict

		try:
			for key_part in self.key_parts + [key]:
				config_elm = config_elm[key_part]
		except KeyError:
			raise KeyError(
				"'{}' in '{}'".format(key, ".".join(self.key_parts))
			)

		return config_elm

	def get(self, key, default=None):
		try:
			return self[key]
		except KeyError:
			return default


config = Config()

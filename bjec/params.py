import collections


class ParamsEvaluable(object):
	def evaluate(self, params):
		raise NotImplemented


def evaluate(obj, params):
	try:
		return obj.evaluate(params)
	except (AttributeError, TypeError) as e:
		pass

	if not isinstance(obj, str) and isinstance(obj, collections.Iterable):
		return (evaluate(i, params) for i in obj)

	return obj


class P(ParamsEvaluable):
	"""Wrapper to allow intuitive parameter inclusion.

	P instances represent a 'future' parameter value, every instance contains
	the `key` of the parameter in the `params` dict.
	Each instance evaluates to the corresponding parameter's value.

	Other modules may accept `P` objects or lists containing `P` objects.
	These are then evaluated for every parameter set.

	Example:
		::

			ProcessArgs("--offset", P("offset"))

	Args:
		key (str): Parameter (key of the parameter in the `params`) dict.
		f (None or function, optional): If not None, `f` is applied to the
			value of ``params[key]`` before returning.
	"""

	def __init__(self, key, f=None):
		super(P, self).__init__()
		self.key = key
		self.f = f

	def evaluate(self, params):
		if self.f is None:
			return params[self.key]
		else:
			return self.f(params[self.key])

	__call__ = evaluate

	@classmethod
	def evaluate_list(cls, l, params):
		r = list()
		for el in l:
			if isinstance(el, cls):
				r.append(
					el.evaluate(params)
				)
			else:
				r.append(el)
		return r


class Join(ParamsEvaluable):
	"""String / Byte Join for lists containing P objects.

	Args:
		*args (object or P, must support str(.)): Elements to join, may be
			instances of P. ``str()`` is applied to every element before
			joining.
		sep (str, optional): Separator used to join elements of `*args`.
			Defaults to ``""``.
	"""
	def __init__(self, *args, sep=""):
		self.args = args
		self.sep = sep

	def evaluate(self, params):
		if isinstance(self.sep, str):
			return self.sep.join(map(str, P.evaluate_list(self.args, params)))
		else:
			return self.sep.join(P.evaluate_list(self.args, params))

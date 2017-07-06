import collections


class ParamsEvaluable(object):
	def evaluate(self, params):
		raise NotImplemented

	def __call__(self, params):
		return self.evaluate(params)


def evaluate(obj, params):
	try:
		return obj.evaluate(params)
	except (AttributeError, TypeError) as e:
		pass

	if (not isinstance(obj, (str, dict)) and
		isinstance(obj, collections.Iterable)
	):
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
			value of ``params[key]`` and the result is returned.
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
	"""String / Bytes Join for lists containing P objects.

	The type of output is determined by the type of the `sep` argument.

	If the output should be a ``str``, ``str(.)`` will be called on each list
	element (in `*args`). If the output should be of type ``bytes``, the user
	has to ensure that each of the list elements are of ``bytes`` type and that
	``P(.)`` returns a ``bytes`` object.

	Example:
		::

			Join("out.", P("n"), ".csv")

	Args:
		*args (object supporting str(.), P or bytes): Elements to join, may
			be instances of P. If the output type is ``str``, ``str()`` is
			applied to every element before joining.
		sep (str or bytes, optional): Separator used to join elements of
			`*args`. Must have the type of the output, i.e. if the output should
			be of a ``bytes`` type, `sep` must be as well. Defaults to ``""``.
	"""
	def __init__(self, *args, sep=""):
		self.args = args
		self.sep = sep

	def evaluate(self, params):
		if isinstance(self.sep, str):
			return self.sep.join(map(str, P.evaluate_list(self.args, params)))
		else:
			return self.sep.join(P.evaluate_list(self.args, params))


class Factory(ParamsEvaluable):
	"""Factory for objects with ParamsEvaluable arguments.

	Example:
		::

			Factory(Concatenate, file_path=Join("out.", P("n"), ".data"))

	Args:
		cls (class object):
		*args (arbitrary, ParamsEvaluable): Variable arguments passed to the
			class constructor. May contain ParamsEvaluable elements.
		**kwargs (arbitrary, ParamsEvaluable): Keyword arguments passed to the
			class constructor. May contain ParamsEvaluable values.
	"""
	def __init__(self, cls, *args, **kwargs):
		self.cls = cls
		self.args = args
		self.kwargs = kwargs

	def evaluate(self, params):
		args = evaluate(self.args, params)
		kwargs = {
			key: evaluate(value, params) for key, value in self.kwargs.items()
		}
		return self.cls(*args, **kwargs)


class Function(ParamsEvaluable):
	"""Wrapper for functions.

	Example:
		::

			Function(lambda p: p["alpha"] / p["beta"])

	Args:
		func (function): Function to be called on evaluation. The parameters
			are passed as the only argument.
	"""
	def __init__(self, func):
		self.func = func

	def evaluate(self, params):
		return self.func(params)

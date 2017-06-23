import itertools


class Generator(object):
	"""Generator represents a generator for input parameters of tasks.

	Every parameter set produced by the generator represents the input for a
	a task.

	The ``Generator`` interface basically is a standard python iterable, i.e.
	the ``__iter__`` method has to be defined and return an iterator.
	"""

	def __iter__(self):
		"""Returns an iterator over the produced parameter sets.

		**Must** be implemented by inheriting classes.
		"""
		raise NotImplementedError


class Product(Generator):
	"""docstring for Product"""
	def __init__(self, **kwargs):
		super(Product, self).__init__()
		self.params = kwargs

	def __iter__(self):
		return iter(self._iterator())

	def _iterator(self):
		for c in itertools.product(*self.params.values()):
			yield dict(zip(self.params.keys(), c))


class RepeatN(Generator):
	def __init__(self, params, n):
		super(RepeatN, self).__init__()
		self.params = params
		self.n = n

	def __iter__(self):
		return iter(itertools.repeat(self.params, self.n))


class Chain(Generator):
	def __init__(self, *generators):
		self.generators = generators

	def __iter__(self):
		return iter(itertools.chain(*self.generators))

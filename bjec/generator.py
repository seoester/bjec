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


class Repeat(Generator):
	def __init__(self, params, n):
		super(Repeat, self).__init__()
		self.params = params
		self.n = n

	def __iter__(self):
		return iter(itertools.repeat(self.params, self.n))


class RepeatG(Generator):
	def __init__(self, generator, n):
		super(RepeatG, self).__init__()
		self.generator = generator
		self.n = n

	def __iter__(self):
		"""

		Roughly equivalent to::

			for params in self.generator:
				for _ in range(self.n):
					yield params

		"""
		return itertools.chain.from_iterable(
			(itertools.repeat(params, self.n) for params in self.generator)
		)


class Chain(Generator):
	def __init__(self, *generators):
		self.generators = generators

	def __iter__(self):
		return iter(itertools.chain(*self.generators))

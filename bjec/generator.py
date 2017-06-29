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


class List(Generator):
	def __init__(self, iterable):
		super(List, self).__init__()
		self.iterable = iterable

	def __iter__(self):
		return iter(self.iterable)


class Product(Generator):
	"""docstring for Product"""
	def __init__(self, **params):
		super(Product, self).__init__()
		self.params = params

	def __iter__(self):
		for c in itertools.product(*self.params.values()):
			yield dict(zip(self.params.keys(), c))


class Repeat(Generator):
	def __init__(self, params, n):
		super(Repeat, self).__init__()
		self.params = params
		self.n = n

	def __iter__(self):
		return itertools.repeat(self.params, self.n)


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
		return itertools.chain(*self.generators)


class Combine(Generator):
	def __init__(self, *generators):
		super(Combine, self).__init__()
		self.generators = generators

	def __iter__(self):
		return (
			dict(itertools.chain.from_iterable(
				params.items() for params in paramsSets
			))
			for paramsSets in itertools.product(*self.generators)
		)

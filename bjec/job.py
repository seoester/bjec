import functools

from .master import Runnable, Constructible, Artefactor, Dependency, WrapperRun
from .master import master as global_master
from .utils import listify


def job(depends=None, master=None):
	def decorator(f):
		j = Job(f, depends=depends)

		nonlocal master

		if master is None:
			master = global_master

		@functools.wraps(f)
		def wrapper():
			j.run()

		master.register(j, wrapper)

		return wrapper

	return decorator


class Job(Dependency, Constructible, Artefactor, WrapperRun, Runnable):
	class Constructor(Dependency.ResolveConstructor, Artefactor.Constructor,
			Constructible.Constructor):
		def generator(self, generator):
			self._obj._generator = generator
			return generator

		def processor(self, processor):
			self._obj._processor = processor
			return processor

		def runner(self, runner):
			self._obj._runner = runner
			return runner

		def collector(self, collector):
			self._obj._collector = collector
			return collector

		def after(self, *after_func):
			self._obj._after_funcs.extend(after_func)
			return after_func

	def __init__(self, constructor_func, depends=None):
		super(Job, self).__init__()
		self._generator = None
		self._processor = None
		self._runner = None
		self._collector = None
		self._after_funcs = list()

		self.constructor_func(constructor_func)

		self.depends(*listify(depends, none_empty=True))

	def run(self):
		super(Job, self).run()

		for after_func in self._after_funcs:
			after_func(self)

	def _run(self):
		processor = self._processor

		processor.generator(self._generator)
		processor.runner_factory(self._runner)
		processor.collector(self._collector)

		processor.process()

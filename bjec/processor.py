import threading
import queue

from .config import config


class Processor(object):
	"""docstring for Processor

	A ``Processor`` is responsible for the task execution pipeline, that is
	fetching parameter sets from a ``Generator``, handing them to a ``Runner``
	and passing the ``Runner``'s output to a ``Collector``.
	Meanwhile the ``Processor`` has to manage its ``Runners'`` lifecycle.
	"""
	def __init__(self):
		super(Processor, self).__init__()
		self._generator = None
		self._runner_factory = None
		self._collector = None

	def generator(self, generator):
		self._generator = generator

	def runner_factory(self, runner_factory):
		self._runner_factory = runner_factory

	def collector(self, collector):
		self._collector = collector

	def process(self):
		"""Process all parameter sets produced by the generator.

		**Must** be implemented by inheriting classes.
		"""
		raise NotImplementedError


class Inline(Processor):
	def process(self):
		runner = self._runner_factory()
		runner.start()

		for params in self._generator:
			output = runner.run(params)
			self._collector.add(params, output)

		runner.stop()


class Threading(Processor):
	"""docstring for Threading

	Args:
		n (int): Number of threads to be run. If ``<= 0``, the configuration
			option of the same name is used instead.

	Configuration Options:
		* ``n``: Number of threads to run, it is used when `n` passed to the
			constructor is ``<= 0``. Defaults to 1.

	"""

	def __init__(self, n):
		super(Threading, self).__init__()
		if n <= 0:
			self.n = config[Threading].get("n", 1)
			if self.n <= 0:
				raise ValueError("Invalid value for n retrieved (<= 0)")
		else:
			self.n = n

		self._queue = queue.Queue(maxsize=n)

		self._started = threading.Event()
		self._exhausted = threading.Event()

	def process(self):
		threads = list()

		for _ in range(self.n):
			thread = threading.Thread(target=self._thread_worker)
			threads.append(thread)
			thread.start()

		self._started.set()

		for params in self._generator:
			self._queue.put(params)

		self._exhausted.set()

		for thread in threads:
			thread.join()

	def _thread_worker(self):
		runner = self._runner_factory()
		runner.start()

		self._started.wait()

		while True:
			try:
				params = self._queue.get(block=False)
			except queue.Empty:
				if self._exhausted.is_set():
					break
				else:
					continue

			output = runner.run(params)

			self._collector.add(params, output)

			self._queue.task_done()

		runner.stop()

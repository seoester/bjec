import subprocess
import shlex
import tempfile

from .params import evaluate


try:
	subprocess.run
except AttributeError:
	from .subprocess_run import run
	subprocess.run = run


class Runner(object):
	"""docstring for Runner"""
	def start(self):
		"""start is called before the Runner is used for the first time.

		**May** be implemented by inheriting classes (but must be defined).

		If starting is an asynchronous process, start() must return after this
		process completed.
		"""
		pass

	def run(self, params):
		"""run is called to have the Runner execute a task.

		**Must** be implemented by inheriting classes.

		The parameter params consists of the task's parameters, its type will
		depend on the Runner's configuration.
		run() must return only after processing completed.
		Its return type will be depend on the Runner's configuration.
		"""
		raise NotImplementedError

	def stop(self):
		"""stop is called when the Runner is no longer needed.

		**May** be implemented by inheriting classes (but must be defined).

		If stopping is an asynchronous process, stop() must return after this
		process completed.
		"""
		pass

	@classmethod
	def factory(cls, *args, **kwargs):
		"""Creates a factory for properly set-up ``Runner`` objects.

		Here, a factory is a function taking no parameters and returning a new
		instance of a ``Runner`` (subclass).

		**May** be implemented by inheriting classes.
		The default implementation will create a new object of the current
		class with the exact same parameters as passed into the ``factory``
		method.

		Returns:
			function: Calling this function will return a new ``Runner``
			instance with the parameters passed into the ``factory`` method.
		"""
		def factory():
			return cls(*args, **kwargs)

		return factory


class SubprocessRunner(Runner):
	"""docstring for SubprocessRunner"""
	class Wrapper(object):
		"""Wrapper is a helper class used by both input and output methods.

		Wrapper is used as a context manager, the ``subprocess.run()`` is wrapped
		in it. Input and output methods can therefore use its ``__enter__``
		methods to modify the ``args`` and ``kwargs`` passed to the
		``subprocess.run()`` call.

		For details also check out the ``InputMethod`` and ``OutputMethod``
		classes as well as the concrete implementations of the both.

		Attributes:
			obj (object): Arbitrary object, meant to contain the instance of
				of the Wrapper's method class, thus enabling access to its
				members.
			params (dict): The parameter set serving as the input of the
				current run / task.
			args (list): The args list passed to ``subprocess.run()``. May be
				modified.
			kwargs (dict): The kwargs list passed to ``subprocess.run()``. May
				be modified.

		"""
		def __init__(self, obj, params, args, kwargs):
			super(SubprocessRunner.Wrapper, self).__init__()
			self.obj = obj
			self.params = params
			self.args = args
			self.kwargs = kwargs

		def __enter__(self):
			pass

		def __exit__(self, type, value, traceback):
			pass

	def __init__(self, *args, input=None, output=None, **kwargs):
		super(SubprocessRunner, self).__init__()

		if kwargs.get("shell", False):
			self.args = args[0]
		else:
			self.args = args

		self.input = input or InputMethod()
		self.output = output or OutputMethod()
		self.kwargs = kwargs

	def run(self, params):
		args = list(self.args)
		kwargs = dict(self.kwargs)

		in_w = self.input.wrapper(params, args, kwargs)
		out_w = self.output.wrapper(params, args, kwargs)

		with in_w, out_w:
			subprocess.run(args, **kwargs)

		return out_w.output()


class InputMethod(object):
	class Wrapper(SubprocessRunner.Wrapper):
		pass

	def wrapper(self, params, args, kwargs):
		return self.Wrapper(self, params, args, kwargs)


class ProcessArgs(InputMethod):
	"""docstring for ProcessArgs

	Args:
		*args (str, ParamsEvaluable): Arguments to execute the subprocess with.
			Supports ParamsEvaluable arguments.
	"""

	class Wrapper(InputMethod.Wrapper):
		def __enter__(self):
			l = map(str, evaluate(self.obj.args, self.params))
			if isinstance(self.args, str):
				self.args += " " + " ".join(map(shlex.quote, l))
			else:
				self.args += l

	def __init__(self, *args):
		super(ProcessArgs, self).__init__()
		self.args = args


class OutputMethod(object):
	class Wrapper(SubprocessRunner.Wrapper):
		def output(self):
			"""Returns the output of the subprocess.

			**Must** be implemented by inheriting classes.

			Will be called by SubprocessRunner after the ``subprocess.run()``
			call has finished.
			"""
			pass

	def wrapper(self, params, args, kwargs):
		return self.Wrapper(self, params, args, kwargs)


class Stdout(OutputMethod):
	"""docstring for Stdout

	Parameters:
		spool (int, default 0): If spool is greater 0, the stdout will be
			stored in a spooled file in memory until its size exceeds
			``spool``.
		named (bool, default False): If True, the output file will be located
			on the file system with its path in the its ``name`` attribute.
			Implies ``spool = 0`` if set to True.
		stdout (bool, default True): If True, the stdout of the subprocess will
			be included in the output file.
		stderr (bool, default False): If True, the stderr of the subprocess
			will be included in the output file
	"""
	class Wrapper(OutputMethod.Wrapper):
		def __init__(self, *args, **kwargs):
			super(Stdout.Wrapper, self).__init__(*args, **kwargs)
			if self.obj.named:
				self.file = tempfile.NamedTemporaryFile()
			elif self.obj.spool > 0:
				self.file = tempfile.SpooledTemporaryFile(
					max_size=self.obj.spool,
				)
			else:
				self.file = tempfile.TemporaryFile()

		def __enter__(self):
			if self.obj.stdout:
				if self.obj.stderr:
					self.kwargs["stderr"] = subprocess.STDOUT
				else:
					self.kwargs["stderr"] = subprocess.DEVNULL

				self.kwargs["stdout"] = self.file
			else:
				if self.obj.stderr:
					self.kwargs["stderr"] = self.file
				else:
					self.kwargs["stderr"] = subprocess.DEVNULL

				self.kwargs["stdout"] = subprocess.DEVNULL

		def __exit__(self, type, value, traceback):
			if type is not None:
				self.file.close()
			else:
				self.file.seek(0)

		def output(self):
			return self.file

	def __init__(self, spool=0, named=False, stdout=True, stderr=False):
		super(Stdout, self).__init__()
		self.spool = spool
		self.named = named
		self.stdout = stdout
		self.stderr = stderr

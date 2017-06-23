import threading
import shutil
import tempfile


class Collector(object):
	"""docstring for Collector"""

	def add(self, params, output):
		"""Adds the output of a run to the collector.

		**Must** be implemented by inheriting classes.

		Inheriting classes can specify whether `add()` may be called after
		`aggregate()` has been called.
		Inheriting classes must ensure, that `add()` is thread-safe.

		Parameters:
			params (dict): The parameters o the run.
			output (any): Output of the run. What kind of object is passed in
				will depend on the Runner.
		"""
		raise NotImplementedError

	def aggregate(self):
		"""Aggregates and returns all the outputs collected.

		**Must** be implemented by inheriting classes.

		Inheriting classes can specify whether `aggregate` may be called
		multiple times.
		Inheriting classes may add optional parameters.

		Returns:
			any: Returns the aggregate of all outputs added to the Collector.
		"""
		raise NotImplementedError


class Concatenate(Collector):
	"""Collector concatenating output (file-like objects) into a new file.

	Parameters:
		file_path (str): The file path to opened as the aggregate file. If
			``None`` a temporary file will be created according to
			`tempfile_class`.
		tempfile_class (class object or function): The class used to create a
			temporary file as the aggregate file. Only used when `file_path` is
			set to ``None``. Please note that a function may be passed in, e.g.
			thus enabling use of `functools.partial` to set `max_size` for
			`tempfile.SpooledTemporaryFile`.
		close_files (bool): If set to ``True``, `add()` will attempt to close
			the output argument (by calling `close()` on it), ignoring any
			AttributeError (i.e. `close()` not defined).
		lock_class (class object or function): The class used to create a lock
			object.

	"""
	def __init__(
		self,
		file_path=None,
		tempfile_class=tempfile.TemporaryFile,
		close_files=True,
		lock_class=threading.Lock,
	):
		super(Concatenate, self).__init__()
		self.close_files = close_files
		self.output_lock = lock_class()
		self._aggregated = False

		if file_path is not None:
			self.aggregate_file = open(file_path, 'w+b')
		else:
			self.aggregate_file = tempfile_class()

	def add(self, params, output):
		if self._aggregated:
			raise Exception("Has already been aggregated")

		with self.output_lock:
			shutil.copyfileobj(output, self.aggregate_file)

		if self.close_files:
			try:
				output.close()
			except AttributeError:
				pass

	def aggregate(self):
		"""Returns the file object containing the aggregated output.

		Returns:
			file-like object: The file object containing the aggregated output,
			the position in the file is reset to ``0`` before returning.
			The caller has the responsible to `close()` the returned
			file-like object.

		"""
		self._aggregated = True

		# Seek to beginning of the aggregate file
		self.aggregate_file.seek(0)

		return self.aggregate_file

	def __del__(self):
		if not self._aggregated:
			try:
				self.aggregate_file.close()
			except:
				pass

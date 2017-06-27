import threading
import shutil
import tempfile
import csv
import io
import itertools

from .params import evaluate
from .utils import listify


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
		before_all=None,
		after_all=None,
		before=None,
		after=None,
	):
		super(Concatenate, self).__init__()
		self.close_files = close_files
		self.output_lock = lock_class()
		self._aggregated = False

		self.after_all = after_all
		self.before = before
		self.after = after

		if file_path is not None:
			self.aggregate_file = open(file_path, 'w+b')
		else:
			self.aggregate_file = tempfile_class()

		if self.before_all is not None:
			self.aggregate_file.write(
				self.before_all
			)

	def add(self, params, output):
		if self._aggregated:
			raise Exception("Has already been aggregated")

		with self.output_lock:
			if self.before is not None:
				self.aggregate_file.write(
					self._convert(self.before, params)
				)

			shutil.copyfileobj(output, self.aggregate_file)

			if self.after is not None:
				self.aggregate_file.write(
					self._convert(self.after, params)
				)

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

		if self.after_all is not None:
			self.aggregate_file.write(
				self.after_all
			)

		# Seek to beginning of the aggregate file
		self.aggregate_file.seek(0)

		return self.aggregate_file

	def _convert(self, obj, params):
		return b""

	def __del__(self):
		if not self._aggregated:
			try:
				self.aggregate_file.close()
			except:
				pass


class CSV(Collector):
	"""Collector concatenating CSV output (from file-like objects).

	The Collector expects file-like objects, those are read as CSV files.
	Each row is appended to an output file.

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
		input_encoding (str, optional): Input encoding (for `output` passed into
			`add()`). Defaults to ``"utf-8"``.
		output_encoding (str, optional): Output encoding (for the aggregate
			file). Defaults to ``"utf-8"``.
		input_csv_args (dict, optional): kwargs passed to the `csv.reader()`
			call used to create a reader for CSV input (from `output` passed
			into `add()`).
		output_csv_args (dict, optional): kwargs passed to the `csv.writer()`
			call used to create a writer for CSV output (to the aggregate file).
		before_all (iterable of iterables, optional): Is inserted at the
			beginning of the output file. `before_all` is interpreted as rows,
			each item in one row is written as column content.
		after_all (iterable of iterables, optional): Is appended to the end of
			the output file. `after_all` is interpreted as rows, each item in
			one row is written as column content.
		before (iterable of iterables, optional): Is inserted before each item's
			data, which is  handed to the Collector using `add()`. `before` is
			interpreted as rows, each item in one row is written as column
			content.
		after (iterable of iterables, optional): Is appended to each item's
			data, which is handed to the Collector using `add()`. `after` is
			interpreted as rows, each item in one row is written as column
			content.
		before_row (iterable, optional): Is inserted at the beginning of each
			row.  Each item of `before_row` is written as column content.
		after_row (iterable, optional): Is appended to each row. Each item of
			`after_row` is written as column content.

	"""
	def __init__(
		self,
		file_path=None,
		tempfile_class=tempfile.TemporaryFile,
		close_files=True,
		lock_class=threading.Lock,
		input_encoding="utf-8",
		output_encoding="utf-8",
		input_csv_args=None,
		output_csv_args=None,
		before_all=None,
		after_all=None,
		before_row=None,
		after_row=None,
		before=None,
		after=None,
	):
		super(CSV, self).__init__()
		self.close_files = close_files
		self.output_lock = lock_class()
		self.input_encoding = input_encoding
		self.input_csv_args = input_csv_args or dict()
		self._aggregated = False

		self.after_all = after_all
		self.before_row = before_row
		self.after_row = after_row
		self.before = before
		self.after = after

		if file_path is not None:
			self.aggregate_file = open(
				file_path,
				'w+',
				encoding=output_encoding,
				newline='',
			)
		else:
			self.aggregate_file = tempfile_class(
				mode='w+',
				encoding=output_encoding,
				newline='',
			)

		output_csv_args = output_csv_args or dict()

		self._writer = csv.writer(
			self.aggregate_file,
			**output_csv_args
		)

		if before_all is not None:
			self._writer.writerows(before_all)

	def add(self, params, output):
		if self._aggregated:
			raise Exception("Has already been aggregated")

		wrapper = io.TextIOWrapper(
			output,
			encoding=self.input_encoding,
			newline='',
		)
		reader = csv.reader(wrapper, **self.input_csv_args)

		with self.output_lock:
			if self.before is not None:
				self._writer.writerows(evaluate(self.before, params))

			for row in reader:
				before = evaluate(
					listify(self.before_row, none_empty=True),
					params,
				)
				after = evaluate(
					listify(self.after_row, none_empty=True),
					params,
				)

				self._writer.writerow(
					itertools.chain(before, row, after)
				)

			if self.after is not None:
				self._writer.writerows(evaluate(self.after, params))

		if self.close_files:
			try:
				output.close()
			except AttributeError:
				pass

	def aggregate(self):
		self._aggregated = True

		if self.after_all is not None:
			self._writer.writerows(self.after_all)

		# Seek to beginning of the aggregate file
		self.aggregate_file.seek(0)

		return self.aggregate_file

	def __del__(self):
		if not self._aggregated:
			try:
				self.aggregate_file.close()
			except:
				pass


class Demux(object):
	"""Demux de-multiplexes output, distributing it to different Collectors.

	Args:
		watch (list of str): List of parameters to watch for: For each distinct
			combination of values in this list, a collector is maintained.
		factory (function): Called to create a new collector. A dict of
			parameters is passed as the only argument, containing only those
			parameters specified in `watch`.
		lock_class (class object or function): The class used to create a lock
			object.
	"""
	def __init__(
		self,
		watch,
		factory,
		lock_class=threading.Lock,
	):
		super(Demux, self).__init__()
		self.watch = watch
		self.factory = factory
		self._collectors = dict()
		self._lock = lock_class()

	def add(self, params, output):
		t = tuple(params[key] for key in self.watch)

		with self._lock:
			try:
				collector = self._collectors[t]
			except KeyError:
				collector = self.factory(params)
				self._collectors[t] = collector

		collector.add(params, output)

	def aggregate(self):
		return [
			collector.aggregate() for collector in self._collectors.values()
		]

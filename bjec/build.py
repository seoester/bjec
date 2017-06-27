import functools
import os
import urllib.parse
import enum
import subprocess
import datetime

import git

from .master import Runnable, Constructible, Artefactor, Dependency, WrapperRun
from .master import master as global_master
from .config import config
from .utils import listify, min_datetime, max_datetime


try:
	subprocess.run
except AttributeError:
	from .subprocess_run import run
	subprocess.run = run


def build(depends=None, master=None):
	def decorator(f):
		b = Build(f, depends=depends)

		nonlocal master

		if master is None:
			master = global_master

		@functools.wraps(f)
		def wrapper():
			b.run()

		master.register(b, wrapper)

		return wrapper

	return decorator


class Build(Dependency, Constructible, Artefactor, WrapperRun, Runnable):
	class Constructor(Dependency.ResolveConstructor, Artefactor.Constructor,
			Constructible.Constructor):
		@property
		def dependencies(self):
			return self._obj.dependencies

		def source(self, source):
			self._obj._sources.append(source)
			return source

		def builder(self, builder):
			self._obj._builders.append(builder)
			return builder

	def __init__(self, constructor_func, depends=None):
		super(Build, self).__init__()
		self._sources = list()
		self._builders = list()

		self.constructor_func(constructor_func)

		self.depends(*listify(depends, none_empty=True))

	def _run(self):
		all_change_info = ChangeInfo(
			ChangeInfo.Status.UNCHANGED,
			min_datetime,
		)

		for source in self._sources:
			change_info = source.scan()

			if change_info.status is ChangeInfo.Status.UNCHANGED:
				pass
			elif change_info.status is ChangeInfo.Status.UNKNOWN:
				if all_change_info.status is not ChangeInfo.Status.CHANGED:
					all_change_info.status = ChangeInfo.Status.UNKNOWN
			elif change_info.status is ChangeInfo.Status.CHANGED:
				all_change_info.status = ChangeInfo.Status.CHANGED

			all_change_info.last_changed = max(
				all_change_info.last_changed,
				change_info.last_changed,
			)

		last_built = min(map(lambda x: x.last_built(), self._builders),
			default=min_datetime)

		if (all_change_info.status is ChangeInfo.Status.CHANGED
			or last_built < all_change_info.last_changed):
			for builder in self._builders:
				builder.build()


class ChangeInfo(object):
	"""Comprises information about the state of changes of a Source.

	A ChangeInfo-like object is returned by ``Source.scan()``.

	Attributes:
		status (ChangeInfo.Status): Conveys any knowledge the Source has about
			whether changes have taken place.
			A Source may set ``status`` to ``CHANGED``, when it changed its
			files directly, e.g. pulled from a remote source, etc.
			``UNCHANGED`` may be set, when a version management system did not
			perform an update, ``UNKNOWN`` is the general case.
		last_changed (datetime.datetime): Date and time of the last change
			which took place in the Source. Generally only changes to a file's
			content are regarded as change.
	"""
	class Status(enum.Enum):
		UNKNOWN = 0
		UNCHANGED = 1
		CHANGED = 2

	def __init__(self, status, last_changed):
		super(ChangeInfo, self).__init__()
		self.status = status
		self.last_changed = last_changed


class Source(object):
	def scan(self):
		"""Perform a scan over the source set and return change info.

		**Must** be implemented by inheriting classes.

		Returns:
			ChangeInfo: An object adhering to the ChangeInfo documentation.
		"""
		raise NotImplementedError

	def local_path(self):
		"""Return the (base) path to the source on the local file system.

		**Must** be implemented by inheriting classes.

		Returns:
			str: The absolute path to the Source's local base directory.
		"""
		raise NotImplementedError


class Local(Source):
	"""docstring for Local"""
	def __init__(self, path):
		super(Local, self).__init__()
		self.path = path
		self._local_path = os.path.abspath(os.path.expanduser(path))

	def scan(self):
		last_time = min_datetime

		for root, dirs, files in os.walk(self._local_path, followlinks=True):
			for file in files:
				info = os.stat(os.path.join(self.path, root, file))
				time = datetime.datetime.fromtimestamp(info.st_mtime,
					tz=datetime.timezone.utc)

				if time > last_time:
					last_time = time

		if last_time == min_datetime:
			last_time = max_datetime

		return ChangeInfo(ChangeInfo.Status.UNKNOWN, last_time)

	def local_path(self):
		return self._local_path


class GitRepo(Source):
	"""docstring for GitRepo

	Args:
		url (str): Remote URL of the repository
		branch (str): Branch of the remote repository to use, default: "master"

	Configuration Options:
		* ``repos_path``: Path to local directory which repositories are
			downloaded to, defaults to `default_repos_path`
		* ``identity_file``: Path to an (SSH) identity file for authentication
		* ``identity_content``: Content of an (SSH) identity file for
			authentication

	Todo:
		* identity_file support
		* improve git url parsing (ports, path)
		* support sub-paths in repo
		* support submodules, ...
		* implement reset method?

	"""

	default_repos_path = "~/bjec/repos"
	"""See configuration option ``repos_path``."""

	def __init__(self, url, branch="master"):
		super(GitRepo, self).__init__()
		self.url = url
		self.branch = branch
		self.repo = None

		self._local_path = None
		self._parse_url()

	def scan(self):
		unchanged = self._try_fetch()

		if unchanged:
			status = ChangeInfo.Status.UNCHANGED
		else:
			status = ChangeInfo.Status.CHANGED

		return ChangeInfo(
			status,
			self.repo.commit().committed_datetime,
		)

	def local_path(self):
		return self._local_path

	def _parse_url(self):
		url = urllib.parse.urlparse(self.url)

		url_path = url.path
		split_ext = os.path.splitext(url_path)
		url_path = split_ext[0] if split_ext[1] == ".git" else url_path

		self._local_path = os.path.abspath(os.path.join(
			os.path.expanduser(
				config[GitRepo].get("repos_path", self.default_repos_path)
			),
			url.netloc,
			url_path.lstrip("/"),
		))

	def _create_repo_structure(self):
		try:
			os.makedirs(self._local_path)
		except FileExistsError:
			pass

	def _ensure_repo(self):
		"""Ensures that the repository is properly set up.

		Ensures the local repository exists with the specified branch and that
		tracking with the remote is configured. Performs the initial clone, if
		necessary. May also fetch from the remote if the branch is changed.

		Returns:
			bool: If any changes were made, False is returned, otherwise True.
		"""
		unchanged = True

		try:
			self.repo = git.Repo(self._local_path)
			remote = self.repo.remote()

			if remote.url != self.url:
				remote.set_url(self.url)
				unchanged = False
		except git.exc.InvalidGitRepositoryError:
			self.repo = git.Repo.init(self._local_path)
			remote = self.repo.create_remote("origin", self.url)
			unchanged = False

		assert remote.exists()

		if self.branch not in self.repo.heads:
			remote.fetch()
			assert self.branch in remote.refs
			local_branch = self.repo.create_head(
				self.branch,
				remote.refs[self.branch]
			)
			unchanged = False
		else:
			local_branch = self.repo.heads[self.branch]

		if local_branch.tracking_branch() != remote.refs[self.branch]:
			local_branch.set_tracking_branch(remote.refs[self.branch])
			unchanged = False

		return unchanged

	def _try_fetch(self):
		self._create_repo_structure()

		unchanged = self._ensure_repo()

		fetch_info = self.repo.remote().fetch()

		if fetch_info[0].commit != self.repo.heads[self.branch].commit:
			self.repo.remote().pull()
			unchanged = False

		if not unchanged:
			self.repo.heads[self.branch].checkout()

		return unchanged


class Builder(object):
	def build(self):
		"""

		**Must** be implemented by inheriting classes.
		"""
		raise NotImplementedError

	def last_built(self):
		"""

		**Must** be implemented by inheriting classes.
		"""
		raise NotImplementedError


class Make(Builder):
	"""docstring for Make

	Args:
		path (str): Path to the directory containing the Makefile
		target (str or list of str, optional): make target(s) to execute
		creates (str or list of str, optional): File path(s) created by make,
			may be absolute (starting with "/") or relative to `path`
		clean_first (bool, optional): When True, call `clean()` before starting
			to build (`clean_target` must be given)
		clean_target (str or list of str, optional): make target(s) to execute
			for cleaning

	Configuration Options:
		* ``environment``: Map of environment variables passed to the make call

	"""
	def __init__(self, path, target=None, creates=None, clean_first=False,
			clean_target=None):
		super(Make, self).__init__()
		self.path = path
		self.target = target
		self.creates = creates

		self.clean_first = clean_first
		self.clean_target = clean_target

		self._has_run = False

	def build(self):
		if self.clean_first:
			self.clean()

		args = ["make"]

		env = config[Make].get("environment")
		if env is not None:
			t = dict(os.environ)
			t.update(env)
			env = t

		if self.target is not None:
			args += listify(self.target)

		subprocess.run(
			args,
			cwd=self.path,
			env=env,
			stdin=subprocess.DEVNULL,
			stdout=subprocess.DEVNULL,
			stderr=subprocess.STDOUT,
			check=True,
		)

		self._has_run = True

	def last_built(self):
		"""

		Returns:
			datetime.datetime: The earliest mtime of any file in `creates`.

			If `creates` is None, empty or None of the files exist,
			datetime.datetime.min (aware, i.e. with added tzinfo) is returned.
		"""
		if self.creates is None:
			return min_datetime

		first_time = max_datetime

		for f_p in listify(self.creates):
			try:
				info = os.stat(os.path.join(self.path, f_p))
				time = datetime.datetime.fromtimestamp(info.st_mtime,
					tz=datetime.timezone.utc)

				if time < first_time:
					first_time = time
			except FileNotFoundError:
				pass

		if first_time == max_datetime:
			return min_datetime

		return first_time

	def clean(self):
		if self.clean_target is None:
			raise NotImplementedError(
				"Can't perform clean: No 'clean_target' parameter given"
			)

		args = ["make"] + listify(self.clean_target)

		env = config[Make].get("environment")
		if env is not None:
			t = dict(os.environ)
			t.update(env)
			env = t

		subprocess.run(
			args,
			cwd=self.path,
			env=env,
			stdin=subprocess.DEVNULL,
			stdout=subprocess.DEVNULL,
			stderr=subprocess.STDOUT,
			check=True,
		)

	def result(self):
		# Make might not have been called, because there have been no changes
		# the files' source.
		# if not self._has_run or self.creates is None:
		if self.creates is None:
			return None

		r = list(map(
			lambda p: os.path.abspath(os.path.join(self.path, p)),
			listify(self.creates)
		))

		if len(r) == 1:
			return r[0]
		else:
			return r

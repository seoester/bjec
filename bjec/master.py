from .utils import listify


class Runnable(object):
	def run(self):
		"""

		**Must** be implemented by inheriting classes.
		"""
		raise NotImplementedError

	def __call__(self):
		self.run()


class WrapperRun(object):
	"""docstring for WrapperRun"""
	def w_run(self):
		self._run()

	def run(self):
		self.w_run()


class Constructible(object):
	"""docstring for Constructible"""
	class Constructor(object):
		def __init__(self, obj):
			super(Constructible.Constructor, self).__init__()
			self._obj = obj

	def __init__(self):
		super(Constructible, self).__init__()
		self.__constructor_func = None
		self.__constructed = False

	def constructor_func(self, constructor_func):
		self.__constructor_func = constructor_func

	def constructed(self):
		return self.__constructed

	def construct(self):
		if self.__constructed:
			return

		cons = self.Constructor(self)
		self.__constructor_func(cons)

		self.__constructed = True

	def w_run(self):
		self.construct()

		super(Constructible, self).w_run()


class Artefactor(object):
	"""docstring for Artefactor

	Todo:
		* Policy on artefact() calls after artefact propagation
	"""
	class Constructor(object):
		def artefact(self, **kwargs):
			self._obj._artefact_funcs.update(kwargs)

	def __init__(self):
		super(Artefactor, self).__init__()
		self._artefact_funcs = dict()
		self.artefacts = dict()

	def artefact(self, **kwargs):
		self._artefact_funcs.update(kwargs)

	def _propagate_artefacts(self):
		for key, val in self._artefact_funcs.items():
			self.artefacts[key] = val()

	def w_run(self):
		super(Artefactor, self).w_run()

		self._propagate_artefacts()


class Registerable(object):
	def __init__(self):
		super(Registerable, self).__init__()
		self.__masters = list()

	def registered_with(self, master):
		self.__masters.append(master)

	def _resolve(self, key):
			for m in self.__masters:
				try:
					return m[key]
				except KeyError:
					pass

			raise KeyError("Could not resolve key '{}'".format(repr(key)))


class Dependency(Registerable):
	"""docstring for Dependency

	Dependency has two different Constructor variants:
	``SetUpConstructor`` allows adding dependencies to the object, while
	``ResolveConstructor`` makes resolved dependencies available with its
	`dependencies` attribute.

	Todo:
		* Policy on depends() calls after dependency fulfillment
		* 2 Constructors: a) Dependencies resolved and available
	"""
	class SetUpConstructor(object):
		def depends(self, *args):
			self._obj.depends(*args)

	class ResolveConstructor(object):
		@property
		def dependencies(self):
			return self._obj.dependencies

	class __Resolver(object):
		def __init__(self, parent, resolvable):
			super(Dependency._Dependency__Resolver, self).__init__()
			self.__parent = parent
			self.__resolvable = resolvable

		def __getitem__(self, key):
			item = self.__parent._resolve(key)

			if item in self.__resolvable:
				return item
			else:
				raise KeyError("Could not resolve key '{}'".format(repr(key)))

	def __init__(self):
		super(Dependency, self).__init__()
		self.__fulfilled = False
		self.__depends = list()
		self.dependencies = None

	def depends(self, *args):
		self.__depends.extend(listify(args))

	def fulfilled(self):
		return self.__fulfilled

	def _mark_fulfilled(self):
		self.__fulfilled = True

	def _fulfill_dependencies(self):
		resolvable = []

		for decl in self.__depends:
			try:
				dependency = self._resolve(decl)
			except KeyError:
				raise KeyError(
					"Could not resolve dependency declaration '{}'"
						.format(repr(decl))
				)

			resolvable.append(dependency)

			if not dependency.fulfilled():
				dependency.fulfill()

		self.dependencies = self.__Resolver(self, resolvable)

	def fulfill(self):
		"""Fulfills this dependency.

		**May** be implemented by inheriting classes, but defaults to calling
		`self.run()`. In this case however, self.run() has to ensure
		`_fulfill_dependencies()` is run.

		Should the object only be run once, the following can be inserted at
		the beginning of this method's implementation (or `self.run()`)::

			if self.fulfilled():
				return

		"""
		self.run()

	def w_run(self):
		if self.fulfilled():
			return

		self._fulfill_dependencies()

		super(Dependency, self).w_run()

		self._mark_fulfilled()


class Master(object):
	def __init__(self):
		self._registry = dict()
		self._name_registry = dict()
		self._func_name_registry = dict()
		self._secondaries = dict()

	def register(self, obj, func, secondary=None):
		secondary = listify(secondary, none_empty=True)

		self._registry[func] = obj
		self._name_registry[func.__module__ + "." + func.__name__] = obj
		self._func_name_registry[func.__name__] = obj

		for s in secondary:
			self._secondaries[s] = obj

		try:
			obj.registered_with(self)
		except AttributeError:
			pass

	def __getitem__(self, key):
		if isinstance(key, str):
			try:
				return self._name_registry[key]
			except KeyError:
				pass

			try:
				return self._func_name_registry[key]
			except KeyError:
				pass
		else:
			try:
				return self._registry[key]
			except KeyError:
				pass

		return self._secondaries[key]


master = Master()

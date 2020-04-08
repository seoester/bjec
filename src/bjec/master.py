from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Generic, Iterable, List, Optional, Set, Sequence, TypeVar, TYPE_CHECKING, Union

from .utils import listify


_T = TypeVar('_T')
_T_inner = TypeVar('_T_inner')
_Listifyable = Union[_T, Sequence[_T]]
_OptListifyable = Optional[Union[_T, Sequence[_T]]]
_FunctionObject = Callable[..., None] # None is not correct, this could be any function object
ResolveKey = Union[str, _FunctionObject]


class Runnable(ABC):
    @abstractmethod
    def run(self) -> None:
        """

        **Must** be implemented by inheriting classes.
        """
        raise NotImplementedError

    def __call__(self) -> None:
        self.run()


if TYPE_CHECKING:
    class _RunnableMixInBase(Runnable):
        def run(self) -> None: ...

        def __call__(self) -> None: ...

        def _run(self) -> None: ...

        def w_run(self) -> None: ...
else:
    _RunnableMixInBase = object


class WrapperRun(_RunnableMixInBase):
    """docstring for WrapperRun"""

    def w_run(self) -> None:
        self._run()

    def run(self) -> None:
        self.w_run()


class Constructible(_RunnableMixInBase):
    """docstring for Constructible"""

    class Constructor(object):
        def __init__(self, obj: Any) -> None:
            super(Constructible.Constructor, self).__init__()
            self._obj: Any = obj

    def __init__(self) -> None:
        super(Constructible, self).__init__()
        self.__constructor_func: Optional[Callable[[Any], None]] = None
        self.__constructed: bool = False

    @property
    def constructor_func(self) -> Callable[[Any], None]:
        if self.__constructor_func is None:
            raise Exception('constructor_func has not been set yet')

        return self.__constructor_func

    @constructor_func.setter
    def constructor_func(self, constructor_func: Callable[[Any], None]) -> None:
        self.__constructor_func = constructor_func

    @property
    def constructed(self) -> bool:
        return self.__constructed

    def construct(self) -> None:
        if self.__constructed:
            return

        if self.__constructor_func is None:
            raise Exception('constructor_func has not been set yet')

        cons = self.Constructor(self)
        self.__constructor_func(cons)

        self.__constructed = True

    def w_run(self) -> None:
        self.construct()

        super(Constructible, self).w_run()


class Artefactor(_RunnableMixInBase):
    """docstring for Artefactor

    Todo:
        * Policy on artefact() calls after artefact propagation
    """

    class Constructor(object):
        _obj: Any

        def add_artefacts(self, **kwargs: Callable[[], Any]) -> None:
            self._obj._artefact_funcs.update(kwargs)

    def __init__(self) -> None:
        super(Artefactor, self).__init__()
        self._artefact_funcs: Dict[str, Callable[[], Any]] = {}
        self._artefacts: Dict[str, Any] = {}

    @property
    def artefacts(self) -> Dict[str, Any]:
        return self._artefacts

    def add_artefacts(self, **kwargs: Callable[[], None]) -> None:
        self._artefact_funcs.update(kwargs)

    def _collect_artefacts(self) -> None:
        for key, val in self._artefact_funcs.items():
            self.artefacts[key] = val()

    def w_run(self) -> None:
        super(Artefactor, self).w_run()

        self._collect_artefacts()


class Registerable(_RunnableMixInBase):
    def __init__(self) -> None:
        super(Registerable, self).__init__()
        self.__masters: List[Master] = []

    def registered_with(self, master: 'Master') -> None:
        self.__masters.append(master)

    def _resolve(self, key: ResolveKey) -> 'Registerable':
            for m in self.__masters:
                try:
                    return m[key]
                except KeyError:
                    pass

            raise KeyError(f'Could not resolve key {key!r}')


class Dependency(Registerable, _RunnableMixInBase):
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
        _obj: 'Dependency'

        def depends(self, *args: ResolveKey) -> None:
            self._obj.depends(*args)

    class ResolveConstructor(object):
        _obj: 'Dependency'

        @property
        def dependencies(self) -> 'Dependency._Resolver':
            return self._obj.dependencies

    class _Resolver(object):
        def __init__(self, parent: 'Dependency', resolvable: Set[Registerable]) -> None:
            super(Dependency._Resolver, self).__init__()
            self.__parent: Dependency = parent
            self.__resolvable: Set[Registerable] = resolvable

        def __getitem__(self, key: ResolveKey) -> Registerable:
            item = self.__parent._resolve(key)

            if item in self.__resolvable:
                return item
            else:
                raise KeyError(f'Could not resolve key {key!r}')

    def __init__(self) -> None:
        super(Dependency, self).__init__()
        self.__fulfilled: bool = False
        self.__depends: List[ResolveKey] = []
        self.dependencies: Dependency._Resolver = Dependency._Resolver(self, set())

    def depends(self, *args: ResolveKey) -> None:
        self.__depends.extend(args)

    def fulfilled(self) -> bool:
        return self.__fulfilled

    def _mark_fulfilled(self) -> None:
        self.__fulfilled = True

    def _fulfill_dependencies(self) -> None:
        resolvable: Set[Registerable] = set()

        for decl in self.__depends:
            try:
                dependency = self._resolve(decl)
            except KeyError:
                raise KeyError(f'Could not resolve dependency declaration {decl!r}')

            resolvable.add(dependency)

            if isinstance(dependency, Dependency) and not dependency.fulfilled():
                dependency.fulfill()

        self.dependencies = self._Resolver(self, resolvable)

    def fulfill(self) -> None:
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

    def w_run(self) -> None:
        if self.fulfilled():
            return

        self._fulfill_dependencies()

        super(Dependency, self).w_run()

        self._mark_fulfilled()


class Master(object):
    def __init__(self) -> None:
        self._registry: Dict[_FunctionObject, Registerable] = {}
        self._name_registry: Dict[str, Registerable] = {}
        self._func_name_registry: Dict[str, Registerable] = {}
        self._aliases: Dict[str, Registerable] = {}

    def register(self, obj: Registerable, func: _FunctionObject, aliases: _OptListifyable[str]=None) -> None:
        aliases = listify(aliases, none_empty=True)

        self._registry[func] = obj
        self._name_registry[func.__module__ + "." + func.__name__] = obj
        self._func_name_registry[func.__name__] = obj

        for s in aliases:
            self._aliases[s] = obj

        try:
            obj.registered_with(self)
        except AttributeError:
            pass

    def __getitem__(self, key: ResolveKey) -> Registerable:
        if isinstance(key, str):
            try:
                return self._name_registry[key]
            except KeyError:
                pass

            try:
                return self._func_name_registry[key]
            except KeyError:
                pass

            return self._aliases[key]
        else:
            return self._registry[key]


master = Master()

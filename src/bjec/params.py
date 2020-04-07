from typing import Any, cast, Callable, Dict, Generic, Iterable, List as TList, Optional, Sequence, Tuple, TypeVar, Union
from typing_extensions import Protocol, runtime_checkable

"""

Todo:
    * Switch ParamSet to Mapping[str, Any]
    * Default value if a parameter is not available via mix-in (with_default).
        Achieve this by subclassing KeyError: UnavailableParam and check for
        this exception from mix-in.
    * Complex Dict type and resolve_mapping
    * Join interface.
        When is str called on arguments
    * Complex String type
    * Complex Path type.
        Interoperable with path lib.
        Supports concatenation through the ``/`` operator.

"""

_T = TypeVar('_T')
_T_inner = TypeVar('_T_inner')
_S = TypeVar('_S')
_T_co = TypeVar('_T_co', covariant=True)
_T_sb = TypeVar('_T_sb', str, bytes)

ParamSet = Dict[str, Any]


@runtime_checkable
class ParamsEvaluable(Protocol[_T_co]):
    def evaluate_with_params(self, params: ParamSet) -> _T_co:
        ...


Resolvable = Union[_T, ParamsEvaluable[_T]]
ListResolvable = Union[TList[Resolvable[_T]], Resolvable[TList[_T]]]
IterableResolvable = Union[Iterable[Resolvable[_T]], Resolvable[Iterable[_T]]]

def resolve(obj: Resolvable[_T], params: ParamSet) -> _T:
    try:
        return cast('ParamsEvaluable[_T]', obj).evaluate_with_params(params)
    except (AttributeError, TypeError):
        return cast('_T', obj)

def resolve_iterable(it: IterableResolvable[_T], params: ParamSet) -> Iterable[_T]:
    try:
        return cast('ParamsEvaluable[Iterable[_T]]', it).evaluate_with_params(params)
    except (AttributeError, TypeError):
        return (resolve(el, params) for el in cast('Iterable[Resolvable[_T]]', it))

def resolve_list(it: IterableResolvable[_T], params: ParamSet) -> TList[_T]:
    return list(resolve_iterable(it, params))


class _IdentityMixIn(object):
    def _set_initialisers(self, *args: Any, **kwargs: Any) -> None:
        self.__args: Tuple[Any, ...] = args
        self.__kwargs: Dict[str, Any] = kwargs

    def __repr__(self) -> str:
        args_str = ', '.join(repr(arg) for arg in self.__args)
        kwargs_str = ', '.join(f'{key!s}={val!r}' for key, val in self.__kwargs.items())
        intialisers = f'{args_str!s}, {kwargs_str!s}' if len(args_str) > 0 and len(kwargs_str) > 0 else f'{args_str!s}{kwargs_str!s}'
        return f'{self.__class__.__name__!s}({intialisers!s})'

    def __to_tuple(self) -> Tuple[Any, ...]:
        return (tuple(self.__args), tuple(self.__kwargs.values()))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _IdentityMixIn):
            return NotImplemented

        if type(other) != type(self):
            return NotImplemented

        return other.__to_tuple() == self.__to_tuple()

    def __hash__(self) -> int:
        return hash(self.__to_tuple())


class _WithTransformMixIn(Generic[_T]):

    # TODO: really don't want to put this method on the object as it adds the
    # method to any inheriting class. Instead the inheriting class must make
    # sure that evaluate_with_params(...) is implemented by one type in the
    # inheritance chain.
    # It is required though to inform the type checker that self is
    # ParamsEvaluable.

    # This definition does not suffice.
    # evaluate_with_params: Callable[[ParamSet], _T]

    def evaluate_with_params(self, params: ParamSet) -> _T:
        try:
            return cast('ParamsEvaluable[_T]', super(_WithTransformMixIn, self)).evaluate_with_params(params)
        except AttributeError:
            raise NotImplementedError(
                f'evaluate_with_params(...) is not properly implemented within the inheritance chain of {self!r}'
            )

    class _WithTransform(Generic[_T_inner, _S]):
        def __init__(self, evaluable: ParamsEvaluable[_T_inner], transform_func: Callable[[_T_inner], _S]) -> None:
            self._evaluable: ParamsEvaluable[_T_inner] = evaluable
            self._transform_func: Callable[[_T_inner], _S] = transform_func

        def evaluate_with_params(self, params: ParamSet) -> _S:
            return self._transform_func(self._evaluable.evaluate_with_params(params))

    def with_transform(self, transform_func: Callable[[_T], _S]) -> '_WithTransformMixIn._WithTransform[_T, _S]':
        return _WithTransformMixIn._WithTransform(self, transform_func)


class P(_IdentityMixIn, _WithTransformMixIn[_T], Generic[_T]):
    """Wrapper to allow intuitive parameter inclusion.

    P instances represent a 'future' parameter value, every instance contains
    the `key` of the parameter in the `params` dict.
    Each instance evaluates to the corresponding parameter's value.

    Other modules may accept `P` objects or lists containing `P` objects.
    These are then evaluated for every parameter set.

    Example:
        ::

            Environment.Fluid().set(CPUS=P('n_cpus'))

    Args:
        key: Parameter (key of the parameter in the `params`) dict.
    """

    def __init__(self, key: str) -> None:
        self._key: str = key
        self._set_initialisers(key)

    def evaluate_with_params(self, params: ParamSet) -> _T:
        return cast('_T', params[self._key])


class Join(_IdentityMixIn, _WithTransformMixIn[_T_sb], Generic[_T_sb]):
    """String / Bytes Join for lists containing ParamsEvaluable objects.

    The type of output is determined by the type of the `sep` argument.

    If the output should be a ``str``, ``str(.)`` will be called on each list
    element (in `*args`). If the output should be of type ``bytes``, the user
    has to ensure that each of the list elements are of ``bytes`` type and that
    ``ParamsEvaluable(.)`` returns a ``bytes`` object.

    Example:
        ::

            Join("out.", P("n"), ".csv")

    Args:

        *args: Elements to join, may be instances of ParamsEvaluable classes.

        sep: Separator used to join elements of `*args`. Must have the type of
            the output, i.e. if the output should be of a ``bytes`` type,
            ``sep`` must be as well. Defaults to ``''`` (str).
    """
    def __init__(self, *args: Union[_T_sb, ParamsEvaluable[_T_sb]], sep: Optional[_T_sb]=None) -> None:
        self._args: Tuple[Union[_T_sb, ParamsEvaluable[_T_sb]], ...] = args
        self._sep: _T_sb = sep if sep is not None else cast('_T_sb', '')
        self._set_initialisers(*args, sep=self._sep)

    def evaluate_with_params(self, params: ParamSet) -> _T_sb:
        return self._sep.join(resolve_iterable(self._args, params))


class Call(_IdentityMixIn, _WithTransformMixIn[_T], Generic[_T]):
    """Calls a function with ParamsEvaluable arguments.

    ``Call`` can also be used to instantiate objects, as this happens in the
    same way a function is called.

    Example:
        ::

            Call(Concatenate, file_path=Join("out.", P("n"), ".data"), close_files=True)

    Args:
        func: Function to be called.
        *args: Variable arguments passed to the class constructor. May contain
            ``ParamsEvaluable`` elements.
        **kwargs: Keyword arguments passed to the class constructor. May contain
            ``ParamsEvaluable`` values.
    """
    def __init__(self, func: Callable[..., _T], *args: Any, **kwargs: Any) -> None:
        self._func: Callable[..., _T] = func
        self._args: Tuple[Any, ...] = args
        self._kwargs: Dict[str, Any] = kwargs
        self._set_initialisers(func, *args, **kwargs)

    def evaluate_with_params(self, params: ParamSet) -> _T:
        args = list(resolve_iterable(self._args, params))
        kwargs = {
            key: resolve(value, params) for key, value in self._kwargs.items()
        }
        return self._func(*args, **kwargs)


class Lambda(_IdentityMixIn, _WithTransformMixIn[_T], Generic[_T]):
    """Calls a function with the params dict as the only argument.

    Convenient way to compute values based on parameters using a lambda
    expression.

    Example:
        ::

            Lambda(lambda p: p['alpha'] / p['beta'])

    Args:
        func: Function to be called on evaluation. The params dict is passed
            as the only argument.
    """
    def __init__(self, func: Callable[[ParamSet], _T]) -> None:
        self._func: Callable[[ParamSet], _T] = func
        self._set_initialisers(func)

    def evaluate_with_params(self, params: ParamSet) -> _T:
        return self._func(params)


class String(_IdentityMixIn, _WithTransformMixIn[str], object):
    """Expands a format string with the params dict on evaluation.

    Example:
        ::

            String('--nprocs={n}')

    Args:
        format_str: String which is expanded with the params dict values
            using ``str.format()``.
    """

    def __init__(self, format_str: str) -> None:
        self._format_str: str = format_str
        self._set_initialisers(format_str)

    def evaluate_with_params(self, params: ParamSet) -> str:
        return self._format_str.format(**params)


class List(_WithTransformMixIn[TList[_T]], Generic[_T]):
    """Utility to construct complex lists depending on parameters.

    Example:
        ::

            ['--mu', P('mu')] + List.Conditional(lambda p: 'sigma' in p, ['--sigma', P('sigma')]) + ['-']

    """

    class _Part(_IdentityMixIn, Generic[_T_inner]):
        def __add__(self, other: 'Union[List[_T_inner], IterableResolvable[_T_inner]]') -> 'List[_T_inner]':
            return cast('List[_T_inner]', List()) + self + other

        def __radd__(self, other: 'Union[List[_T_inner], IterableResolvable[_T_inner]]') -> 'List[_T_inner]':
            return cast('List[_T_inner]', List()) + other + self

        def evaluate_with_params(self, params: ParamSet) -> Iterable[_T_inner]:
            raise NotImplementedError()


    class Literal(_Part[_T_inner]):
        def __init__(self, it: IterableResolvable[_T_inner]) -> None:
            self._it: IterableResolvable[_T_inner] = ensure_multi_iterable(it)
            self._set_initialisers(it)

        def evaluate_with_params(self, params: ParamSet) -> Iterable[_T_inner]:
            return resolve_iterable(self._it, params)


    class Conditional(_Part[_T_inner]):
        def __init__(self, condition: Callable[[ParamSet], bool], it: IterableResolvable[_T_inner]) -> None:
            self._condition: Callable[[ParamSet], bool] = condition
            self._it: IterableResolvable[_T_inner] = ensure_multi_iterable(it)
            self._set_initialisers(condition, it)

        def evaluate_with_params(self, params: ParamSet) -> Iterable[_T_inner]:
            if self._condition(params):
                return resolve_iterable(self._it, params)
            else:
                return []


    def __init__(self) -> None:
        self._parts: TList[List._Part[_T]] = []

    def evaluate_with_params(self, params: ParamSet) -> TList[_T]:
        return [v for part in self._parts for v in part.evaluate_with_params(params)]

    def __add__(self, other: 'Union[List[_T], IterableResolvable[_T]]') -> 'List[_T]':
        if isinstance(other, List):
            return List._with_parts(self._parts + other._parts)

        if isinstance(other, List._Part):
            other_list: TList[List._Part[_T]] = [other]
            return List._with_parts(self._parts + other_list)

        wrapped: List._Part[_T] = List.Literal(other)
        return List._with_parts(self._parts + [wrapped])

    def __radd__(self, other: 'Union[List[_T], IterableResolvable[_T]]') -> 'List[_T]':
        if isinstance(other, List):
            return List._with_parts(other._parts + self._parts)

        if isinstance(other, List._Part):
            other_list: TList[List._Part[_T]] = [other]
            return List._with_parts(other_list + self._parts)

        wrapped: List._Part[_T] = List.Literal(other)
        return List._with_parts([wrapped] + self._parts)

    def __repr__(self) -> str:
        parts_str = ', '.join(repr(part) for part in self._parts)
        return f'{self.__class__.__name__}({parts_str})'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, List):
            return NotImplemented

        return other._parts == self._parts

    @classmethod
    def _with_parts(cls, parts: 'Iterable[List._Part[_T]]') -> 'List[_T]':
        l: List[_T] = List()
        l._parts = list(parts)
        return l


def ensure_multi_iterable(it: IterableResolvable[_T]) -> IterableResolvable[_T]:
    """Returns a multi-iterable variant of it.

    An iterator is a valid iterable but can only be iterated once. This
    function creates a semantic copy of ``it`` which can be iterated many
    times.

    If ``it`` fulfills the ``ParamsEvaluable`` protocol, it is assumed that
    multi-iteration is supported and ``it`` is returned as is.
    If ``it`` fulfills the ``Sequence`` ABC, multi-iteration is supported and
    ``it`` is returned as is.
    Otherwise, ``it`` is read into a list which is then returned.
    """

    if isinstance(it, ParamsEvaluable):
        return it

    if isinstance(it, Sequence):
        return it

    return list(it)

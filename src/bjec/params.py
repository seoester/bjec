from typing import Any, cast, Callable, Dict as TDict, Generic, Iterable, Iterator, List as TList, Mapping, Optional, Sequence, Tuple, Type, TypeVar, Union
from typing_extensions import Protocol, runtime_checkable

"""

Todo:
    * Join generalisation?
        Under what circumstances (if every) is str/bytes called on arguments?
    * Complex String type
        Should subsume Join and String.
        Supports concatenation through the ``+`` operator.
        How to integrate with P(), which can evaluate to anything?
        P() could check when __radd__ is called what type the other element
        has and act accordingly... Similarly for the ``/`` operator.
        Join to enable iterator joining like ``''.join()``?
    * Complex Path type.
        Interoperable with path lib.
        Supports concatenation through the ``/`` operator.

"""

_T = TypeVar('_T')
_T_inner = TypeVar('_T_inner')
_S = TypeVar('_S')
_S_inner = TypeVar('_S_inner')
_T_co = TypeVar('_T_co', covariant=True)
_T_sb = TypeVar('_T_sb', str, bytes)

ParamSet = Mapping[str, Any]


@runtime_checkable
class ParamsEvaluable(Protocol[_T_co]):
    def evaluate_with_params(self, params: ParamSet) -> _T_co:
        ...


Resolvable = Union[_T, ParamsEvaluable[_T]]
ListResolvable = Union[TList[Resolvable[_T]], Resolvable[TList[_T]]]
IterableResolvable = Union[Iterable[Resolvable[_T]], Resolvable[Iterable[_T]]]
MappingResolvable = Union[Mapping[Resolvable[_T], Resolvable[_S]], Resolvable[Mapping[_T, _S]]]
PairsResolvable = Union[Resolvable[Iterable[Tuple[_T, _S]]], Iterable[Resolvable[Tuple[_T, _S]]], Iterable[Tuple[Resolvable[_T], Resolvable[_S]]]]

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

def resolve_mapping(m: MappingResolvable[_T, _S], params: ParamSet) -> Mapping[_T, _S]:
    try:
        return cast('ParamsEvaluable[Mapping[_T, _S]]', m).evaluate_with_params(params)
    except (AttributeError, TypeError):
        return {
            resolve(key, params): resolve(value, params)
            for key, value in cast('Mapping[Resolvable[_T], Resolvable[_S]]', m).items()
        }

def resolve_dict(m: MappingResolvable[_T, _S], params: ParamSet) -> TDict[_T, _S]:
    return dict(resolve_mapping(m, params))

def _resolve_pairs(pairs: PairsResolvable[_T, _S], params: ParamSet) -> Iterable[Tuple[_T, _S]]:
    try:
        return cast('ParamsEvaluable[Iterable[Tuple[_T, _S]]]', pairs).evaluate_with_params(params)
    except (AttributeError, TypeError):
        def f(element: Union[Resolvable[Tuple[_T, _S]], Tuple[Resolvable[_T], Resolvable[_S]]]) -> Tuple[_T, _S]:
            if isinstance(element, tuple):
                return (resolve(element[0], params), resolve(element[1], params))
            else:
                return element.evaluate_with_params(params)

        return map(f, cast('Union[Iterable[Resolvable[Tuple[_T, _S]]], Iterable[Tuple[Resolvable[_T], Resolvable[_S]]]]', pairs))


class _IdentityMixIn(object):
    def _set_initialisers(self, *args: Any, **kwargs: Any) -> None:
        self.__args: Tuple[Any, ...] = args
        self.__kwargs: TDict[str, Any] = kwargs

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


class ParamUnavailable(KeyError):
    pass

    @classmethod
    def wrap_params(cls, params: ParamSet) -> '_CustomKeyErrorMapping':
        """Returns wrapped ``params`` raising ParamUnavailable on key miss.
        """
        return _CustomKeyErrorMapping(params, exc_type=cls)


class _CustomKeyErrorMapping(Mapping[str, Any]):
    def __init__(self, params: ParamSet, exc_type: Type[KeyError]=ParamUnavailable) -> None:
        self._params: ParamSet = params
        self._exc_type: Type[KeyError] = exc_type

    def __getitem__(self, key: str) -> Any:
        try:
            return self._params[key]
        except KeyError as e:
            raise self._exc_type(*e.args)

    def __iter__(self) -> Iterator[str]:
        return iter(self._params)

    def __len__(self) -> int:
        return len(self._params)

    def __contains__(self, obj: object) -> bool:
        return obj in self._params


class _WithMixIn(Generic[_T]):

    # TODO: really don't want to put this method on the object as it adds the
    # method to any inheriting class. Instead the inheriting class must make
    # sure that evaluate_with_params(...) is implemented by a type in the
    # inheritance chain.
    # The definition is required here though to inform the type checker that
    # self is ParamsEvaluable.

    # These definitions do not suffice.
    # Fails ParamsEvaluable interface for _WithMixIn[_T]
    # evaluate_with_params: Callable[[ParamSet], _T]
    # P and other inheriters are fail due to
    # "Signature of evaluate_with_params incompatible with supertype ..."
    # evaluate_with_params: Callable[['_WithMixIn[_T]', ParamSet], _T] # Does not suffice

    def evaluate_with_params(self, params: ParamSet) -> _T:
        try:
            return cast('ParamsEvaluable[_T]', super(_WithMixIn, self)).evaluate_with_params(params)
        except AttributeError:
            raise NotImplementedError(
                f'evaluate_with_params(...) is not properly implemented within the inheritance chain of {self!r}'
            )

    class _Transform(Generic[_T_inner, _S_inner]):
        def __init__(self, evaluable: ParamsEvaluable[_T_inner], transform_func: Callable[[_T_inner], _S_inner]) -> None:
            self._evaluable: ParamsEvaluable[_T_inner] = evaluable
            self._transform_func: Callable[[_T_inner], _S_inner] = transform_func

        def evaluate_with_params(self, params: ParamSet) -> _S_inner:
            return self._transform_func(self._evaluable.evaluate_with_params(params))

        def transform(self, transform_func: Callable[[_S_inner], _S]) -> '_WithMixIn._Transform[_S_inner, _S]':
            return _WithMixIn._Transform(self, transform_func)

        def default(self, default: _S) -> '_WithMixIn._Default[_S_inner, _S]':
            return _WithMixIn._Default(self, default)

    class _Default(Generic[_T_inner, _S_inner]):
        def __init__(self, evaluable: ParamsEvaluable[_T_inner], default: _S_inner) -> None:
            self._evaluable: ParamsEvaluable[_T_inner] = evaluable
            self._default: _S_inner = default

        def evaluate_with_params(self, params: ParamSet) -> Union[_T_inner, _S_inner]:
            try:
                return self._evaluable.evaluate_with_params(ParamUnavailable.wrap_params(params))
            except ParamUnavailable:
                return self._default

        def transform(self, transform_func: Callable[[Union[_T_inner, _S_inner]], _S]) -> '_WithMixIn._Transform[Union[_T_inner, _S_inner], _S]':
            return _WithMixIn._Transform(self, transform_func)

        def default(self, default: _S) -> '_WithMixIn._Default[Union[_T_inner, _S_inner], _S]':
            return _WithMixIn._Default(self, default)

    def transform(self, transform_func: Callable[[_T], _S]) -> '_WithMixIn._Transform[_T, _S]':
        return _WithMixIn._Transform(self, transform_func)

    def default(self, default: _S) -> '_WithMixIn._Default[_T, _S]':
        return _WithMixIn._Default(self, default)


class P(_IdentityMixIn, _WithMixIn[_T], Generic[_T]):
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


class Join(_IdentityMixIn, _WithMixIn[_T_sb], Generic[_T_sb]):
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


class Call(_IdentityMixIn, _WithMixIn[_T], Generic[_T]):
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
    def __init__(self, func: Callable[..., _T], *args: Resolvable[Any], **kwargs: Resolvable[Any]) -> None:
        self._func: Callable[..., _T] = func
        self._args: Tuple[Resolvable[Any], ...] = args
        self._kwargs: TDict[str, Resolvable[Any]] = kwargs
        self._set_initialisers(func, *args, **kwargs)

    def evaluate_with_params(self, params: ParamSet) -> _T:
        args = list(resolve_iterable(self._args, params))
        kwargs = {
            key: resolve(value, params) for key, value in self._kwargs.items()
        }
        return self._func(*args, **kwargs)


class Lambda(_IdentityMixIn, _WithMixIn[_T], Generic[_T]):
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


class String(_IdentityMixIn, _WithMixIn[str]):
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


class List(_WithMixIn[TList[_T]], Generic[_T]):
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


class Dict(_WithMixIn[TDict[_T, _S]], Generic[_T, _S]):
    """Utility to construct complex dictionaries depending on parameters.

    Example:
        ::

            {'--mu': P('mu')} + Dict.Conditional(lambda p: 'sigma' in p, {'--sigma': P('sigma')}) + {P('extra_key'): P('extra_value')}

    Todo:
        * Unsetting keys?
    """

    class _Part(_IdentityMixIn, Generic[_T_inner, _S_inner]):
        def __add__(self, other: 'Union[Dict[_T_inner, _S_inner], MappingResolvable[_T_inner, _S_inner]]') -> 'Dict[_T_inner, _S_inner]':
            return cast('Dict[_T_inner, _S_inner]', Dict()) + self + other

        def __radd__(self, other: 'Union[Dict[_T_inner, _S_inner], MappingResolvable[_T_inner, _S_inner]]') -> 'Dict[_T_inner, _S_inner]':
            return cast('Dict[_T_inner, _S_inner]', Dict()) + other + self

        def evaluate_with_params(self, params: ParamSet) -> Mapping[_T_inner, _S_inner]:
            raise NotImplementedError()


    class Literal(_Part[_T_inner, _S_inner]):
        def __init__(self, m: MappingResolvable[_T_inner, _S_inner]) -> None:
            self._m: MappingResolvable[_T_inner, _S_inner] = m
            self._set_initialisers(m)

        def evaluate_with_params(self, params: ParamSet) -> Mapping[_T_inner, _S_inner]:
            return resolve_mapping(self._m, params)


    class Conditional(_Part[_T_inner, _S_inner]):
        def __init__(self, condition: Callable[[ParamSet], bool], m: MappingResolvable[_T_inner, _S_inner]) -> None:
            self._condition: Callable[[ParamSet], bool] = condition
            self._m: MappingResolvable[_T_inner, _S_inner] = m
            self._set_initialisers(condition, m)

        def evaluate_with_params(self, params: ParamSet) -> Mapping[_T_inner, _S_inner]:
            if self._condition(params):
                return resolve_mapping(self._m, params)
            else:
                return {}


    class Pairs(_Part[_T_inner, _S_inner]):
        def __init__(self, it: IterableResolvable[Tuple[_T_inner, _S_inner]]) -> None:
            self._it: IterableResolvable[Tuple[_T_inner, _S_inner]] = ensure_multi_iterable(it)
            self._set_initialisers(it)

        def evaluate_with_params(self, params: ParamSet) -> Mapping[_T_inner, _S_inner]:
            return dict(_resolve_pairs(self._it, params))


    def __init__(self) -> None:
        self._parts: TList[Dict._Part[_T, _S]] = []

    def evaluate_with_params(self, params: ParamSet) -> TDict[_T, _S]:
        return {
            key: value
            for part in self._parts
            for key, value in part.evaluate_with_params(params).items()
        }

    def __add__(self, other: 'Union[Dict[_T, _S], MappingResolvable[_T, _S]]') -> 'Dict[_T, _S]':
        if isinstance(other, Dict):
            return Dict._with_parts(self._parts + other._parts)

        if isinstance(other, Dict._Part):
            other_dict: TList[Dict._Part[_T, _S]] = [other]
            return Dict._with_parts(self._parts + other_dict)

        wrapped: Dict._Part[_T, _S] = Dict.Literal(other)
        return Dict._with_parts(self._parts + [wrapped])

    def __radd__(self, other: 'Union[Dict[_T, _S], MappingResolvable[_T, _S]]') -> 'Dict[_T, _S]':
        if isinstance(other, Dict):
            return Dict._with_parts(other._parts + self._parts)

        if isinstance(other, Dict._Part):
            other_dict: TList[Dict._Part[_T, _S]] = [other]
            return Dict._with_parts(other_dict + self._parts)

        wrapped: Dict._Part[_T, _S] = Dict.Literal(other)
        return Dict._with_parts([wrapped] + self._parts)

    def __repr__(self) -> str:
        parts_str = ', '.join(repr(part) for part in self._parts)
        return f'{self.__class__.__name__}({parts_str})'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Dict):
            return NotImplemented

        return other._parts == self._parts

    @classmethod
    def _with_parts(cls, parts: 'Iterable[Dict._Part[_T, _S]]') -> 'Dict[_T, _S]':
        l: Dict[_T, _S] = Dict()
        l._parts = list(parts)
        return l

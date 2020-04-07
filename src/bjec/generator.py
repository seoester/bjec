from abc import ABC, abstractmethod
import itertools
from typing import Any, Dict, Iterable, Iterator, Tuple

from .params import ParamSet


class Generator(ABC):
    """Produces parameter sets.

    A top-level generator produces fully specified parameter sets. Each such
    parameter set results in an independent invocation or execution when
    processed by a :obj:`Processor`.

    So-called higher-level generators take other generators on input and
    combine the produced parameter sets in specific ways.

    The ``Generator`` ABC is basically a standard python iterable, i.e. the
    ``__iter__`` method has to be defined and return an iterator.
    """

    @abstractmethod
    def __iter__(self) -> Iterator[ParamSet]:
        """Returns an iterator over the produced parameter sets.

        **Must** be implemented by inheriting classes.
        """
        raise NotImplementedError()


class Literal(Generator):
    def __init__(self, **params: Any) -> None:
        super(Literal, self).__init__()
        self._param_set: Dict[str, Any] = params

    def __iter__(self) -> Iterator[ParamSet]:
        return iter([self._param_set])


class Matrix(Generator):
    def __init__(self, **params: Iterable[Any]) -> None:
        super(Matrix, self).__init__()
        self._params: Dict[str, Iterable[Any]] = params

    def __iter__(self) -> Iterator[ParamSet]:
        return (
            dict(zip(self._params.keys(), values))
            for values in itertools.product(*self._params.values())
        )


class Repeat(Generator):
    def __init__(self, generator: Generator, n: int) -> None:
        super(Repeat, self).__init__()
        self._generator: Generator = generator
        self._n: int = n

    def __iter__(self) -> Iterator[ParamSet]:
        """

        Roughly equivalent to::

            for params in self._generator:
                for _ in range(self._n):
                    yield params

        """
        return itertools.chain.from_iterable(
            (itertools.repeat(params, self._n) for params in self._generator)
        )


class Chain(Generator):
    def __init__(self, *generators: Generator):
        super(Chain, self).__init__()
        self._generators: Tuple[Generator, ...] = generators

    def __iter__(self) -> Iterator[ParamSet]:
        return itertools.chain(*self._generators)


class Product(Generator):
    def __init__(self, *generators: Generator):
        super(Product, self).__init__()
        self._generators: Tuple[Generator, ...] = generators

    def __iter__(self) -> Iterator[ParamSet]:
        return (
            dict(itertools.chain.from_iterable(
                params.items() for params in param_sets
            ))
            for param_sets in itertools.product(*self._generators)
        )

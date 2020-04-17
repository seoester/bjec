from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any, Generic, Iterable, Iterator, Optional, Tuple, Type, TypeVar

from .params import ParamSet


_T = TypeVar('_T')
_T_co = TypeVar('_T_co', covariant=True)


class Processor(Generic[_T_co], ABC):
    """docstring for Processor
    """

    def __enter__(self) -> 'Processor[_T_co]':
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional[TracebackType] = None,
    ) -> Optional[bool]:
        return None

    @abstractmethod
    def process(
        self,
        runnable: Any,
        params_it: Iterable[ParamSet],
    ) -> Iterator[Tuple[ParamSet, _T_co]]:
        """Process all parameter sets in the iterable according to a runnable.

        **Must** be implemented by inheriting classes.
        """
        raise NotImplementedError()

from abc import ABC, abstractmethod
from typing import Any, Generic, Iterable, Iterator, Optional, Tuple, TypeVar

from .params import ParamSet


_T = TypeVar('_T')
_T_co = TypeVar('_T_co', covariant=True)


class Processor(Generic[_T_co], ABC):
    """docstring for Processor

    A ``Processor`` is responsible for the task execution pipeline, that is
    fetching parameter sets from a ``Generator``, handing them to a ``Runner``
    and passing the ``Runner``'s output to a ``Collector``.
    Meanwhile the ``Processor`` has to manage its ``Runners'`` lifecycle.
    """

    def __enter__(self) -> 'Processor[_T_co]':
        pass

    def __exit__(
        self,
        t: Optional[type] = None,
        exc: Optional[BaseException] = None,
        tb: Optional[Any] = None,
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

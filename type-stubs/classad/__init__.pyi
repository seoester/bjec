from typing import Iterator, Mapping, overload, Optional, TypeVar, Union

_T = TypeVar('_T')


class ExprTree:
    """Placeholder for classad.ExprTree
    """
    pass


_ClassAdValue = Union[str, int, float, bool, ExprTree]


class ClassAd(Mapping[str, _ClassAdValue]):
    """Placeholder for classad.ClassAd
    """
    def __init__(self, input: Optional[str] = ...) -> None: ...

    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...
    def __getitem__(self, k: str) -> _ClassAdValue: ...
    @overload
    def get(self, k: str) -> Optional[_ClassAdValue]: ...
    @overload
    def get(self, k: str, default: Union[_ClassAdValue, _T]) -> Union[_ClassAdValue, _T]: ...

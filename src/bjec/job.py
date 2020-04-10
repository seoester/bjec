import functools
from typing import Any, Callable, List, Optional, Union

from .master import Runnable, Registerable, Constructible, Artefactor, Dependency, WrapperRun, Master, ResolveKey
from .master import master as global_master
from .utils import listify
from .generator import Generator
from .collector import Collector
from .processor import Processor


class Job(Dependency, Constructible, Artefactor, WrapperRun, Runnable):
    class Constructor(Dependency.ResolveConstructor, Artefactor.Constructor,
            Constructible.Constructor):
        _obj: 'Job'

        @property
        def generator(self) -> Generator:
            if self._obj._generator is None:
                raise Exception('Insufficiently specified')

            return self._obj._generator

        @generator.setter
        def generator(self, generator: Generator) -> None:
            self._obj._generator = generator

        @property
        def processor(self) -> Processor[Any]:
            if self._obj._processor is None:
                raise Exception('Insufficiently specified')

            return self._obj._processor

        @processor.setter
        def processor(self, processor: Processor[Any]) -> None:
            self._obj._processor = processor

        @property
        def runnable(self) -> Any:
            if self._obj._runnable is None:
                raise Exception('Insufficiently specified')

            return self._obj._runnable

        @runnable.setter
        def runnable(self, runnable: Any) -> None:
            self._obj._runnable = runnable

        @property
        def collector(self) -> Collector[Any]:
            if self._obj._collector is None:
                raise Exception('Insufficiently specified')

            return self._obj._collector

        @collector.setter
        def collector(self, collector: Collector[Any]) -> None:
            self._obj._collector = collector

        def after(self, *after_funcs: Callable[['Job'], None]) -> None:
            self._obj._after_funcs.extend(after_funcs)


    def __init__(
        self,
        constructor_func: Callable[['Job.Constructor'], None],
        depends: List[ResolveKey] = [],
    ) -> None:
        super(Job, self).__init__()
        self._generator: Optional[Generator] = None
        self._processor: Optional[Processor[Any]] = None
        self._runnable: Optional[Any] = None
        self._collector: Optional[Collector[Any]] = None
        self._after_funcs: List[Callable[[Job], None]] = []

        self.constructor_func = constructor_func

        self.depends(*depends)

    @property
    def generator(self) -> Generator:
        if self._generator is None:
            raise Exception('Insufficiently specified')

        return self._generator

    @property
    def processor(self) -> Processor[Any]:
        if self._processor is None:
            raise Exception('Insufficiently specified')

        return self._processor

    @property
    def runnable(self) -> Any:
        if self._runnable is None:
            raise Exception('Insufficiently specified')

        return self._runnable

    @property
    def collector(self) -> Collector[Any]:
        if self._collector is None:
            raise Exception('Insufficiently specified')

        return self._collector

    def run(self) -> None:
        super(Job, self).run()

        for after_func in self._after_funcs:
            after_func(self)

    def _run(self) -> None:
        if self._generator is None:
            raise Exception('Insufficiently specified, generator missing')

        if self._processor is None:
            raise Exception('Insufficiently specified, processor missing')

        if self._collector is None:
            raise Exception('Insufficiently specified, collector missing')

        with self._processor as processor, self._collector as collector:
            collector.collect(processor.process(self._runnable, self._generator))


def job(
    depends: List[ResolveKey] = [],
    master: Optional[Master] = None,
) -> Callable[[Callable[[Job.Constructor], None]], Callable[[], None]]:
    def decorator(f: Callable[[Job.Constructor], None]) -> Callable[[], None]:
        j = Job(f, depends=depends)

        nonlocal master

        if master is None:
            master = global_master

        @functools.wraps(f)
        def wrapper() -> None:
            j.run()

        master.register(j, wrapper)

        return wrapper

    return decorator

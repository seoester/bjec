from typing import cast

from bjec.collector import Collector, Noop

def typing_test_variance() -> None:
    """Statically test that type var variance works as expected.

    This test is statically performed by mypy, the type checker.
    The test is considered passed if mypy does not raise any errors.
    """

    class A: ...


    class B(A): ...


    def write_to_collector_of_a(c: Collector[A]) -> None:
        pass

    def write_to_collector_of_b(c: Collector[B]) -> None:
        pass

    write_to_collector_of_a(cast('Collector[A]', Noop()))
    # mypy should error here: The collector expects all written results to be
    # instances of B. An instance of A is not an instance of B.
    write_to_collector_of_a(cast('Collector[B]', Noop())) # type: ignore[arg-type]

    write_to_collector_of_b(cast('Collector[B]', Noop()))
    # This should work: The collector exepcts all written results to be
    # instances of A. An instance of B is also an instance of A.
    write_to_collector_of_b(cast('Collector[A]', Noop()))

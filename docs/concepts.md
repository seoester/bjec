# Concepts

## Constructor

### Constructor Function

A constructor function is similar to ``__init__()`` in Python. It is function
which configures an object instance without being attached to the object's
type.

It is suitable to alleviate the need of sub-classing: A constructor function
is defined for each kind of :obj:`Job` instead of a sub-class with only a
``__init__()`` method. Furthermore, additional capabilities can be made
available within the constructor function. This results in a well-defined
interface for the user. For example, the list of dependencies is already
populated. Constructor function can be defined for each kind of
user-constructible object.

Similar to ``self``, an object to be manipulated is passed as the first
parameter. This is the *constructor object*. Constructor objects are not the
final object to be configured, but instead a mutable container which holds all
configurable values. For the user these details are not relevant and

The parameter is commonly named after the type. For example, ``job``
or ``j`` is used for :obj:`Job` constructor functions.


The lifecycle of object construction through a constructor function is as
follows:

 1. The constructor object is initialised. It may receive additional data
    such as the list of dependencies or be linked to an instance of the class
    which it configures.
    The constructor object types are implemented through the use of mix-ins
    with clear responsibilities.
 2. The constructor function is called with the constructor object as its
    first parameter.
 3. The final object, e.g. a :obj:`Job` instance, is finalised from the
    configuration stored in the constructor object.

These steps are performed by the constructor of the final object, e.g. the
:obj:`Job` instance, during the :obj:`Runnable.run` call.

## Configuration File

## Fluid Builder

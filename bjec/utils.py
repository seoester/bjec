import collections
import datetime


def listify(obj, none_empty=False):
	"""listify turns `obj` into an iterable.

	Returns:
		`obj` is simply returned, if it already is an iterable.
		Otherwise - or if it a string - it is wrapped in a list.
		If `none_empty` is set to ``True``, an empty list is returned, if `obj`
		is ``None``.
	"""
	if obj is None and none_empty:
		return []
	elif isinstance(obj, str):
		return [obj]
	elif not isinstance(obj, collections.Iterable):
		return [obj]
	else:
		return obj


min_datetime = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
"""Minimum representable datetime with timezone ("aware") set to UTC."""

max_datetime = datetime.datetime.max.replace(tzinfo=datetime.timezone.utc)
"""Maximum representable datetime with timezone ("aware") set to UTC."""

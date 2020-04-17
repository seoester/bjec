
from bjec.htcondor import _args_to_str, _environment_to_str, _file_remaps_to_str

def test_args_to_str() -> None:
	"""

	These test cases are based on testing conducted with HTCondor v8.9.4.
	"""

	assert _args_to_str([]) == '""'
	assert _args_to_str(['a']) == '"a"'
	assert _args_to_str(['a and b']) == '"\'a and b\'"'
	assert _args_to_str(['"']) == '""""'
	assert _args_to_str(['"a ..." said he']) == '"\'""a ..."" said he\'"'
	assert _args_to_str(['\'']) == '"\'\'\'\'"'
	assert _args_to_str(['\'and b\' said she']) == '"\'\'\'and b\'\' said she\'"'
	assert _args_to_str(['']) == '"\'\'"'

	assert _args_to_str(['"a ..." said he', '\'and b\' said she']) \
		== '"\'""a ..."" said he\' \'\'\'and b\'\' said she\'"'

def test_environment_to_str() -> None:
	"""

	These test cases are based on testing conducted with HTCondor v8.9.4.
	"""

	assert _environment_to_str({}) == '""'
	assert _environment_to_str({'K': 'a'}) == '"K=a"'
	assert _environment_to_str({'K': 'a and b'}) == '"K=\'a and b\'"'
	assert _environment_to_str({'K': '"'}) == '"K="""'
	assert _environment_to_str({'K': '"a ..." said he'}) == '"K=\'""a ..."" said he\'"'
	assert _environment_to_str({'K': '\''}) == '"K=\'\'\'\'"'
	assert _environment_to_str({'K': '\'and b\' said she'}) == '"K=\'\'\'and b\'\' said she\'"'
	assert _environment_to_str({'K': ''}) == '"K=\'\'"'

	assert _environment_to_str({'K': '"a ..." said he', 'P': '\'and b\' said she'}) \
		== '"K=\'""a ..."" said he\' P=\'\'\'and b\'\' said she\'"'

def test_file_remaps_to_str() -> None:
	"""

	These test cases are based on testing conducted with HTCondor v8.9.4.
	"""

	assert _file_remaps_to_str({}) == '""'
	assert _file_remaps_to_str({'a': 'b'}) == '"a=b"'
	assert _file_remaps_to_str({'a': 'b', 'c': '/d/d'}) == '"a=b;c=/d/d"'
	assert _file_remaps_to_str({'a': 'b b'}) == '"a=b b"'
	assert _file_remaps_to_str({'a;a=a': 'b;b=b'}) == '"a;a\\=a=b\\;b=b"'

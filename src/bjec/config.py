from typing import Any, Dict, Iterable, Optional, Tuple, Type, Union
import yaml

from .io import PathType


class ModuleConfig(object):
    def __init__(self, config: 'Config', key_parts: Iterable[str]) -> None:
        super(ModuleConfig, self).__init__()
        self._config: Config = config
        self._key_parts: Tuple[str, ...] = tuple(key_parts)

    @property
    def key_parts(self) -> Tuple[str, ...]:
        return self._key_parts

    def __getitem__(self, key: str) -> Union['ModuleConfig', Any]:
        config_elm: Union[Dict[str, Any], Any] = self._config._config_dict

        key_parts = self._key_parts + (key,)

        try:
            for key_part in key_parts:
                config_elm = config_elm[key_part]
        except KeyError:
            key_str = '.'.join(key_parts)
            raise KeyError(f'{key!r} in {key_str} not in config')
        except TypeError as e:
            key_str = '.'.join(key_parts)
            raise KeyError(f'{key!r} in {key_str} does not resolve to a dict')

        return config_elm

    def get(self, key: str, default: Optional[Union[Any]]=None) -> Optional[Union['ModuleConfig', Any]]:
        try:
            return self[key]
        except KeyError:
            return default


class Config(object):
    def __init__(self, namespace: str='bjec') -> None:
        super(Config, self).__init__()
        self._namespace: str = namespace
        self._config_dict: Dict[str, Any] = {}
        self._user_module: ModuleConfig = ModuleConfig(self, ['User'])

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def User(self) -> ModuleConfig:
        return self._user_module

    def __getitem__(self, key: Union[str, object, Type[object]]) -> Any:
        if not isinstance(key, str):
            if isinstance(key, type):
                key = key.__module__ + '.' + key.__name__
            else:
                key = key.__module__ + '.' + key.__class__.__name__

        key_parts = key.split('.')

        if key_parts[0] == self._namespace:
            key_parts = key_parts[1:]

        return ModuleConfig(self, key_parts)

    def read_yaml(self, path: PathType) -> None:
        with open(path) as f:
            config = yaml.load(f)

        self._config_dict.update(config)


config = Config()

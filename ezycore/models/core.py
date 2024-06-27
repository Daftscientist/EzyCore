from __future__ import annotations
from typing import Generic, Dict, Iterator, TypeVar, Union, Any

from pydantic import BaseModel, Field, ValidationError, root_validator
from ezycore.exceptions import ModalMissingConfig


class Config(BaseModel):
    """
    Configuration class used by the ezycore module. 
    Used to customise and control how ezycore behaves with segments and models

    Parameters
    ----------
    search_by: :class:`str`
        Which key to store as the primary key

        .. warning::
            This field **MUST** be **UNIQUE**
    exclude: Union[:class:`dict`, :class:`set`]
        Fields to exclude from being returned when being fetched
    partials: Dict[:class:`str`, :class:`str`]
        Mapping of partial vars to segment names.
    invalidate_after: :class:`int`
        Automatically invalidates entry after it is fetched n times
    """
    search_by: str
    exclude: Union[dict, set] = Field(default_factory=set)
    partials: Dict[str, str] = Field(default_factory=dict)
    invalidate_after: int = -1

    ezycore_internal__: dict = Field(default_factory=lambda: {'n_fetch': 0})


class Model(BaseModel):
    ezycore_internal__partials: tuple = None  # Renamed from __ezycore_partials__
    _config: Config

    @classmethod
    def _read_partials(cls) -> Iterator[str]:
        for k, v in cls.__annotations__.items():
            if hasattr(v, '__origin__') and v.__origin__ is PartialRef:
                if issubclass(v.__args__[0], Model):
                    yield k
                else:
                    raise ValueError(f'Invalid model provided for partial definition: {k}')

    @classmethod
    def _verify_partials(cls) -> None:
        partials = tuple(cls._read_partials())
        cls.ezycore_internal__partials = partials  # Updated to use the new name

        defined_partials = cls._config.partials

        missing = [i for i in partials if i not in defined_partials]
        if missing:
            raise ValueError(f'Missing partial definitions for: {", ".join(missing)}')

    @classmethod
    def __init_subclass__(cls, **kwds) -> None:
        super().__init_subclass__(**kwds)
        try:
            r = getattr(cls, '_config')
        except AttributeError as err:
            raise ModalMissingConfig('_config variable not found') from err

        if isinstance(r, dict):
            setattr(cls, '_config', Config(**r)) 
        else:
            assert isinstance(r, Config), 'Invalid config class provided'
        cls._verify_partials()


M = TypeVar('M', dict, Model)
_M = TypeVar('_M', bound=Model)


class PartialRef(Generic[_M]):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v: Any, field: Any):
        type_: Model = field.outer_type_.__args__[0]

        if isinstance(v, type_):
            return v

        primary_key: str = type_._config.search_by

        primary_field = type_.__fields__[primary_key]
        valid_value, err = primary_field.validate(v, {}, loc=primary_key)
        if err:
            raise ValidationError([err], cls)

        return valid_value

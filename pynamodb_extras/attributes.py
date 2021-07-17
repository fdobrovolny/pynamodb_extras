from typing import Any, Callable, Optional, TypeVar, Union

import ulid
from pynamodb.attributes import UnicodeAttribute
from pynamodb.models import Model

_T = TypeVar("_T")


class ULIDAttributeMixin:
    """
    Attribute for storing ULID.
    """

    def serialize(self, value: ulid.ulid) -> str:
        return super().serialize(value.str)

    def deserialize(self, value: str) -> ulid.ulid:
        return ulid.parse(super().deserialize(value))


class PrefixedUnicodeAttribute(UnicodeAttribute):
    """
    Unicode Attribute with static prefix.
    """

    prefix: str = None

    def __init__(self, prefix, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prefix = prefix

    def serialize(self, value):
        if value is not None:
            return self.prefix + (super().serialize(value) if value != "" else value)
        return value

    def deserialize(self, value):
        if value is None:
            return value
        elif value.startswith(self.prefix):
            return value[len(self.prefix) :]
        else:
            raise AttributeError(
                f"Prefix {self.prefix} was not found during deserialization in value '{str(value)}'"
            )


class PrefixedULIDAttribute(ULIDAttributeMixin, PrefixedUnicodeAttribute):
    pass


class ULIDAttribute(ULIDAttributeMixin, UnicodeAttribute):
    pass


class StaticUnicodeAttribute(UnicodeAttribute):
    """
    Static attribute
    """

    static_value: str

    def __init__(self, static_value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.static_value = static_value
        self.default_for_new = static_value

    def serialize(self, value: str) -> str:
        if value != self.static_value:
            raise ValueError(
                f"Static value {self.static_value} does not match '{value}'."
            )

        return super().serialize(self.static_value)

    def deserialize(self, value: str) -> str:
        if value != self.static_value:
            raise ValueError(
                f"Static value {self.static_value} does not match '{value}'."
            )

        return super().deserialize(self.static_value)


class SourcedAttributeMixin:
    """
    Attribute Mixin to be able to base this field value based on another field.

    Attributes:
        source - name of attribute you want to use it's value or
                 callable which gets ModelInstance (during get() if this attribute
                 is range_key ModelInstance will be None), current value
                 if any exists and this attribute instance.
        only_default - Use source value only if current value is None. Default False
        source_hash_key - In case this attribute is range_key if True and call
                                    will not have range key specified setting this to true
                                    will automatically calculate it's value
    """

    source: Union[
        str, Callable[[Any, Optional[Model], "SourcedAttributeMixin"], Any], None
    ]
    source_hash_key: bool = False
    only_default: bool = False

    def __init__(
        self,
        source=None,
        only_default=False,
        source_hash_key=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if source_hash_key is False and source is None:
            raise ValueError("source can not be null if source_hash_key is False.")
        self.source = source
        self.only_default = only_default
        self.source_hash_key = source_hash_key

    def get_source_value_old(self, obj: Model, value=None):
        if self.only_default and value is not None:
            return value
        if self.source is None and self.source_hash_key:
            return obj.get_attributes()[obj._hash_keyname].serialize(
                getattr(obj, obj._hash_keyname, "")
            )
        if callable(self.source):
            return self.source(value, obj, self)
        return obj.get_attributes()[self.source].serialize(getattr(obj, self.source))

    def get_source_value(self, obj: Model, value=None):
        if self.only_default and value is not None:
            return value
        if self.source is None and self.source_hash_key:
            return getattr(obj, obj._hash_keyname, "")
        if callable(self.source):
            return self.source(value, obj, self)
        return getattr(obj, self.source)


class SourcedUnicodeAttribute(UnicodeAttribute, SourcedAttributeMixin):
    pass


class SourcedULIDAttribute(ULIDAttribute, SourcedAttributeMixin):
    pass


class SourcedPrefixedUnicodeAttribute(SourcedAttributeMixin, PrefixedUnicodeAttribute):
    pass


class SourcedPrefixedULIDAttribute(SourcedAttributeMixin, PrefixedULIDAttribute):
    pass

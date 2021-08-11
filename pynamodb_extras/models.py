import json
from typing import Dict, Any, List, Union

from pynamodb.attributes import Attribute, UnicodeAttribute, MapAttribute
from pynamodb.exceptions import AttributeNullError
from pynamodb.models import Model

from pynamodb_extras.attributes import SourcedAttributeMixin


class ExtrasModel(Model):
    # Use the following attributes to be bale to overwrite hash and range key on different entities
    _base_hash_keyname: str
    _base_range_keyname: str

    _dict_serialize_fields: Union[List[str], str]
    _dict_serialize_exclude: List[str]

    def _container_serialize(self, *args, **kwargs):
        """
        Injects Source attribute initialization to serialization.
        """
        for name, attr in self.get_attributes().items():
            if isinstance(attr, SourcedAttributeMixin):
                setattr(self, name, attr.get_source_value(self, getattr(self, name)))

        return super()._container_serialize(*args, **kwargs)

    def _set_attributes(self, **kwargs):
        """
        This function is called when a new object is initialized via __init__

        If some sourced value is based on value with default this will initialize it at model instance creation.
        """
        super()._set_attributes(**kwargs)

        """
        for name, attr in [
            (name, attr)
            for name, attr in self.get_attributes().items()
            if isinstance(attr, SourcedAttributeMixin)
        ]:
            setattr(self, name, attr.get_value(self, getattr(self, name, "")))
            """

    @classmethod
    def _serialize_keys(cls, hash_key, range_key=None):
        serialized_hash_key, serialized_range_key = super()._serialize_keys(
            hash_key, range_key
        )

        range_key_attr: Attribute = cls._range_key_attribute()
        if range_key_attr is not None and serialized_range_key is None:
            if isinstance(range_key_attr, SourcedAttributeMixin):
                if not range_key_attr.only_default and range_key_attr.source_hash_key is True:
                    serialized_range_key = range_key_attr.serialize(hash_key)

        return serialized_hash_key, serialized_range_key

    @classmethod
    def _hash_key_attribute(cls):
        """
        Returns the attribute class for the hash key
        """
        hash_key_attribute = super()._hash_key_attribute()
        if hash_key_attribute is None and hasattr(cls, "_base_hash_keyname"):
            return UnicodeAttribute(hash_key=True, attr_name=cls._base_hash_keyname)
        return hash_key_attribute

    @classmethod
    def _range_key_attribute(cls):
        """
        Returns the attribute class for the range key
        """
        range_key_attribute = super()._range_key_attribute()
        if range_key_attribute is None and hasattr(cls, "_base_range_keyname"):
            return UnicodeAttribute(range_key=True, attr_name=cls._base_range_keyname)
        return range_key_attribute

    def dict_serialize(self, null_check: bool = True, use_python_names: bool = True) -> Dict[str, Any]:
        """
        Serialize attribute values into dictionary
        """
        if hasattr(self, "_dict_serialize_fields") and hasattr(self, "_dict_serialize_exclude"):
            raise AttributeError("`_dict_serialize_fields` and `_dict_serialize_exclude` are mutually exclusive")

        attribute_values: Dict[str, Dict[str, Any]] = {}
        for name, attr in self.get_attributes().items():
            if name in getattr(self, "_dict_serialize_exclude", []) or (
                    "__all__" != getattr(
                self, "_dict_serialize_fields", "__all__"
            ) and name not in getattr(self, "_dict_serialize_fields", "__all__")
            ) or (
                    "__all__" == getattr(
                self, "_dict_serialize_fields", "__all__"
            ) and name.startswith("_")):
                continue
            value = getattr(self, name)
            try:
                if isinstance(value, MapAttribute) and not value.validate(null_check=null_check):
                    raise ValueError("Attribute '{}' is not correctly typed".format(name))
            except AttributeNullError as e:
                e.prepend_path(name)
                raise

            if value is not None:
                if isinstance(attr, MapAttribute):
                    attr_value = attr.serialize(value, null_check=null_check)
                else:
                    attr_value = attr.serialize(value)
            else:
                attr_value = None
            if null_check and attr_value is None and not attr.null:
                raise AttributeNullError(name)

            attribute_values[name if use_python_names else attr.attr_name] = attr_value
        return attribute_values

    def json(self, null_check: bool = True, use_python_names: bool = True):
        """
        Serialize attribute values into json.
        """
        return json.dumps(self.dict_serialize(null_check, use_python_names))

from pynamodb.attributes import Attribute
from pynamodb.models import Model

from pynamodb_extras.attributes import SourcedAttributeMixin


class ExtrasModel(Model):
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
                if range_key_attr.only_default:
                    return serialized_hash_key, range_key
                if range_key_attr.source_hash_key is True:
                    return serialized_hash_key, range_key_attr.serialize(hash_key)
                elif callable(range_key_attr.source):
                    return (
                        serialized_hash_key,
                        range_key_attr.source(hash_key, None, range_key_attr),
                    )

        return serialized_hash_key, serialized_range_key

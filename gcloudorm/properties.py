import cPickle as pickle
import datetime
import json
import six
import uuid
import zlib


class Property(object):
    """
    The base for model properties. A property has a value, can have validators and a default value. Useful attributes of
    a property include:

    * :attr:`name` the name of this property.
    * :attr:`indexed` whether this property is indexed
    * :attr:`is_id` is this property being used as the id for a model?

    This class shouldn't be used directly. Instead, it is intended to be extended by concrete property implementations.
    """
    def __init__(self, indexed=True, repeated=False, required=False, default=None, choices=None, validator=None,
                 key_id=False):
        """
        Initialise a property.

        :param bool indexed: should this field be indexed? Defaults to True.
        :param bool repeated: is the property repeated (a list of other properties)? Defaults to False.
        :param bool required: is this property required? Defaults to False.
        :param default: what is the default value of this property? If this is a callable it will be invoked to retreive
        the default value.
        :param list choices: a list of values this property should have.
        :param func validator: a function to use for validation.
        :param bool key_id: is this property to be used as the id for the model? Defaults to False.
        """
        self._name = None
        self._indexed = indexed
        self._repeated = repeated
        self._required = required
        self._default = default
        self._choices = choices
        self._validator = validator
        self._is_id = key_id

    def __get__(self, instance, owner):
        if self._repeated:
            if self._name not in instance:
                self.__set__(instance, self._default or [])

            return [self.from_base_type(k) for k in instance[self._name]]

        if self._name not in instance:
            if callable(self._default):
                self.__set__(instance, self._default())
            else:
                self.__set__(instance, self._default)

        return self.from_base_type(instance[self._name])

    def __set__(self, instance, value):
        if self._repeated:
            assert isinstance(value, (tuple, list)), "Repeated property only accept list or tuple"
            value = [self.validate(k) for k in value]
            instance[self._name] = [self.to_base_type(k) for k in value]
        else:
            value = self.validate(value)
            instance[self._name] = self.to_base_type(value)

    def __delete__(self, instance):
        instance.pop(self._name, None)

    @property
    def name(self):
        return self.name

    @property
    def indexed(self):
        return self._indexed

    @property
    def is_id(self):
        return self._is_id

    def validate(self, value):
        """
        Validate value for use as the value for this property.

        :param value: the value to be tested.
        :return: the value if the checks pass, otherwise None.
        """
        assert self._choices is None or value in self._choices
        assert not (self._required and value is not None)
        if value is None:
            return

        value = self._validate(value)
        if self._validator is not None:
            return self._validator(self, value)

        return value

    def from_base_type(self, value):
        """
        Do any conversion required on value to convert it from an internal property value to one suitable for external
        use. An example might be decompressing the value for property that supports compression. Conversion back to the
        internal representation can be done using :func:`to_base_type`.

        :param value: the value to convert.
        :return: the external representation of the value.
        """
        if value is None:
            return value
        return self._from_base_type(value)

    def to_base_type(self, value):
        """
        Convert value into the format used internally by this property. An example might be a property that stores
        itself in a compressed format performing compression on value. Conversion back to the external representation '
        can be done via :func:`from_base_type`.

        :param value: the value to convert.
        :return: the internal representation of value.
        """
        if value is None:
            return value
        return self._to_base_type(value)

    def _fix_up(self, cls, name):
        self._name = name

    def _to_base_type(self, value):
        return value

    def _from_base_type(self, value):
        return value

    def _validate(self, value):
        return value


class BooleanProperty(Property):
    """A bool property."""
    def _validate(self, value):
        assert isinstance(value, bool)
        return value


class IdProperty(Property):
    """An auto generated id property that uses uuid4 for its value."""
    def __init__(self, key_id=True):
        """
        Initialise this property. Default behaviour is to set key_id to True.

        :param bool key_id: is this property the key_id?  Defaults to True.
        """
        super(IdProperty, self).__init__(default=lambda: uuid.uuid4().hex, indexed=True, key_id=key_id)

    def _validate(self, value):
        assert isinstance(value, six.string_types) or isinstance(value, int)
        return value


class IntegerProperty(Property):
    """An int property."""
    def _validate(self, value):
        assert isinstance(value, (int, long))
        return int(value)


class FloatProperty(Property):
    """A float property."""
    def _validate(self, value):
        assert isinstance(value, (int, long, float))
        return float(value)


class BlobProperty(Property):
    """Store data as bytes. Supports compression."""
    def __init__(self, compressed=False, **kwargs):
        """
        Initialise this property. Has an option to compress using zlib that defaults to False. **Note** that this
        property can't be compressed and indexed!

        :param bool compressed: should this property store its value compressed? Defaults to False.
        """
        kwargs.pop('indexed', None)
        super(BlobProperty, self).__init__(indexed=False, **kwargs)

        self._compressed = compressed
        assert not (compressed and self._indexed), \
            "BlobProperty %s cannot be compressed and indexed at the same time." % self._name

    def _validate(self, value):
        assert isinstance(value, str), value
        return value

    def _to_base_type(self, value):
        if self._compressed:
            return zlib.compress(value)

        return value

    def _from_base_type(self, value):
        if self._compressed:
            return zlib.decompress(value)

        return value


class TextProperty(BlobProperty):
    """Store data as unicode."""
    def __init__(self, indexed=False, **kwargs):
        super(TextProperty, self).__init__(indexed=indexed, **kwargs)

    def _validate(self, value):
        if isinstance(value, str):
            value = value.decode('utf-8')

        assert isinstance(value, unicode)
        return value

    def _to_base_type(self, value):
        if isinstance(value, str):
            return value.decode('utf-8')

        return value

    def _from_base_type(self, value):
        if isinstance(value, str):
            return unicode(value, 'utf-8')
        elif isinstance(value, unicode):
            return value


class PickleProperty(BlobProperty):
    """Store data as pickle. Takes care of (un)pickling."""
    def _to_base_type(self, value):
        return super(PickleProperty, self)._to_base_type(pickle.dumps(value, pickle.HIGHEST_PROTOCOL))

    def _from_base_type(self, value):
        return pickle.loads(super(PickleProperty, self)._from_base_type(value))

    def _validate(self, value):
        return value


class JsonProperty(BlobProperty):
    """Store data as JSON. Takes care of conversion to/from JSON."""
    def __init__(self, name=None, schema=None, **kwargs):
        super(JsonProperty, self).__init__(name, **kwargs)
        self._schema = schema

    def _to_base_type(self, value):
        return super(JsonProperty, self)._to_base_type(json.dumps(value))

    def _from_base_type(self, value):
        return json.loads(super(JsonProperty, self)._from_base_type(value))

    def _validate(self, value):
        return value


class DateTimeProperty(Property):
    """Store data as a timestamp represented as datetime.datetime."""
    def __init__(self, name=None, auto_now_add=False, auto_now=False, **kwargs):
        assert not ((auto_now_add or auto_now) and kwargs.get("repeated", False))
        super(DateTimeProperty, self).__init__(name, **kwargs)
        self._auto_now_add = auto_now_add
        self._auto_now = auto_now

    def _validate(self, value):
        assert isinstance(value, datetime.datetime), value
        return value

    def _now(self):
        return datetime.datetime.utcnow()

    def _prepare_for_put(self, entity):
        v = getattr(entity, self._name)
        if v is None and self._auto_now_add:
            setattr(entity, self._name, self._now())

        if self._auto_now:
            setattr(entity, self._name, self._now())


class DateProperty(DateTimeProperty):
    """Store data as a date and represented as datetime.date."""
    def _validate(self, value):
        assert isinstance(value, datetime.date)
        return value

    def _to_base_type(self, value):
        return datetime.datetime(value.year, value.month, value.day)

    def _from_base_type(self, value):
        return value.date()

    def _now(self):
        return datetime.datetime.utcnow().date()


class TimeProperty(DateTimeProperty):
    """Store data as time represented using datetime.time."""
    def _validate(self, value):
        assert isinstance(value, datetime.time)
        return value

    def _to_base_type(self, value):
        return datetime.datetime(
            1970, 1, 1,
            value.hour, value.minute, value.second,
            value.microsecond
        )

    def _from_base_type(self, value):
        return value.time()



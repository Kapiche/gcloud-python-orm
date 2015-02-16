import cPickle as pickle
import datetime
import json
import zlib

from gcloud.datastore import api, entity, key


class Property(object):
    """
    A property of a model.
    """
    def __init__(self, indexed=True, repeated=False, required=False, default=None, choices=None, validator=None):
        self._name = None
        self._indexed = indexed
        self._repeated = repeated
        self._required = required
        self._default = default
        self._choices = choices
        self._validator = validator

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

    def validate(self, value):
        assert self._choices is None or value in self._choices
        assert not (self._required and value is not None)
        if value is None:
            return

        if self._validator is not None:
            return self._validator(self, value)

        return value

    def from_base_type(self, value):
        if value is None:
            return value
        return self._from_base_type(value)

    def _fix_up(self, cls, name):
        self._name = name

    def to_base_type(self, value):
        if value is None:
            return value
        return self._to_base_type(value)

    def _to_base_type(self, value):
        return value

    def _from_base_type(self, value):
        return value


class BooleanProperty(Property):
    def _validate(self, value):
        assert isinstance(value, bool)
        return value


class IntegerProperty(Property):
    def _validate(self, value):
        assert isinstance(value, (int, long))
        return int(value)


class FloatProperty(Property):
    def _validate(self, value):
        assert isinstance(value, (int, long, float))
        return float(value)


class BlobProperty(Property):
    def __init__(self, compressed=False, **kwargs):
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
            return zlib.decompress(value.z_val)

        return value


class TextProperty(BlobProperty):
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

    def _from_db_value(self, value):
        if isinstance(value, str):
            return value.decode('utf-8')

        return value


class StringProperty(TextProperty):
    def __init__(self, indexed=True, **kwargs):
        super(StringProperty, self).__init__(indexed=indexed, **kwargs)


class PickleProperty(BlobProperty):
    def _to_base_type(self, value):
        return super(PickleProperty, self)._to_base_type(pickle.dumps(value, pickle.HIGHEST_PROTOCOL))

    def _from_base_type(self, value):
        return pickle.loads(super(PickleProperty, self)._from_base_type(value))

    def _validate(self, value):
        return value


class JsonProperty(BlobProperty):
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


class MetaModel(type):
    def __init__(cls, name, bases, classdict):
        super(MetaModel, cls).__init__(name, bases, classdict)
        cls._fix_up_properties()


class Model(entity.Entity):
    """
    A Model is just a :class:`gcloud.datastore.entity.Entity`. The kind of the Entity is the name of the class that
    extends Model (``self.__class__.__name__``).

    A Model has properties as declared on the class itself. For example:

        from gcloudorm import model

        class Person(model.Model):
            name = model.TextProperty(indexed=False)
            dob = model.DateProperty()

    To save/update an entity, call :func:`.put` on it. To fetch an entity, call :func:`.get`.

    """
    __metaclass__ = MetaModel

    # name, prop dict
    _properties = None
    _kind_map = {}

    _model_exclude_from_indexes = None

    def __init__(self, id=None, parent=None, **kwargs):
        """
        Create a new instance of the model.

        Underneath, this creates the gcloud Entity and Key for that entity.

        :param id: Used in the id part of the Key for this underlying gcloud Entity.
        :param parent: A :class:`gcloud.datastore.key.Key` to use as a parent.
        :param kwargs: The value of the properties for this model. Unrecognised properties are ignored.
        """
        # Determine our key.
        if not id and 'id' in self._properties and self._properties['id']._default:
            prop = self._properties['id']
            if isinstance(prop, TextProperty) or isinstance(prop, IntegerProperty):
                default = prop._default if not callable(prop._default) else prop._default()
                id = prop.to_base_type(prop.validate(default))
        if id:
            if isinstance(parent, key.Key):
                self._key = key.Key(self.__class__.__name__, id, parent=parent)
            else:
                self._key = key.Key(self.__class__.__name__, id)
        else:
            if isinstance(parent, key.Key):
                self._key = key.Key(self.__class__.__name__, parent=parent)
            else:
                self._key = key.Key(self.__class__.__name__)
        super(Model, self).__init__(self._key, exclude_from_indexes=self._model_exclude_from_indexes)

        # Set our properties
        for attr in self._properties:
            setattr(self, attr, getattr(self, attr))

        for name in kwargs:
            if name in self._properties:  # Don't store random properties
                setattr(self, name, kwargs[name])

    @classmethod
    def _fix_up_properties(cls):
        cls._properties = {}
        cls._model_exclude_from_indexes = set()

        for name, attr in cls.__dict__.items():
            if isinstance(attr, Property):
                attr._fix_up(cls, name)
                cls._properties[name] = attr
                if not attr.indexed:
                    cls._model_exclude_from_indexes.add(name)

        cls._kind_map[cls.__name__] = cls

    @classmethod
    def _lookup_model(cls, kind):
        return cls._kind_map[kind]

    def __repr__(self):
        if self._key:
            return "<%s%s %s>" % (
                self.__class__.__name__,
                self._key.path,
                super(Model, self).__repr__()
            )
        else:
            return "<%s %s>" % (
                self.__class__.__name__,
                super(Model, self).__repr__()
            )

    @classmethod
    def from_entity(cls, e):
        obj = cls()
        obj._key = e.key

        for name, prop in cls._properties.items():
            obj[name] = prop.from_db_value(e.get(name))

        return obj

    @classmethod
    def get_by_id(cls, id):
        """
        Get the entity identified by id.

        :param id: The id of the entity to fetch
        :return: The model instance.
        """
        e = api.get([key.Key(cls.__name__, id)])
        if e:
            return cls.from_entity(entity)

    @classmethod
    def filter(cls, ids):
        """
        Get the entities identified by ids.

        :param ids: The ids to fetch.
        :return:
        """
        entities = api.get([key.Key(cls.__name__, i) for i in ids])
        return [cls.from_entity(e) for e in entities if e]

    def save(self):
        return api.put([self])

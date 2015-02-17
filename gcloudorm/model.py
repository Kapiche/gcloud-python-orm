from __future__ import absolute_import

from gcloud.datastore import api, entity, key

from .properties import IdProperty, IntegerProperty, Property, TextProperty


class ObjectDoesNotExist(Exception):
    """Couldn't fetch an entity by id."""


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
    _id_prop = None

    def __init__(self, parent=None, **kwargs):
        """
        Create a new instance of the model.

        Underneath, this creates the gcloud Entity and Key for that entity.

        :param id: Used in the id part of the Key for this underlying gcloud Entity.
        :param parent: A :class:`gcloud.datastore.key.Key` to use as a parent.
        :param kwargs: The value of the properties for this model. Unrecognised properties are ignored.
        """
        # Determine our key.
        if not self._id_prop:
            if 'id' in self._properties:
                if not isinstance(self._properties['id'], (IdProperty, IntegerProperty, TextProperty)):
                    raise TypeError("You haven't specified a key_id property and have included an id property that "
                                    "isn't a suitable type to be used as the key_id.")
                self._properties['id']._is_id = True
            else:
                self._properties['id'] = IdProperty(key_id=True)
            self._id_prop = 'id'

        # Figure out the id value
        id_prop = self._properties[self._id_prop]
        id_value = kwargs.get(self._id_prop, None) or \
                   (id_prop._default() if callable(id_prop._default) else id_prop._default)
        if not id_value:
            raise ValueError('You need to specify a value for your key_id property or use a default value.')

        # Create the key now we know our id
        id_value = id_prop.to_base_type(id_prop.validate(id_value))  # Make sure id is in the right form for storage
        if isinstance(parent, key.Key):
            self._key = key.Key(self.__class__.__name__, id_value, parent=parent)
        else:
            self._key = key.Key(self.__class__.__name__, id_value)
        super(Model, self).__init__(self._key, exclude_from_indexes=self._model_exclude_from_indexes)

        # Set our properties
        for attr in self._properties:
            if attr == self._id_prop:
                setattr(self, attr, id_value)  # Functionality of getattr has already been called for key_id prop
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
                if attr.is_id:
                    if cls._id_prop:
                        raise ValueError('You have specified more then one key_id property.')
                    if not isinstance(attr, (TextProperty, IntegerProperty, IdProperty,)):
                        raise TypeError("The field you've marked as key_id isn't one of TextProperty, "
                                        "IntegerProperty or IdProperty.")
                    cls._id_prop = name

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
        obj = cls(id=e.key.id_or_name)
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
            return cls.from_entity(e[0])
        raise ObjectDoesNotExist

    @classmethod
    def filter(cls, ids):
        """
        Get the entities identified by ids.

        :param ids: The ids to fetch.
        :return: a list of Model instances
        """
        entities = api.get([key.Key(cls.__name__, i) for i in ids])
        return [cls.from_entity(e) for e in entities if e]

    def save(self):
        return api.put([self])

    def delete(self):
        return api.delete([self._key])

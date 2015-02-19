import six
import unittest2

from gcloud.datastore import helpers, key, set_default_dataset_id, set_default_connection

from gcloudorm import model, properties


class TestModel(unittest2.TestCase):
    def setUp(self):
        set_default_dataset_id(_DATASET_ID)

    def testModel(self):
        p = key.Key('ParentModel', 'foo')

        # key auto id (name)
        m = model.Model()
        self.assertEqual(m.key.id_or_name, m.id)
        self.assertEqual(m.key.kind, model.Model.__name__)
        # + parent
        m = model.Model(parent=p)
        self.assertEqual(m.key.path, key.Key('ParentModel', 'foo', 'Model', m.id).path)

        # name from field with default
        class TestModel(model.Model):
            id = properties.TextProperty(default="abc", key_id=True)
        m = TestModel()
        self.assertEqual(m.key.id_or_name, "abc")
        self.assertEqual(m.id, "abc")
        # pass name
        m = TestModel(id='cba')
        self.assertEqual(m.key.id_or_name, "cba")
        self.assertEqual(m.id, "cba")
        # + parent
        m = TestModel(parent=p)
        self.assertEqual(m.key.path, key.Key('ParentModel', 'foo', 'TestModel', m.id).path)
        # pass + parent
        m = TestModel(id='cba', parent=p)
        self.assertEqual(m.key.path, key.Key('ParentModel', 'foo', 'TestModel', 'cba').path)

        # name from field with callable default
        class TestModel(model.Model):
            id = model.TextProperty(default=lambda: "abc", key_id=True)
        m = TestModel()
        self.assertEqual(m.key.id_or_name, "abc")
        self.assertEqual(m.id, "abc")

        # name from int field with callable default
        class TestModel(model.Model):
            id = model.IntegerProperty(default=lambda: 111, key_id=True)
        m = TestModel()
        self.assertEqual(m.key.id_or_name, 111)
        self.assertEqual(m.id, 111)

        # name from IdProeprty
        class TestModel(model.Model):
            the_id = model.IdProperty(key_id=True)

    def testInsert(self):
        connection = _Connection()
        transaction = connection._transaction = _Transaction()
        dataset = _Dataset(connection)
        key = _Key()
        set_default_connection(connection)

        class TestModel(model.Model):
            test_value = properties.TextProperty()

        entity = TestModel()
        entity.key = key
        entity.test_value = '123'
        entity.save()

        self.assertEqual(entity['test_value'], '123')
        self.assertEqual(connection._saved, (_DATASET_ID, 'KEY', {'test_value': '123'}, ()))
        self.assertEqual(key._path, None)


_MARKER = object()
_DATASET_ID = 'DATASET'
_KIND = 'KIND'
_ID = 1234
helpers._prepare_key_for_request = lambda x: x


class _Key(object):
    _MARKER = object()
    _key = 'KEY'
    _partial = False
    _path = None

    def to_protobuf(self):
        return self._key

    def is_partial(self):
        return self._partial

    def path(self, path=_MARKER):
        if path is self._MARKER:
            return self._path
        self._path = path

    @property
    def dataset_id(self):
        return _DATASET_ID


class _Dataset(dict):
    def __init__(self, connection=None):
        super(_Dataset, self).__init__()
        self._connection = connection

    def id(self):
        return _DATASET_ID

    def connection(self):
        return self._connection

    def get_entity(self, key):
        return self.get(key)


class _Connection(object):
    _transaction = _saved = _deleted = None
    _save_result = True

    def transaction(self):
        return self._transaction

    def save_entity(self, dataset_id, key_pb, properties,
                    exclude_from_indexes=()):
        self._saved = (dataset_id, key_pb, properties,
                       tuple(exclude_from_indexes))
        return self._save_result

    def delete_entities(self, dataset_id, key_pbs):
        self._deleted = (dataset_id, key_pbs)


class _Transaction(object):
    _added = ()

    def __nonzero__(self):
        return True
    __bool__ = __nonzero__

    def add_auto_id_entity(self, entity):
        self._added += (entity,)

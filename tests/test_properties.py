import six
import unittest2

from gcloud.datastore import helpers, key, set_default_dataset_id

from gcloudorm import model, properties


class TestProperties(unittest2.TestCase):
    _DATASET_ID = 'DATASET'

    def setUp(self):
        set_default_dataset_id(self._DATASET_ID)

    def testBooleanProperty(self):
        class TestModel(model.Model):
            test_bool = properties.BooleanProperty()

        m = TestModel()
        self.assertEqual(m.test_bool, None)
        self.assertEqual(m['test_bool'], None)

        m = TestModel(test_bool=False)
        self.assertEqual(m.test_bool, False)
        self.assertEqual(m['test_bool'], False)

        m.test_bool = True
        self.assertEqual(m.test_bool, True)
        self.assertEqual(m['test_bool'], True)

        class TestModel(model.Model):
            test_bool = properties.BooleanProperty(default=True)

        m = TestModel()
        self.assertEqual(m.test_bool, True)
        self.assertEqual(m['test_bool'], True)

    def testIdProperty(self):
        class TestModel(model.Model):
            test_id = properties.IdProperty()

        m = TestModel()
        self.assertIsInstance(m.test_id, six.string_types)
        self.assertIs(m.test_id, m.key.id_or_name)

    def testIntegerProperty(self):
        class TestModel(model.Model):
            test_int = properties.IntegerProperty()

        m = TestModel()
        self.assertEqual(m.test_int, None)
        self.assertEqual(m['test_int'], None)

        class TestModel(model.Model):
            test_int = properties.IntegerProperty(default=3)

        m = TestModel()
        self.assertEqual(m['test_int'], 3)

        m.test_int = 4
        self.assertEqual(m.test_int, 4)
        self.assertEqual(m['test_int'], 4)

    def testFloatproperty(self):
        class TestModel(model.Model):
            test_float = properties.FloatProperty()

        m = TestModel()
        self.assertEqual(m.test_float, None)
        self.assertEqual(m['test_float'], None)

        class TestModel(model.Model):
            test_float = properties.FloatProperty(default=0.1)

        m = TestModel()
        self.assertEqual(m['test_float'], 0.1)

        m.test_float = 0.2
        self.assertEqual(m['test_float'], 0.2)

    def testTextProperty(self):
        class TestModel(model.Model):
            test_text = properties.TextProperty()

        m = TestModel()
        self.assertEqual(m.test_text, None)

        class TestModel(model.Model):
            test_text = properties.TextProperty(default="")

        m = TestModel()
        self.assertEqual(m['test_text'], "")

        class TestModel(model.Model):
            test_text = properties.TextProperty(default=lambda: "")

        m = TestModel()
        self.assertEqual(m['test_text'], "")

    def testPickleProperty(self):
        class TestModel(model.Model):
            test_pickle = properties.PickleProperty()

        m = TestModel()
        self.assertEqual(m.test_pickle, None)
        m = TestModel(test_pickle={"123": "456"})
        self.assertEqual(m.test_pickle, {"123": "456"})

        m.test_pickle = {'456': '789'}
        self.assertEqual(m.test_pickle, {'456': '789'})

    def testJsonProperty(self):
        class TestModel(model.Model):
            test_pickle = properties.JsonProperty()

        m = TestModel()
        self.assertEqual(m.test_pickle, None)
        m = TestModel(test_pickle={"123": "456"})
        self.assertEqual(m.test_pickle, {"123": "456"})

        m.test_pickle = {'456': '789'}
        self.assertEqual(m.test_pickle, {'456': '789'})

    def testDataTimeProperty(self):
        import datetime

        class TestModel(model.Model):
            test_datetime = properties.DateTimeProperty()

        m = TestModel()
        self.assertEqual(m.test_datetime, None)

        utcnow = datetime.datetime.utcnow()
        m.test_datetime = utcnow
        self.assertEqual(m.test_datetime, utcnow)

    def testDateProperty(self):
        import datetime

        class TestModel(model.Model):
            test_date = properties.DateProperty()

        m = TestModel()
        self.assertEqual(m.test_date, None)

        today = datetime.date.today()
        m.test_date = today
        self.assertEqual(m.test_date, today)

    def testTimeProperty(self):
        import datetime

        class TestModel(model.Model):
            test_time = properties.TimeProperty()

        m = TestModel()
        self.assertEqual(m.test_time, None)

        t = datetime.time()
        m.test_time = t

        self.assertEqual(m.test_time, t)

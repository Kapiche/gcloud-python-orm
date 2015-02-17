gcloud-python-orm
=================
ndb/Django like ORM support based on [gcloud-python](https://github.com/GoogleCloudPlatform/gcloud-python) datastore.

[![Build Status](https://travis-ci.org/lucemia/gcloud-python-orm.svg?branch=master)](https://travis-ci.org/lucemia/gcloud-python-orm)

Usage
-----
Create a Model with some Properties and away you go.

    from gcloudorm import model, properties
    
    class Person(model.Model):
        name = properties.TextProperty()
        age = properties.IntegerProperty(default=15)
        
    p = Person(name="Alice", age=21)
    p.save()
    
    alice = Person.get_by_id(p.id)
    
For the above code to work, you will need to have access to Google Datastore and gcloud-python will need to be 
configured to use it (using ``datastore.set_defaults()`` or similar).
    
Django Specific Notes
---------------------
There is no specific middleware required by this library. This should be a fairly straight replacement for the existing
Model API in Django. The API specifics have changed slightly but it is very functional.

Don't forget that you need to configure gcloud-python with a dataset id and project id. That can ususally be done from
the settings.py file or at the top of your views.py file.

TODO
----
* Expand the query support via ``Model.filter()``
* Add better tests for ``Model.save()``

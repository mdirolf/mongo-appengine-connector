#!/usr/bin/env python
#
# Copyright 2008-2009 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from google.appengine.api import users
from google.appengine.ext import db
from google.appengine.api import datastore

import datetime
import time
import types

print 'Content-Type: text/html'
print ''
print '<html><head><body>'

print '<strong>Datastore API</strong><br/>'
print 'Test a simple db example...<br/>'
class Person(db.Model):
    name = db.StringProperty(required=True)

for result in Person.all().fetch(1000):
    result.delete()

test = Person(name="Mike")
key = test.put()
result = Person.get(key)
assert result.name == "Mike"

print 'Test that ids get incremented properly between sessions...<br/>'
class Blah(db.Model):
    something = db.StringProperty()

# NOT erasing old data here

prev_count = Blah.all().count()
Blah().put()
Blah(something="hello").put()
assert Blah.all().count() == prev_count + 2

print 'Slightly less simple db test...<br/>'
class Pet(db.Model):
    name = db.StringProperty(required=True)
    type = db.StringProperty(required=True, choices=set(["cat", "dog", "bird"]))
    birthdate = db.DateProperty()
    weight_in_pounds = db.IntegerProperty()
    spayed_or_neutered = db.BooleanProperty()

for result in Pet.all().fetch(1000):
    result.delete()

pet = Pet(name="Fluffy", type="cat")
pet.weight_in_pounds = 24
key = db.put(pet)
result = db.get(key)
assert result.name == "Fluffy"
assert result.type == "cat"
assert result.weight_in_pounds == 24
assert result.birthdate == None
assert result.spayed_or_neutered == None

print 'Test db exceptions...<br/>'
pet = Pet(name="Fluffy", type="cat")
try:
    pet.type = "mike"
    assert False
except db.BadValueError:
    pass
pet.type = "dog"
try:
    pet.name = None
    assert False
except db.BadValueError:
    pass
pet.name = "mike"
try:
    pet.weight_in_pounds = "hello"
    assert False
except db.BadValueError:
    pass
pet.weight_in_pounds = 30
try:
    pet.spayed_or_neutered = 24
    assert False
except db.BadValueError:
    pass
pet.spayed_or_neutered = False

print 'Test a delete...<br/>'
key = pet.put()
result = Pet.get(key)
assert result.name == "mike"
pet.delete()
assert pet.name == "mike"
result = db.get(key)
assert result == None

print 'Test a delete on an unsaved object...<br/>'
pet = Pet(name="Fluffy", type="cat")
try:
    pet.delete()
    assert False
except db.NotSavedError:
    pass
assert pet.name == "Fluffy"

print 'Test a date property...<br/>'
class Story(db.Model):
    title = db.StringProperty(required=True)
    created = db.DateTimeProperty(auto_now_add=True)

for result in Story.all().fetch(1000):
    result.delete()

story = Story(title="My Story")
key = story.put()
result = Story.get(key)
assert result.title == "My Story"
assert result.created.year in [2008, 2009, 2010]
result.delete()

print 'Test a simple query...<br/>'

s1 = Story(title="The Three Little Pigs")
db.put(s1)

time.sleep(0.25)
s2 = Story(title="Little Red Riding Hood")
s2.put()

time.sleep(0.25)
try:
    s = Story()
    assert False
except db.BadValueError:
    pass

s3 = Story(title="Winnie the Pooh")
s3.put()

assert s1.created < s2.created or s2.created < s3.created

query = Story.all()
query.order('-created')
s = query.get()
assert s.title == "Winnie the Pooh"
query = db.Query(Story)
query.order('created')
s = query.get()
assert s.title == "The Three Little Pigs"

print 'Test some different properties...<br/>'
class Article(db.Model):
    title = db.StringProperty(required=True, default="no title")
    content = db.TextProperty(required=True)
    tags = db.ListProperty(db.Category)
    author_mail = db.EmailProperty()
    link = db.LinkProperty(required=True)
    rating = db.RatingProperty()

for result in Article.all().fetch(1000):
    result.delete()

print '&nbsp;&nbsp;&nbsp;&nbsp;Validation<br/>'
try:
    art = Article()
    assert False
except db.BadValueError:
    pass
try:
    art = Article(content="some content")
    assert False
except db.BadValueError:
    pass
art = Article(content="some content", link="http://www.example.com")
assert art.title == 'no title'
try:
    art = Article(content="some content", link="not a link")
    assert False
except db.BadValueError:
    pass
art = Article(content="some content", link="http://www.example.com", author_mail="not an email")
try:
    art = Article(content="some content", link="http://www.example.com", author_mail="not an email", rating=101)
    assert False
except db.BadValueError:
    pass
art = Article(title="my title", content="some content", tags=[db.Category("awesome"), db.Category("super")],
              author_mail="test@example.com", link="http://www.example.com",
              rating=65)
assert art.title == "my title"
assert art.tags[1] == "super"
print '&nbsp;&nbsp;&nbsp;&nbsp;Put and Fetch<br/>'

art.put()
out = Article.all().fetch(10)[0]
assert art.title == out.title
assert art.content == out.content
assert art.tags == out.tags
assert art.author_mail == out.author_mail
assert art.link == out.link
assert art.rating == out.rating
print '&nbsp;&nbsp;&nbsp;&nbsp;Some more query orderings<br/>'
art = Article(title="a title", content="writing", tags=[db.Category("super")],
              author_mail="anothertest@example.com", link="http://www.10gen.com",
              rating=95)
art.put()
art = Article(title="some title", content="writing i wrote", tags=[db.Category("writing"), db.Category("other")],
              link="http://www.example.org",
              rating=70)
art.put()
assert Article.all().order('title').fetch(1, 1)[0].title == "my title"
try:
    print Article.all().order('title').fetch(1, 1)[1].title
    assert False
except IndexError:
    pass
assert Article.all().order('-title').fetch(5)[2].title == "a title"
assert Article.all().order('link').get().title == 'a title'
assert Article.all().order('-link').get().title == 'some title'
assert Article.all().order('rating').get().title == 'my title'
assert Article.all().order('-rating').get().title == 'a title'
assert Article.all().order('tags').get().title == 'my title'
assert Article.all().order('-tags').get().title == 'some title'

print 'Test a foreign key...<br/>'
class FirstModel(db.Model):
    prop = db.IntegerProperty()

for result in FirstModel.all().fetch(1000):
    result.delete()

try:
    class Bad(db.Model):
        ref_one = db.ReferenceProperty(FirstModel)
        ref_two = db.ReferenceProperty(FirstModel)
    assert False
except db.DuplicatePropertyError:
    pass

class SecondModel(db.Model):
    reference = db.ReferenceProperty(FirstModel)
    ref2 = db.ReferenceProperty(FirstModel, collection_name="testcollection")
    selfref = db.SelfReferenceProperty()

for result in SecondModel.all().fetch(1000):
    result.delete()

obj1 = FirstModel()
obj1.prop = 42
obj1.put()

obj2 = SecondModel()
obj2.reference = obj1.key()
obj2.ref2 = obj1
key2 = obj2.put()

obj3 = SecondModel()
obj3.reference = obj1
obj3.selfref = obj2
key3 = obj3.put()

obj4 = FirstModel()
obj4.prop = 6
obj4.put()

obj5 = SecondModel()
obj5.reference = obj4
obj5.selfref = obj3
key5 = obj5.put()

obj2.reference.prop = 999
obj2.reference.put()

assert FirstModel.all().count() == 2

assert db.get(key3).reference.prop == 999

assert db.get(key5).selfref.reference.prop == 999

assert db.get(obj1.key()).prop == 999

assert obj1.testcollection.count() == 1

assert obj1.secondmodel_set.count() == 2

assert isinstance(obj1.secondmodel_set[0], SecondModel)

print 'Test query counts...<br/>'
class CountTest(db.Model):
    prop = db.StringProperty()

for result in CountTest.all().fetch(1000):
    result.delete()

assert CountTest.all().count() == 0

CountTest(prop="hello").put()
CountTest(prop="hello").put()
CountTest(prop="hello").put()
CountTest(prop="goodbye").put()
CountTest(prop="hello").put()
CountTest(prop="hello").put()
CountTest(prop="goodbye").put()
CountTest(prop="hello").put()
CountTest(prop="goodbye").put()
CountTest(prop="hello").put()

assert CountTest.all().count() == 10
assert CountTest.all().count(5) == 5
assert CountTest.all().filter('prop =', 'hello').count() == 7
assert CountTest.all().filter('prop =', 'hello').count(5) == 5
assert CountTest.all().filter('prop =', 'goodbye').count() == 3
assert CountTest.all().filter('prop =', 'hello').filter('prop =', 'goodbye').count() == 0

assert CountTest.all().filter('prop !=', 'hello').count() == 3
assert CountTest.all().filter('prop !=', 'goodbye').count() == 7

print 'Test filtering on a non-existent key...<br/>'
assert CountTest.all().filter('test <', 5).count() == 0

print 'Test a query using the datastore API directly...<br/>'
query = datastore.Query('CountTest')
query['prop ='] = 'hello'
assert query.Count() == 7

print 'Test creating and querying on an Entity directly...<br/>'
for entity in datastore.Query('mike').Get(1000):
    datastore.Delete(entity.key())

entity = datastore.Entity("mike")
entity["demo"] = 5
datastore.Put(entity)

entity = datastore.Entity("mike")
entity["demo"] = 10
datastore.Put(entity)

entity = datastore.Entity("mike")
entity["demo"] = -4
datastore.Put(entity)

query = datastore.Query('mike')
query['demo >'] = 3
assert query.Count() == 2

query = datastore.Query('mike')
query['demo ='] = -4
entities = query.Get(1)
assert len(entities) == 1
assert entities[0]['demo'] == -4

print 'Test gets and deletes using key_name...<br/>'
class KeyName(db.Model):
    x = db.IntegerProperty()
for result in KeyName.all().fetch(1000):
    result.delete()

k = KeyName(x=2, key_name="test")
db.put(k)
assert k.key().id() == None
assert k.key().name() == "test"
assert KeyName.all().count() == 1
assert db.get(k.key()).x == 2
db.delete(k.key())
assert KeyName.all().count() == 0


print 'Test __key__ queries...<br/>'
class KeySortTest(db.Model):
    x = db.IntegerProperty()

for result in KeySortTest.all().fetch(1000):
    result.delete()

zero  = KeySortTest(x=0).put()
one   = KeySortTest(x=1, key_name="test").put()
two   = KeySortTest(x=2).put()
three = KeySortTest(x=3, key_name="again").put()
four  = KeySortTest(x=4).put()
five  = KeySortTest(x=5, key_name="something").put()

assert KeySortTest.gql('WHERE __key__ > :1', one).count() == 0
assert KeySortTest.gql('WHERE __key__ <= :1', one).count() == 6
assert KeySortTest.gql('WHERE __key__ > :1', three).count() == 2
assert KeySortTest.gql('WHERE __key__ <= :1', three).count() == 4
assert KeySortTest.gql('WHERE __key__ < :1', zero).count() < 3
assert KeySortTest.gql('WHERE __key__ < :1', zero).count() != KeySortTest.gql('WHERE __key__ < :1', two).count()
assert KeySortTest.gql('WHERE __key__ < :1', zero).count() + KeySortTest.gql('WHERE __key__ >= :1', zero).count() == 6

res = KeySortTest.gql('ORDER BY __key__')
assert res[3].x == 3
assert res[4].x == 5
assert res[5].x == 1

res = KeySortTest.all().order('__key__')
assert res[3].x == 3
assert res[4].x == 5
assert res[5].x == 1

first_three = [a.x for a in res[:3]]
db.delete(two)
no_2 = [a.x for a in KeySortTest.gql('ORDER BY __key__')]
assert first_three.index(no_2[0]) < first_three.index(no_2[1])


print "Test paths in __key__ queries...<br/>"
class KeyPath(db.Model):
    x = db.IntegerProperty()
for result in KeyPath.all().fetch(1000):
    result.delete()

a = KeyPath(x=0, key_name="test").put()
b = KeyPath(x=1, key_name="mike").put()
c = KeyPath(x=2, key_name="test", parent=a).put()
d = KeyPath(x=3, key_name="mike", parent=a).put()
e = KeyPath(x=4, key_name="test", parent=b).put()
f = KeyPath(x=5, key_name="mike", parent=c).put()
g = KeyPath(x=6, key_name="test", parent=f).put()

assert [a.x for a in KeyPath.gql('ORDER BY __key__ ASC')] == [1, 4, 0, 3, 2, 5, 6]
assert [a.x for a in KeyPath.gql('ORDER BY __key__ DESC')] == [6, 5, 2, 3, 0, 4, 1]


print "Test key uniqueness constraints...<br/>"
class KeyUnique(db.Model):
    x = db.IntegerProperty()
for result in KeyUnique.all().fetch(1000):
    result.delete()

assert KeyUnique.all().count() == 0

KeyUnique(x=1).put()
assert KeyUnique.all().count() == 1

KeyUnique(x=2, key_name="test").put()
assert KeyUnique.all().count() == 2

KeyUnique(x=3, key_name="test").put()
assert KeyUnique.all().count() == 2

a = KeySortTest(x=4, key_name="test").put()
assert KeyUnique.all().count() == 2

b = KeyUnique(x=5, key_name="test", parent=a).put()
assert KeyUnique.all().count() == 3

c = KeyUnique(x=6, key_name="test", parent=b).put()
assert KeyUnique.all().count() == 4

KeyUnique(x=7, key_name="test", parent=a).put()
assert KeyUnique.all().count() == 4

KeyUnique(x=8, key_name="test", parent=c).put()
assert KeyUnique.all().count() == 5


print "Test that an entity's key is the same after a round trip to the db...<br/>"
class KeyUnchangedTest(db.Model):
    x = db.IntegerProperty()
for result in KeyUnchangedTest.all().fetch(1000):
    result.delete()

a = KeyUnchangedTest(x=1).put()
b = KeyUnchangedTest(x=2, parent=a).put()
c = KeyUnchangedTest(x=3, parent=a, key_name="test").put()

assert b.parent() == a
assert c.parent() == a
assert b.name() == None
assert c.name() == "test"
assert b.id() != None
assert c.id() == None
assert isinstance(b.id(), (int, long))

bprime = KeyUnchangedTest.all().filter("x =", 2).get().key()
cprime = KeyUnchangedTest.all().filter("x =", 3).get().key()

assert bprime.parent() == a
assert cprime.parent() == a
assert bprime.name() == None
assert cprime.name() == "test"
assert bprime.id() != None
assert cprime.id() == None
assert isinstance(bprime.id(), (int, long))


print "Test basic ancestor queries...<br/>"
class Ancestor(db.Model):
    x = db.IntegerProperty()
for result in Ancestor.all().fetch(1000):
    result.delete()

a = Ancestor(x=0).put()
b = Ancestor(x=1, parent=a)
b.put()
c = Ancestor(x=2, parent=b).put()
d = Ancestor(x=3, parent=a).put()

assert Ancestor.all().ancestor(a).count() == 4
assert Ancestor.all().ancestor(b).count() == 2
assert Ancestor.all().ancestor(d).count() == 1


print "Test ancestor queries across kinds...<br/>"
class Ancestor2(db.Model):
    x = db.IntegerProperty()
for result in Ancestor.all().fetch(1000):
    result.delete()

e = Ancestor2(x=4, parent=d).put()
f = Ancestor(x=5, parent=e).put()

assert Ancestor.all().ancestor(d).count() == 1
assert Ancestor.all().ancestor(e).count() == 1
assert Ancestor.all().ancestor(f).count() == 1
assert Ancestor2.all().ancestor(d).count() == 1
assert Ancestor2.all().ancestor(e).count() == 1
assert Ancestor2.all().ancestor(f).count() == 0


print "Test trickier ancestor queries...<br/>"
a = Ancestor(x=10).put()
b = Ancestor(x=11, key_name="test", parent=a).put()
c = Ancestor(x=12, key_name="test").put()
d = Ancestor(x=13, parent=b).put()

assert db.GqlQuery("SELECT * FROM Ancestor WHERE ANCESTOR IS :1", a).count() == 3
assert db.GqlQuery("SELECT * FROM Ancestor WHERE ANCESTOR IS :1", b).count() == 2
assert db.GqlQuery("SELECT * FROM Ancestor WHERE ANCESTOR IS :1", c).count() == 1
assert db.GqlQuery("SELECT * FROM Ancestor WHERE ANCESTOR IS :1", d).count() == 1


print "Test some queries that should have a count of 0...<br/>"
class NeverBeenSaved(db.Model):
    something = db.StringProperty()

assert NeverBeenSaved.all().count() == 0
assert NeverBeenSaved.all().order("something").get() == None

class UnorderableProperty(db.Model):
    text = db.TextProperty()

for result in UnorderableProperty.all().fetch(1000):
    result.delete()

UnorderableProperty(text="hello").put()
UnorderableProperty(text="goodbye").put()

assert UnorderableProperty.all().count() == 2
assert UnorderableProperty.all().order("text").count() == 0


print 'Test get_by_id...<br/>'
CountTest(prop="abeginning").put()
akey = CountTest(prop="zend", key_name="a").put()
CountTest(prop="middle", key_name="b").put()
CountTest(prop="huh?").put()
key1 = CountTest(prop="mike").put()
key = CountTest(prop="10gen").put()

assert CountTest.get_by_id(key.id()).prop == "10gen"
assert [a.prop for a in CountTest.get_by_id([key1.id(), key.id()])] == ["mike", "10gen"]
assert CountTest.get_by_id(key5.id()) == None


print 'Test get_by_key_name...<br/>'
assert CountTest.get_by_key_name("a").prop == "zend"
assert [a.prop for a in CountTest.get_by_key_name(["b", "a"])] == ["middle", "zend"]

assert CountTest.all().filter('prop =', 'zend').count() == 1
CountTest(prop="zend1", key_name="a").put()
assert CountTest.get_by_key_name("a").prop == "zend1"
assert CountTest.get(akey).prop == "zend1"
assert CountTest.all().filter('prop =', 'zend').count() == 0


print 'Test get_or_insert...<br/>'
class WikiTopic(db.Model):
    creation_date = db.DateTimeProperty(auto_now_add=True)
    body = db.TextProperty(required=True)

# The first time through we'll create the new topic.
wiki_word = 'CommonIdioms'
topic = WikiTopic.get_or_insert(wiki_word,
                                body='This topic is totally new!')
assert topic.key().name() == 'CommonIdioms'
assert topic.body == 'This topic is totally new!'

# The second time through will just retrieve the entity.
overwrite_topic = WikiTopic.get_or_insert(wiki_word,
                                          body='A totally different message!')
assert topic.key().name() == 'CommonIdioms'
assert topic.body == 'This topic is totally new!'


print 'Test filters...<br/>'
assert CountTest.all().filter('prop <', 'hello').count() == 5
assert CountTest.all().filter('prop <=', 'hello').count() == 12
assert CountTest.all().filter('prop >', 'hello').count() == 4
assert CountTest.all().filter('prop >=', 'hello').count() == 11

class FilterTest(db.Model):
    num = db.IntegerProperty()

for result in FilterTest.all().fetch(1000):
    result.delete()

FilterTest(num=1).put()
FilterTest(num=19).put()
FilterTest(num=10).put()
FilterTest(num=2).put()
FilterTest(num=8).put()
FilterTest(num=11).put()
FilterTest(num=8).put()
FilterTest(num=1).put()

assert FilterTest.all().filter('num <', 2).count() == 2
assert FilterTest.all().filter('num <=', 2).count() == 3
assert FilterTest.all().filter('num =', 6).count() == 0
assert FilterTest.all().filter('num =', 8).count() == 2
assert FilterTest.all().filter('num >=', 10).count() == 3
assert FilterTest.all().filter('num >', 10).count() == 2
assert FilterTest.all().filter('num !=', 8).count() == 6

assert FilterTest.all().filter('num <', 10).filter('num >=', 2).count() == 3
assert FilterTest.all().filter('num =', 10).filter('num >=', 2).count() == 1
assert FilterTest.all().filter('num =', 10).filter('num <', 2).count() == 0
assert FilterTest.all().filter('num =', 10).filter('num =', 2).count() == 0
assert FilterTest.all().filter('num =', 10).filter('num =', 10).count() == 1

print 'Test deleting an entity directly using its key...<br/>'
db.delete(key)
assert CountTest.all().count() == 15


print 'Test a list filter...<br/>'
class ListFilterTest(db.Model):
    tags = db.StringListProperty()
    list = db.ListProperty(int)

for result in ListFilterTest.all().fetch(1000):
    result.delete()

ListFilterTest(tags=["hello", "world"], list=[5,5,1986]).put()
ListFilterTest(tags=["world", "of", "warcraft"], list=[100, 19]).put()
ListFilterTest(tags=["huh", "what"], list=[]).put()
ListFilterTest(tags=[], list=[19, 5]).put()

assert ListFilterTest.all().filter('tags =', 'hello').count() == 1
assert ListFilterTest.all().filter('tags =', 'goodbye').count() == 0
assert ListFilterTest.all().filter('tags =', 'world').count() == 2
assert ListFilterTest.all().filter('tags =', 'huh').count() == 1
assert ListFilterTest.all().filter('list =', 1986).count() == 1
assert ListFilterTest.all().filter('list =', 2008).count() == 0
assert ListFilterTest.all().filter('list =', 5).count() == 2
assert ListFilterTest.all().filter('list =', 100).count() == 1
assert ListFilterTest.all().filter('tags >', "what").count() == 2
assert ListFilterTest.all().filter('tags >=', "what").count() == 3
assert ListFilterTest.all().filter('tags <', "huh").count() == 1
assert ListFilterTest.all().filter('tags <=', "huh").count() == 2

print 'Test Expandos...<br/>'
class Song(db.Expando):
    title = db.StringProperty()

for result in Song.all().fetch(1000):
    result.delete()

crazy = Song(title='Crazy like a diamond',
             author='Lucy Sky',
             publish_date='yesterday',
             rating=5.0)
crazy_key = crazy.put()
hoboken = Song(title='The man from Hoboken',
               author=['Anthony', 'Lou'],
               publish_date=datetime.datetime(1977, 5, 3))
hobo_key = hoboken.put()

crazy.last_minute_note=db.Text('Get a train to the station.')
crazy.put()

a = db.get(crazy_key)
assert a.author == "Lucy Sky"
assert a.rating == 5.0
assert a.last_minute_note == "Get a train to the station."
del a.publish_date
a.put()
try:
    db.get(crazy_key).publish_date
    assert False
except AttributeError:
    pass
b = db.get(hobo_key)

assert 'Anthony' in b.author

assert b.publish_date.year == 1977

print 'Test that query results are iterable...<br/>'
count = 0
for a in FilterTest.all():
    count += 1
assert count == 8

count = 0
for a in FilterTest.all().filter('num >=', 10):
    count += 1
assert count == 3

count = 0
for a in FilterTest.all().filter('num >=', 10).fetch(1000):
    count += 1
assert count == 3

count = 0
query = db.Query(FilterTest)
for a in query:
    count += 1
assert count == 8

print 'Test some simple GQL...<br/>'
numbers = db.GqlQuery("SELECT * FROM FilterTest ORDER BY num DESC LIMIT 4")
numbers = [a.num for a in numbers]
assert numbers == [19, 11, 10, 8]
numbers = db.GqlQuery("SELECT * FROM FilterTest WHERE num = 11")
assert numbers.count() == 1
numbers = db.GqlQuery("SELECT * FROM FilterTest WHERE num = :1", 8)
assert numbers.count() == 2
numbers = db.GqlQuery("SELECT * FROM FilterTest WHERE num = :number", hello="mike", number=19)
assert numbers.count() == 1

assert db.GqlQuery("SELECT * FROM FilterTest WHERE num != 8").count() == 6

print 'Test the different arguments to property constructors...<br/>'

def validateHaha(x):
    if x != "haha":
        raise Exception("Not haha!")

class Test(db.Model):
    a = db.StringProperty(verbose_name="hello", default="mike", required=True)
    b = db.StringProperty(name="hello")
    c = db.StringProperty(default="testing", choices=None)
    d = db.StringProperty(required=True)
    e = db.StringProperty(validator=validateHaha)
    f = db.StringProperty(choices=["mike", "10gen"])

for result in Test.all().fetch(1000):
    result.delete()

t1 = Test(d="hello", e="haha")
assert t1.a == "mike"
assert t1.c == "testing"
assert not t1.b
assert not t1.f
assert t1.d == "hello"
assert t1.e == "haha"
t1.put()

t2 = Test(a="hello", hello="world", c="random", d="something", e="haha", f="10gen")
assert t2.a == "hello"
assert t2.b == "world"
assert t2.c == "random"
assert t2.d == "something"
assert t2.e == "haha"
assert t2.f == "10gen"
t2.put()

assert Test.all().filter('hello =', 'world').count() == 1

try:
    t1 = Test(a=None, d="hello", e="haha")
    assert False
except db.BadValueError:
    pass

try:
    t1 = Test(e="haha")
    assert False
except db.BadValueError:
    pass

try:
    t1 = Test(d="hello", e="hello")
    assert False
except Exception:
    pass

try:
    t1 = Test(d="hello", e="haha", f="random")
    assert False
except db.BadValueError:
    pass

print 'Test arguments for DateTimeProperty...<br/>'
class DateTimeTest(db.Model):
    normal = db.DateTimeProperty()
    change = db.DateTimeProperty(auto_now=True)
    create = db.DateTimeProperty(auto_now_add=True)

dt = datetime.datetime.now()
dt = datetime.datetime(dt.year,
                       dt.month,
                       dt.day,
                       dt.hour,
                       dt.minute,
                       dt.second,
                       int(dt.microsecond / 1000) * 1000)
time.sleep(0.5)
d = DateTimeTest(normal=dt, change=dt)
time.sleep(0.5)
key = d.put()
d = db.get(key)
create = d.create
change = d.change
assert d.normal == dt
assert d.change > d.create
assert d.change > d.normal
assert d.create > dt
key = d.put()
d = db.get(key)
assert d.normal == dt
assert d.change > d.create
assert d.change > d.normal
assert d.create > dt
assert d.create == create
assert d.change > change

print 'Test arguments for ListProperty...<br/>'
try:
    class ListTest2(db.Model):
        list = db.ListProperty()
    assert False
except TypeError:
    pass

class ListTest(db.Model):
    list = db.ListProperty(item_type=int)
    list2 = db.ListProperty(int, default=None)

l = ListTest(list=[])
l = ListTest(list=[1,2,3])
assert l.list2 == []
try:
    l = ListTest(list=None)
    assert False
except db.BadValueError:
    pass

print 'Test multiline StringProperty...<br/>'
class StringTest(db.Model):
    yes = db.StringProperty(multiline=True)
    no = db.StringProperty(multiline=False)
s = StringTest(yes="hello\nworld", no="hello world")
try:
    s = StringTest(yes="hello\nworld", no="hello\nworld")
    assert False
except db.BadValueError:
    pass

print 'Test sorting on mixed string and unicode objects...<br/>'
class StringSort(db.Model):
    prop = db.StringProperty()
for result in StringSort.all().fetch(1000):
    result.delete()
StringSort(prop="hello").put()
StringSort(prop="goodbye").put()
StringSort(prop="test").put()
StringSort(prop=u"mike").put()
StringSort(prop=u"example").put()
StringSort(prop=u"random").put()
res = StringSort.all().order("prop").fetch(6)
res = [e.prop for e in res]
assert res == ["example", "goodbye", "hello", "mike", "random", "test"]
assert StringSort.all().filter("prop >", "oops").count() == 2
assert StringSort.all().filter("prop >", u"oops").count() == 2


print 'Test saving and restoring strings and unicode...<br/>'
class TestStrUni(db.Model):
    string = db.StringProperty()
    uni = db.StringProperty()

for result in TestStrUni.all().fetch(1000):
    result.delete()

test = TestStrUni(string = "hello", uni = u"hello")
assert isinstance(test.string, types.StringType)
assert isinstance(test.uni, types.UnicodeType)
out = db.get(test.put())
assert isinstance(out.string, types.UnicodeType)
assert isinstance(out.uni, types.UnicodeType)


print 'Test saving and restoring every kind of property...<br/>'
class Everything(db.Model):
    str = db.StringProperty()
    bool = db.BooleanProperty()
    int = db.IntegerProperty()
    float = db.FloatProperty()
    datetime = db.DateTimeProperty()
    date = db.DateProperty()
    time = db.TimeProperty()
    list = db.ListProperty(types.IntType)
    strlist = db.StringListProperty()
    user = db.UserProperty()
    blob = db.BlobProperty()
    text = db.TextProperty()
    category = db.CategoryProperty()
    link = db.LinkProperty()
    email = db.EmailProperty()
    geopt = db.GeoPtProperty()
    im = db.IMProperty()
    phonenumber = db.PhoneNumberProperty()
    postaladdress = db.PostalAddressProperty()
    rating = db.RatingProperty()

for result in Everything.all().fetch(1000):
    result.delete()

d2 = datetime.datetime.now()
d2 = datetime.datetime(d2.year,
                       d2.month,
                       d2.day,
                       d2.hour,
                       d2.minute,
                       d2.second,
                       int(d2.microsecond / 1000) * 1000)
time.sleep(0.5)
d = datetime.datetime.now()
d = datetime.datetime(d.year,
                      d.month,
                      d.day,
                      d.hour,
                      d.minute,
                      d.second,
                      int(d.microsecond / 1000) * 1000)

e1 = Everything(str=u"hello",
                bool=True,
                int=10,
                float=5.05,
                datetime=d,
                date=d.date(),
                time=d.time(),
                list=[1,2,3],
                strlist=["hello", u'world'],
                user=users.User("mike@example.com"),
                blob=db.Blob("somerandomdata"),
                text=db.Text("some random text"),
                category=db.Category("awesome"),
                link=db.Link("http://www.10gen.com"),
                email=db.Email("test@example.com"),
                geopt=db.GeoPt(40.74067, -73.99367),
                im=db.IM("http://aim.com/", "example"),
                phonenumber=db.PhoneNumber("1 (999) 123-4567"),
                postaladdress=db.PostalAddress("40 W 20th St., New York, NY"),
                rating=db.Rating(99),
                )
out = db.get(e1.put())
def failIfNot(reference, value, type):
    assert value == reference
    assert isinstance(value, type)

failIfNot(e1.str, out.str, types.UnicodeType)

failIfNot(e1.bool, out.bool, types.BooleanType)

# TODO on AE this would always be types.LongType
# This gets difficult with our database, as longs are stored as doubles.
# For now our datastore API just stores and fetches (int, long) as
# their respective type.
failIfNot(e1.int, out.int, (types.IntType, types.LongType))

failIfNot(e1.float, out.float, types.FloatType)
failIfNot(e1.datetime, out.datetime, datetime.datetime)
failIfNot(e1.date, out.date, datetime.date)
failIfNot(e1.time, out.time, datetime.time)
failIfNot(e1.list, out.list, list)
failIfNot(e1.strlist, out.strlist, list)
failIfNot(e1.user, out.user, users.User)
failIfNot(e1.blob, out.blob, db.Blob)
failIfNot(e1.text, out.text, db.Text)
failIfNot(e1.category, out.category, db.Category)
failIfNot(e1.link, out.link, db.Link)
failIfNot(e1.email, out.email, db.Email)
failIfNot(e1.geopt, out.geopt, db.GeoPt)
failIfNot(e1.im, out.im, db.IM)
failIfNot(e1.phonenumber, out.phonenumber, db.PhoneNumber)
failIfNot(e1.postaladdress, out.postaladdress, db.PostalAddress)
failIfNot(e1.rating, out.rating, db.Rating)

e2 = Everything(str="goodbye",
                bool=False,
                int=5,
                float=1.01,
                datetime=d2,
                date=d2.date(),
                time=d2.time(),
                list=[10,0,3],
                strlist=["zinc", u'alpha'],
                user=users.User("dave@example.com"),
                blob=db.Blob("aoeunthauneot"),
                text=db.Text(";qtjkhrchauenth"),
                category=db.Category("aaawesome"),
                link=db.Link("http://10gen.com"),
                email=db.Email("mike@example.com"),
                geopt=db.GeoPt(38.5, -70.99367),
                im=db.IM("http://aim.com/", "dave"),
                phonenumber=db.PhoneNumber("1 (888) 123-4567"),
                postaladdress=db.PostalAddress("39 W 20th St., New York, NY"),
                rating=db.Rating(90),
                )
e2.put()

def checkOrder(attr, order, str_value):
    assert Everything.all().order(order + attr).get().str == str_value

checkOrder('str', '', 'goodbye')
checkOrder('bool', '', 'goodbye')
checkOrder('float', '', 'goodbye')
checkOrder('datetime', '', 'goodbye')
checkOrder('time', '', 'goodbye')
checkOrder('list', '', 'goodbye')
checkOrder('strlist', '', 'goodbye')
checkOrder('user', '', 'goodbye')
try:
    checkOrder('blob', '', 'goodbye')
    assert False
except AttributeError:
    pass
try:
    checkOrder('text', '', 'goodbye')
    assert False
except AttributeError:
    pass
checkOrder('category', '', 'goodbye')
checkOrder('link', '', 'goodbye')
checkOrder('email', '', 'goodbye')
checkOrder('geopt', '', 'goodbye')
checkOrder('im', '', 'goodbye')
checkOrder('phonenumber', '', 'goodbye')
checkOrder('postaladdress', '', 'goodbye')
checkOrder('rating', '', 'goodbye')

checkOrder('str', '-', 'hello')
checkOrder('bool', '-', 'hello')
checkOrder('float', '-', 'hello')
checkOrder('datetime', '-', 'hello')
checkOrder('time', '-', 'hello')
checkOrder('list', '-', 'goodbye')
checkOrder('strlist', '-', 'goodbye')
checkOrder('user', '-', 'hello')
try:
    checkOrder('blob', '-', 'hello')
    assert False
except AttributeError:
    pass
try:
    checkOrder('text', '-', 'hello')
    assert False
except AttributeError:
    pass
checkOrder('category', '-', 'hello')
checkOrder('link', '-', 'hello')
checkOrder('email', '-', 'hello')
checkOrder('geopt', '-', 'hello')
checkOrder('im', '-', 'hello')
checkOrder('phonenumber', '-', 'hello')
checkOrder('postaladdress', '-', 'hello')
checkOrder('rating', '-', 'hello')

assert Everything.all().order('list').order('blob').get() == None
assert Everything.all().order('blob').order('list').get() == None
assert Everything.all().order('list').order('-bool').get().str == 'goodbye'

print 'Test a ByteString...<br/>'
class TestBytes(db.Model):
    a = db.ByteStringProperty()

for result in TestBytes.all().fetch(1000):
    result.delete()

test = TestBytes(a="\x05\x22aek")
out = db.get(test.put())
assert out.a == test.a
assert isinstance(out, test.__class__)

# The remaining tests are adapted from here:
# http://code.google.com/p/gae-sqlite/source/browse/trunk/unittests.py
# Which is copyright 2008 Jens Scheffler and used under the Apache License v2.0
class TestModel(db.Model):
    text = db.StringProperty(default="some text")
    number = db.IntegerProperty(default=42)
    float = db.FloatProperty(default=3.14)
    cond1 = db.BooleanProperty(default=True)
    cond2 = db.BooleanProperty(default=False)

for result in TestModel.all().fetch(1000):
    result.delete()

print 'Test writing a value twice...<br/>'
model = TestModel()
key = model.put()
id = key._Key__reference.path().element_list()[-1].id()
key = model.put()
id2 = key._Key__reference.path().element_list()[-1].id()
assert id == id2

print 'Test getting a single model from the datastore...<br/>'
model1 = TestModel(number=1)
model2 = TestModel(number=2, text='#2')
model3 = TestModel(number=3)
key1 = model1.put()
key2 = model2.put()
key3 = model3.put()
fetched = TestModel.get(key2)
assert 2 == fetched.number
assert '#2' == fetched.text
assert 3.14 == fetched.float
assert True == fetched.cond1
assert False == fetched.cond2

print 'Test getting a single model from the datastore with a string key...<br/>'
model1 = TestModel(number=1)
model2 = TestModel(key_name='custom', number=2, text='#2')
model3 = TestModel(number=3)
key1 = model1.put()
key2 = model2.put()
key3 = model3.put()
fetched = TestModel.get_by_key_name('custom')
assert 2 == fetched.number
assert '#2' == fetched.text

print 'Test getting several models from the datastore...<br/>'
model1 = TestModel(number=1)
model2 = TestModel(number=2)
model3 = TestModel(number=3)
key1 = model1.put()
key2 = model2.put()
key3 = model3.put()
fetched = TestModel.get([key2, key1])
assert 2 == len(fetched)
assert 2 == fetched[0].number
assert 1 == fetched[1].number

print 'Test if get_or_insert works correctly...<br/>'
model1 = TestModel(number=1)
model1.put()
model2 = TestModel.get_or_insert('foo', number=13, text='t')
assert 13 == model2.number
fetched = TestModel.get_by_key_name('foo')
assert 13 == fetched.number

print 'Test a simple query...<br/>'
model = TestModel(text='t1', number=13)
model.put()
data = TestModel.gql(
    'WHERE text=:1 and number=:2 order by text desc',
    't1', 13).fetch(5)
assert 1 == len(data)
assert 't1' == data[0].text
assert 13 == data[0].number

print 'Test query on non-existent field...<br/>'
model = TestModel(text='t1', number=13)
model.put()
data = TestModel.gql(
    'WHERE text2=:1 and number=:2 order by text desc',
    't1', 13).fetch(5)
assert 0 == len(data)

print 'Test query on non-existent collection...<br/>'
class UnknownKind(TestModel):
    pass
data = UnknownKind.gql(
    'WHERE text=:1', 't1').fetch(5)
assert 0 == len(data)

print 'Test storing an unknown kind of Model...<br/>'
class UnknownKind(TestModel):
    pass
model = UnknownKind()
model.put()

print 'Test what happens if a field gets added to a model...<br/>'
class Mutation(TestModel):
    @classmethod
    def kind(cls):
        return 'TestModel'
    text2 = db.StringProperty(default='some more text')
model = Mutation(text='Text 1', text2='Text 2')
model.put()
model.delete()

print 'Test what happens if a field changes its type...<br/>'
class Mutation(db.Model):
    @classmethod
    def kind(cls):
        return 'TestModel'
    text = db.IntegerProperty(default=42)
model = Mutation(text=23)
model.put()
model.delete()

print '</body></html>'

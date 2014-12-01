import os, sys
import time 

sys.path.append("src/")

from baserecord import BaseRecord
from fields import StringField, IntegerField, DateTimeField, \
        OneToManyRelation, FloatField, OptionField, ManyToOneRelation, \
        ManyToManyRelation
from core import Database


# first the declaritive model 
class Author(BaseRecord):
    name = StringField(size=50)
    family_name = StringField(size=50)
    avail_since = DateTimeField(auto_now_add=True)

class Book(BaseRecord):
    title = StringField(size=300)
    pub_date = DateTimeField()
    avail_since = DateTimeField(auto_now_add=True)
    abstract = StringField(size=1000)
    isbn = StringField(size=50)
    author = ManyToOneRelation(Author)

# set up database
fn = "::memory::"
db = Database()
db.init(fn)
db.check_for_tables()

# create model instances
a1 = Author(name="Karl", family_name="der Tolle")
a2 = Author(name="Rick", family_name="Ruckelig")

# just save an instance in order to get it into the database
a1.save()
a2.save()

# other instances ...
b1 = Book(title="Der am Rechner stand", 
        pub_date=int(time.time())-60*60*24*31,
        abstract="Sie gingen hier und spranger dort, yipee, was war das super",
        isbn="AJI42GER32FDG9",
        author=a1)

# saving
b1.save()

# printing, and now fast a query...
print a1 
print a2 
print b1

# access objects transparently through their class
books = Book.objects.all()
mybook = books[0]
print mybook 

mybook.isbn = "iojdoijofsd"
mybook.abstract = "idosjifsdfoifsjd"
mybook.save()

# auto-join not yet active again...
print mybook.author
print mybook



import os, sys
import time 

sys.path.append("src/")

from baserecord import BaseRecord
#from baseview import BaseView
from fields import StringField, IntegerField, DateTimeField, \
        FloatField, OptionField, ManyToOneRelation, \
        ManyToManyRelation, OneToOneRelation
from core import SQLiteDatabase, MemoryDatabase

# first the declaritive model 
class Author(BaseRecord):
    name = StringField(size=50)
    family_name = StringField(size=50)
    avail_since = DateTimeField(auto_now_add=True)
    birthday = DateTimeField()
    
class Address(BaseRecord):
    author = OneToOneRelation(Author, backref="address")
    city = StringField(size=100)
    street = StringField(size=100)
    street_no = IntegerField()
    postal_code = IntegerField()    

class Book(BaseRecord):
    title = StringField(size=300)
    pub_date = DateTimeField()
    avail_since = DateTimeField(auto_now_add=True)
    abstract = StringField(size=1000)
    isbn = StringField(size=50)
    author = ManyToOneRelation(Author, backref="books")

# declare a view-model providing all books 
#class LivingInHanau(BaseView):
#    expr = 

# set up database
fn = ":memory:"
db = SQLiteDatabase()
db.init(fn)
db.setup_relations()
db.create_tables()


# create model instances
a1 = Author(name="Karl", family_name="der Tolle", 
            birthday=int(time.time())-60*60*24*15)
a2 = Author(name="Rick", family_name="Ruckelig", 
            birthday=int(time.time())-60*60*24*32)

# just save an instance in order to get it into the database
a1.save()
a2.save()

# other instances ...
b1 = Book(title="Der am Rechner stand", 
        pub_date=int(time.time())-60*60*24*31,
        abstract="Sie gingen hier und sprangen dort, yipee, was war das super",
        isbn="AJI42GER32FDG9",
        author=a1)
# saving
b1.save()

b2 = Book(title="MoIssna", pub_date=int(time.time())-3423, 
          abstract="brudalst", isbn="324FFWGRW324GS5S", author=a1)
# saving
b2.save()

# count
print "books inside: ", len(a1.books)

# access objects transparently through their class
books = Book.objects.all()
mybook = books[0]
print mybook 

mybook.isbn = "iojdoijofsd"
mybook.abstract = "idosjifsdfoifsjd"
mybook.save()

# joining (of explicit relations) works out of the box now!
print mybook.author

adr = Address(
    author=mybook.author,
    city="Frankfurt", street="Dummstrasse", 
    street_no=123, postal_code=60325)

mybook.author.address = adr

print mybook.author
print mybook

adr.save()
print mybook.author.address

# book 2 
b2.author = a2 
b2.save()

# fine backref + 1:N + N:1 works good
print len(a1.books)



db.close()

import os, sys
import time 

from baserecord import BaseRecord
#from baseview import BaseView
from fields import StringField, IntegerField, DateTimeField, \
        FloatField, OptionField, ManyToOneRelation, \
        ManyToManyRelation, OneToOneRelation, BaseFieldGroup
from core import SQLiteDatabase, MemoryDatabase

sys.path.append("..")

class Point3D:
    """Keeps a 3D point/vector (abstract)"""
    def __init__(self, x=None, y=None, z=None, scale=None):
        if scale is None:
            self.x = x
            self.y = y
            self.z = z
        else:
            self.x = x * scale
            self.y = y * scale
            self.z = z * scale

    def trans(self, matrix):
        """Transform based on 4x4 matrix"""
        raise NotImplementedError()

    def __repr__(self):
        return "{}({}, {}, {})".format(self.__class__.__name__,
                self.x , self.y, self.z)

class Vector(Point3D):
    """Specialization of Point3D for vectors"""
    def trans(self, matrix, scale=None):
        if scale is None:
            return Vector(*np.dot(matrix[0:3, 0:3], 
                np.array([self.x, self.y, self.z]))[:3])
        return Vector(*np.dot(matrix[0:3, 0:3], 
            np.array([self.x*scale, self.y*scale, self.z*scale]))[:3])
    def angle_to(self, other):
        return math.acos((self.x*other.x+self.y*other.y+self.z*other.z) / 
               (math.sqrt(self.x*self.x+self.y*self.y+self.z*self.z) *
                math.sqrt(other.x*other.x+other.y*other.y+other.z*other.z)))

class Coord(Point3D):
    """Specialization of Point3D for coordinates"""
    def trans(self, matrix, scale=None):
        if scale is None:
            return Coord(*np.dot(matrix, 
                np.array([self.x, self.y, self.z, 1.0]))[:3])
        return Coord(*np.dot(matrix, 
            np.array([self.x*scale, self.y*scale, self.z*scale, 1.0]))[:3])

## easy assembling field groups to wrap any data-type
class Point3DFieldGroup(BaseFieldGroup):
    cls = Point3D 
    cls_ctor_args = ()
    key2field = {
        "x": FloatField(),
        "y": FloatField(), 
        "z": FloatField()
    }

class Vector3DFieldGroup(Point3DFieldGroup):
    cls = Vector

class Coord3DFieldGroup(Point3DFieldGroup):
    cls = Coord

class MultiLevelData:
    def __init__(self):
        self.pos = Coord()
        self.direction = Vector()
        self.info = ""

class MultiLevelFieldGroup(BaseFieldGroup):
    cls = MultiLevelData
    cls_ctor_args = {}
    key2field = {
        "pos": Coord3DFieldGroup(),
        "direction": Vector3DFieldGroup(),
        "info": StringField(size=200)
    }

# now the declaritive DB models
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

class HeadTrackData(BaseRecord):
    pos = Coord3DFieldGroup()
    direction = Vector3DFieldGroup()
    author = OneToOneRelation(Author, backref="het")
    multi = MultiLevelFieldGroup()


# declare a view-model providing all books 
#class LivingInHanau(BaseView):
#    expr = 

# set up database
fn = ":memory:"
db = SQLiteDatabase(fn)


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

# backref + 1:N + N:1 works good
print len(a1.books)

# field group to transparently put any datastructure inside the db 
m = MultiLevelData()
m.pos = Coord(24,21,43)
m.direction = Vector(23,23,42)
m.info = "Man is des scharf!"

head_track = HeadTrackData(pos=Coord(1,2,4), 
                           direction=Vector(4,2,5), 
                           author=a2,
                           multi=m)
print head_track.multi
print head_track.multi.info
head_track.save()

print head_track.objects.all()
print head_track.multi
print head_track.multi.direction
print head_track.multi.info

print "head tracking data, saved---printing:"
print "rowid", head_track.rowid
print "pos", head_track.pos
print "dir", head_track.direction 
print "author", head_track.author
m = head_track.multi
print "multi-depth", m.pos, m.direction, m.info

db.backup("foo.sqlite")
db.close()
db.setup("foo.sqlite")

obj = head_track.objects.all()[0]
print obj
print obj.multi.pos, obj.multi.direction, obj.multi.info
print obj.pos, obj.direction
    

db.close()

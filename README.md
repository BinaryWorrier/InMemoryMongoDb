# InMemoryMongoDb
Implements the .net MongoDb 2.5 drivers interfaces, to allow unit testing  
OK, so I haven't done the public opensource project before, be gentle with me good people
At this stage this is pretty much the results of a 2 day "can this be done" expirament, and the answer to which is 
"Yes this is absoutely doable"

Project Ethos.
- I'm reluctant to pull in anythign that isn't .net standard.
I'm used to working with an IoC container, if one becomes necessary I'd nearly hand roll a simple one, 
or include the code for TinyIoC rather than introduce a NuGet dependancy


- static
There are a lot of static classes and methods, I am in no way married to this approach.
The INMFilter class (that applied a FilterDefinition<T> against a BsonDocument) is one overly large God class, 
refactoring needs to happen there.

_Done, to do and things that aren't difficult_  
**No brainer things**  
-	Serialization of entities between Bson and back

**Done & mostly done**
-	Barebones of an InMemoryClient object & InMemoryDatabase object  
They’re functional but may need a small amount of work
-	InsertOne, InsertMany  assumes all Ids are ObjectIds
There’s a registry somewhere we can use to determine the correct Id factory to use for each entity to create the correct type of Id.
It’s not going to be terribly difficult, it just needs to be done.
-	DeleteOne, DeleteMany
-	Count
-	Filtering list of Objects on any operators I could find / think of ($and, $in, $exists, etc), simple to add more.
-	Getting a typed .net value from a field (harder than it sounds).

**Todo**
-	Projections
I imagine being technically difficult, will need figure out how these are represented and build something to work with it. 
Will require reflection.
-	Updates
These will be straightforward, but there are lots of them. 
One a basic structure is set up, these can be farmed out and the list of Operations can be done in parallel by different people
-	Aggregations
Once projections are done, there’s little new here beyond grouping, which is easily achieved with a dictionary or a HashSet
It will need Filtering, matching & projections and grouping.
It feels like it should be scary but I don’t think Aggregations will be rocket science.
-	Sessions, I’ve largely been ignoring sessions as I haven't used them and I’m not sure what they’re used for.
-	Views, I’ve completely ignored views as I haven't used them, however once projections are done views might be straightforward to knock off.

**Testing** 
I have some nunit tests put together for some  of what I’ve done so far, but what would be good is to expand on those, and have a set of data, a set of operations on that data.
We load the initial data set into MongoDb, run the operations on it, get the expected result.
Then load the initial data set into the InMemoryDb, run the operations on it, and compare that final dataset against the expected result.
Something to do the last part would be a good thing to have.



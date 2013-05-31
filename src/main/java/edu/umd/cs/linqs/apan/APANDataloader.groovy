package edu.umd.cs.linqs.apan;

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import edu.umd.cs.psl.config.ConfigBundle
import edu.umd.cs.psl.config.ConfigManager
import edu.umd.cs.psl.database.DataStore
import edu.umd.cs.psl.database.Database
import edu.umd.cs.psl.database.DatabasePopulator
import edu.umd.cs.psl.database.Partition
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type
import edu.umd.cs.psl.groovy.PSLModel
import edu.umd.cs.psl.model.argument.ArgumentType
import edu.umd.cs.psl.model.argument.GroundTerm
import edu.umd.cs.psl.model.argument.UniqueID
import edu.umd.cs.psl.model.argument.Variable
import edu.umd.cs.psl.model.atom.GroundAtom
import edu.umd.cs.psl.ui.loading.InserterUtils
import edu.umd.cs.psl.util.database.Queries

/** INITIALIZATION **/

Logger log = LoggerFactory.getLogger(this.class)

ConfigManager cm = ConfigManager.getManager()
ConfigBundle cb = cm.getBundle("apan")
//cb.setProperty("rdbmsdatastore.usestringids", true)

def defaultPath = System.getProperty("java.io.tmpdir")
String dbpath = cb.getString("dbpath", defaultPath + File.separator + "apan")
DataStore data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, dbpath, true), cb)

PSLModel m = new PSLModel(this, data)


/** MODEL **/

/* Number of groups */
//int numGroups = cb.getInt('numGroups', -1);

/* Predicates */
m.add predicate: "Discussion", types: [ArgumentType.UniqueID];
m.add predicate: "Word", types: [ArgumentType.UniqueID, ArgumentType.String];
m.add predicate: "Group", types: [ArgumentType.UniqueID, ArgumentType.String];
m.add predicate: "UsedWord", types: [ArgumentType.UniqueID, ArgumentType.UniqueID];
m.add predicate: "Friends", types: [ArgumentType.UniqueID, ArgumentType.UniqueID];
m.add predicate: "PostsIn", types: [ArgumentType.UniqueID, ArgumentType.UniqueID];
m.add predicate: "InGroup", types: [ArgumentType.UniqueID, ArgumentType.UniqueID];


/** LOAD DATA **/

/* Partitions */
Partition eviPart = new Partition(cb.getInt("eviPart", -1));
Partition tgtPart = new Partition(cb.getInt("tgtPart", -1));
Partition eStepPart = new Partition(cb.getInt('eStepPart', -1));
Partition mStepPart = new Partition(cb.getInt('mStepPart', -1));

def dataDir = "data/predicates/";
def inserter;
System.out.println("Loading predicate data ...");

/* Discussions */
inserter = data.getInserter(Discussion, eviPart);
InserterUtils.loadDelimitedData(inserter, dataDir + "Threads.csv", ",");
System.out.println("  Loaded Discussion predicate");

/* Words */
inserter = data.getInserter(Word, eviPart);
InserterUtils.loadDelimitedData(inserter, dataDir + "Words.csv", ",");
System.out.println("  Loaded Word predicate");

/* Groups */
inserter = data.getInserter(Group, eviPart);
InserterUtils.loadDelimitedData(inserter, dataDir + "Groups.csv", ",");
System.out.println("  Loaded Group predicate");

/* Friends */
inserter = data.getInserter(Friends, eviPart);
InserterUtils.loadDelimitedData(inserter, dataDir + "Friends.csv", ",");
System.out.println("  Loaded Friends predicate");

/* Word usage */
inserter = data.getInserter(UsedWord, eviPart);
InserterUtils.loadDelimitedData(inserter, dataDir + "UsedWord.csv", ",");
System.out.println("  Loaded UsedWord predicate");

/* Posts */
inserter = data.getInserter(PostsIn, tgtPart);
InserterUtils.loadDelimitedData(inserter, dataDir + "PostsInThread.csv", ",");
System.out.println("  Loaded PostsIn predicate");


/** POPULATE E-/M-step partitions **/

Database db;
Set<GroundAtom> atoms;
DatabasePopulator dbPop;

System.out.println("Populating E-/M-step paritions ...");

/* Build substitution map */
Map<Variable, Set<GroundTerm>> submap = new HashMap<Variable, Set<GroundTerm>>();
Variable U = new Variable("U");
Variable G = new Variable("G");
Variable T = new Variable("T");
Set<GroundTerm> users = new HashSet<GroundTerm>();
Set<GroundTerm> groups = new HashSet<GroundTerm>();
Set<GroundTerm> threads = new HashSet<GroundTerm>();
submap.put(U, users);
submap.put(G, groups);
submap.put(T, threads);

/* Open evidence partition */
db = data.getDatabase(eviPart);

/* Add all users from Friends predicate (should contain all users) */
atoms = Queries.getAllAtoms(db, Friends);
for (GroundAtom a : atoms) {
	users.add(a.getArguments()[0]);
}
atoms = Queries.getAllAtoms(db, UsedWord);
for (GroundAtom a : atoms) {
	users.add(a.getArguments()[0]);
}
System.out.println(String.format("  Added %d users", users.size()));

/* Add all groups */
//for (int g = 0; g < numGroups; g++) {
//	UniqueID groupID = data.getUniqueID(g);
//	groups.add(groupID);
//}
atoms = Queries.getAllAtoms(db, Group);
for (GroundAtom a : atoms) {
	groups.add(a.getArguments()[0]);
}
System.out.println(String.format("  Added %d groups", groups.size()));

/* Close evidence partition */
db.close();

/* Add all threads */
db = data.getDatabase(tgtPart);
atoms = Queries.getAllAtoms(db, PostsIn);
for (GroundAtom a : atoms) {
	threads.add(a.getArguments()[1]);
}
db.close();
System.out.println(String.format("  Added %d threads", threads.size()));

/* Populate E-step variables */
db = data.getDatabase(eStepPart);
dbPop = new DatabasePopulator(db);
dbPop.populate(InGroup(U,G).getFormula(), submap);
db.close();
System.out.println("  Populated E-step variables");

/* Populate M-step variables */
db = data.getDatabase(mStepPart);
dbPop = new DatabasePopulator(db);
dbPop.populate(InGroup(U,G).getFormula(), submap);
dbPop.populate(PostsIn(U,T).getFormula(), submap);
db.close();
System.out.println("  Populated M-step variables");

System.out.println("Finished data loading.")


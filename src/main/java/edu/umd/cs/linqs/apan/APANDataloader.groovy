package edu.umd.cs.linqs.apan;

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import edu.umd.cs.psl.config.ConfigBundle
import edu.umd.cs.psl.config.ConfigManager
import edu.umd.cs.psl.database.DataStore
import edu.umd.cs.psl.database.Database;
import edu.umd.cs.psl.database.DatabasePopulator;
import edu.umd.cs.psl.database.DatabaseQuery;
import edu.umd.cs.psl.database.Partition
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type
import edu.umd.cs.psl.groovy.PSLModel
import edu.umd.cs.psl.model.argument.ArgumentType
import edu.umd.cs.psl.model.argument.GroundTerm;
import edu.umd.cs.psl.model.argument.Variable;
import edu.umd.cs.psl.ui.loading.InserterUtils;

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
int numGroups = cb.getInt('numGroups', -1);

/* Predicates */
m.add predicate: "Discussion", types: [ArgumentType.UniqueID];
m.add predicate: "Word", types: [ArgumentType.UniqueID, ArgumentType.String];
m.add predicate: "UsedWord", types: [ArgumentType.UniqueID, ArgumentType.UniqueID];
m.add predicate: "Friends", types: [ArgumentType.UniqueID, ArgumentType.UniqueID];
m.add predicate: "PostsIn", types: [ArgumentType.UniqueID, ArgumentType.UniqueID];
m.add predicate: "InGroup", types: [ArgumentType.UniqueID, ArgumentType.Integer];


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

System.out.println("Populating E-/M-step paritions ...");

///* Builds sets of user IDs and target interactions */
//Set<GroundTerm> users = new HashSet<GroundTerm>();
//Set<GroundTerm> targetInteractions = new HashSet<GroundTerm>();
//
//Database db = data.getDatabase(targetInteractionsPart);
//DatabaseQuery query = new DatabaseQuery((TargetInteraction(UserA, UserB)).getFormula());
//results = db.executeQuery(query);
//for (int iResults = 0; iResults < results.size(); iResults++) {
//    users.add(results.get(iResults, UserA.toAtomVariable()));
//    targetInteractions.add(results.get(iResults, UserB.toAtomVariable()));
//}
//
//System.out.println("Num users: " + users.size());
//System.out.println("Target user interactions: " + targetInteractions.size());
//db.close()
//
///* Builds set of cluster terms */
//Set<GroundTerm> clusters = new HashSet<GroundTerm>();
//for (int iCluster = 0; iCluster < numClusters; iCluster++)
//    clusters.add(data.getUniqueID(iCluster));
//    
///* Performs population */
//Map<Variable, Set<GroundTerm>> submap = new HashMap<Variable, Set<GroundTerm>>();
//submap.put(new Variable("U"), users);
//submap.put(new Variable("C"), clusters);
//submap.put(new Variable("TI"), targetInteractions);
//
//db = data.getDatabase(eStepClusterPart);
//DatabasePopulator populator = new DatabasePopulator(db);
//populator.populate(InCluster(U, C).getFormula(), submap);
//db.close();
//System.out.println("Populated E step cluster atoms");
//
//db = data.getDatabase(mStepPart);
//populator = new DatabasePopulator(db);
//populator.populate(InCluster(U, C).getFormula(), submap);
//populator.populate(TargetInteraction(U, TI).getFormula(), submap);
//db.close();
//System.out.println("Populated M step cluster and TargetInteraction atoms");


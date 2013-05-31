package edu.umd.cs.linqs.apan;

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import edu.umd.cs.psl.application.inference.MPEInference
import edu.umd.cs.psl.application.learning.weight.WeightLearningApplication
import edu.umd.cs.psl.application.learning.weight.maxlikelihood.MaxLikelihoodMPE
import edu.umd.cs.psl.config.ConfigBundle
import edu.umd.cs.psl.config.ConfigManager
import edu.umd.cs.psl.database.DataStore
import edu.umd.cs.psl.database.Database
import edu.umd.cs.psl.database.Partition
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type
import edu.umd.cs.psl.groovy.PSLModel
import edu.umd.cs.psl.groovy.PredicateConstraint
import edu.umd.cs.psl.model.argument.GroundTerm
import edu.umd.cs.psl.model.argument.UniqueID
import edu.umd.cs.psl.model.atom.GroundAtom
import edu.umd.cs.psl.model.kernel.Kernel
import edu.umd.cs.psl.util.database.Queries


/** DATA LOADER **/

evaluate(new File("src/main/java/edu/umd/cs/linqs/apan/APANDataloader.groovy"));


/** INITIALIZATION **/

Logger log = LoggerFactory.getLogger(this.class)

ConfigManager cm = ConfigManager.getManager()
ConfigBundle cb = cm.getBundle("apan")
//cb.setProperty("rdbmsdatastore.usestringids", true)

def defaultPath = System.getProperty("java.io.tmpdir")
String dbpath = cb.getString("dbpath", defaultPath + File.separator + "apan")
DataStore data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, dbpath, false), cb)

PSLModel m = new PSLModel(this, data);


/** PARTITIONS **/

Partition eviPart = new Partition(cb.getInt("eviPart", -1));
Partition tgtPart = new Partition(cb.getInt("tgtPart", -1));
Partition eStepPart = new Partition(cb.getInt("eStepPart", -1));
Partition mStepPart = new Partition(cb.getInt("mStepPart", -1));


/** MODEL PARAMETERS **/

//int numGroups = cb.getInt("numGroups", -1);
boolean sq = cb.getBoolean("squared", true);

/* Initialize weights using random Gaussian values */
Random rand = new Random(311311);
Map<Kernel,Double> initWeights = new HashMap<Kernel,Double>();
double variance = cb.getDouble("weightVariance", 1.0);
double meanWordWeight = cb.getDouble("meanWordWeight", 1.0);
double meanFriendWeight = cb.getDouble("meanFriendWeight", 1.0);
double meanGroupWeight = cb.getDouble("meanGroupWeight", 1.0);
double meanPriorWeight = cb.getDouble("meanPriorWeight", 1.0);
double seedWeightMult = cb.getDouble("seedWeightMult", 10.0);

/* Predicate groups */
def evidencePreds = [Discussion, Word, Friends, UsedWord] as Set;
def labelPreds = [InGroup, PostsIn] as Set;


/** RULES **/

/* Open an evidence DB for queries */
Database eviDB = data.getDatabase(eviPart);

/* For storing kernels/weights */
double initWeight;
Kernel k;

/* Friends tend to be in the same group */
initWeight = Math.max(meanFriendWeight + rand.nextGaussian() * variance, 0.05);
k = m.add rule: ( Friends(U1,U2) & InGroup(U1,G) ) >> InGroup(U2,G), weight: initWeight, squared: sq;
initWeights.put(k, initWeight);

/* Relates word usage to group membership */
Set<GroundAtom> words = Queries.getAllAtoms(eviDB, Word);
Set<GroundAtom> groups = Queries.getAllAtoms(eviDB, Group);
for (GroundAtom w : words) {
	UniqueID wordID = (UniqueID) w.getArguments()[0];
	for (GroundAtom g : groups) {
		UniqueID groupID = (UniqueID) g.getArguments()[0];
		initWeight = Math.max(meanWordWeight + rand.nextGaussian() * variance, 0.05);
		/* Apply heuristic weighting */
		if (
			   (wordID == data.getUniqueID(7) && groupID == data.getUniqueID(1))
			|| (wordID == data.getUniqueID(2) && groupID == data.getUniqueID(2))
			|| (wordID == data.getUniqueID(8) && groupID == data.getUniqueID(3))
			|| (wordID == data.getUniqueID(25) && groupID == data.getUniqueID(4))
			) {
			initWeight *= seedWeightMult;
		}
		k = m.add rule: UsedWord(U,wordID) >> InGroup(U,groupID), weight: initWeight, squared: sq;
		initWeights.putAt(k, initWeight);
	}
}

/* Relates how groups post in discussions */
Set<GroundAtom> threads = Queries.getAllAtoms(eviDB, Discussion);
for (GroundAtom t : threads) {
	UniqueID threadID = (UniqueID) t.getArguments()[0];
	for (GroundAtom g : groups) {
		UniqueID groupID = (UniqueID) g.getArguments()[0];
		initWeight = Math.max(meanGroupWeight + rand.nextGaussian() * variance, 0.05);
		k = m.add rule: InGroup(U,groupID) >> PostsIn(U,threadID), weight: initWeight, squared: sq;
		initWeights.put(k, initWeight);
	}
}

/* Priors */
for (GroundAtom g : groups) {
	UniqueID groupID = (UniqueID) g.getArguments()[0];
	m.add rule: ~InGroup(U,groupID), weight: meanPriorWeight, squared: sq;
}
	
/* Constraints */
m.add PredicateConstraint.Functional, on: InGroup;

/* Remember to close the DB! */
eviDB.close();

/* Print model */
System.out.println(m.toString());


/** EXPERIMENT **/

/* Experiment parameters */
def numIters = cb.getInt("numIters", 10);

/* Performs EM */
for (int i = 0; i < numIters; i++) {
	System.out.println("Starting iteration " + i);
	
	/* E step: infers cluster membership */
	Database db = data.getDatabase(eStepPart, evidencePreds + PostsIn, eviPart, tgtPart);
	MPEInference mpe = new MPEInference(m, db, cb);
	mpe.mpeInference();
	mpe.close();
	//db.close();
	
	/* Output memberships to file */
	Set<GroundAtom> groupMemberships = Queries.getAllAtoms(db, InGroup);
	File file = new File("output/InGroup.csv");
	file.createNewFile();
	FileWriter fw = new FileWriter(file);
	for (GroundAtom a : groupMemberships) {
		GroundTerm[] terms = a.getArguments();
		UniqueID u = (UniqueID) terms[0];
		UniqueID g = (UniqueID) terms[1];
		double v = a.getValue();
		fw.write(String.format("%s,%s,%f\n", u.toString(), g.toString(), v));
	}
	fw.flush();
	fw.close();
	db.close();
	
	/* M step: optimizes parameters */
	Database rvDB = data.getDatabase(mStepPart, evidencePreds, eviPart);
	Database obsvDB = data.getDatabase(eStepPart, labelPreds, tgtPart);
	WeightLearningApplication wl = new MaxLikelihoodMPE(m, rvDB, obsvDB, cb);
	wl.learn();
	wl.close();
	rvDB.close();
	obsvDB.close();
}

System.out.println("Completed clustering.");
System.out.println(m);


/** GROUP MEMBERSHIPS **/

/* Infer group memberships */
System.out.println("Inferring group memberships ...");
Database db = data.getDatabase(eStepPart, evidencePreds + PostsIn, eviPart, tgtPart);
MPEInference mpe = new MPEInference(m, db, cb);
mpe.mpeInference();
mpe.close();

/* Output memberships to file */
System.out.println("Writing group memberships to file ...");
Set<GroundAtom> groupMemberships = Queries.getAllAtoms(db, InGroup);
File file = new File("output/InGroup.csv");
file.createNewFile();
FileWriter fw = new FileWriter(file);
for (GroundAtom a : groupMemberships) {
	GroundTerm[] terms = a.getArguments();
	UniqueID u = (UniqueID) terms[0];
	UniqueID g = (UniqueID) terms[1];
	double v = a.getValue();
	fw.write(String.format("%s,%s,%f\n", u.toString(), g.toString(), v));
}
fw.flush();
fw.close();
db.close();

System.out.println("Done!");




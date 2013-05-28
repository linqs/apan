package edu.umd.cs.linqs.apan;

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import edu.umd.cs.psl.application.inference.MPEInference;
import edu.umd.cs.psl.application.learning.weight.WeightLearningApplication;
import edu.umd.cs.psl.application.learning.weight.maxlikelihood.MaxLikelihoodMPE;
import edu.umd.cs.psl.application.learning.weight.maxlikelihood.MaxPseudoLikelihood;
import edu.umd.cs.psl.config.ConfigBundle
import edu.umd.cs.psl.config.ConfigManager
import edu.umd.cs.psl.database.DataStore
import edu.umd.cs.psl.database.Database;
import edu.umd.cs.psl.database.DatabasePopulator;
import edu.umd.cs.psl.database.DatabaseQuery;
import edu.umd.cs.psl.database.Partition
import edu.umd.cs.psl.database.ResultList;
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type
import edu.umd.cs.psl.groovy.PSLModel
import edu.umd.cs.psl.groovy.PredicateConstraint;
import edu.umd.cs.psl.model.argument.ArgumentType
import edu.umd.cs.psl.model.argument.GroundTerm;
import edu.umd.cs.psl.model.argument.UniqueID
import edu.umd.cs.psl.model.argument.Variable;
import edu.umd.cs.psl.model.atom.GroundAtom
import edu.umd.cs.psl.model.atom.RandomVariableAtom;
import edu.umd.cs.psl.model.kernel.Kernel;
import edu.umd.cs.psl.model.kernel.predicateconstraint.DomainRangeConstraintKernel;
import edu.umd.cs.psl.model.kernel.predicateconstraint.DomainRangeConstraintType;
import edu.umd.cs.psl.ui.loading.InserterUtils;
import edu.umd.cs.psl.util.database.Queries;

/** INITIALIZATION **/

Logger log = LoggerFactory.getLogger(this.class)

ConfigManager cm = ConfigManager.getManager()
ConfigBundle cb = cm.getBundle("apan")
//cb.setProperty("rdbmsdatastore.usestringids", true)

def defaultPath = System.getProperty("java.io.tmpdir")
String dbpath = cb.getString("dbpath", defaultPath + File.separator + "apan")
DataStore data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, dbpath, false), cb)

PSLModel m = new PSLModel(this, data)


/** PARTITIONS **/

Partition eviPart = new Partition(cb.getInt("eviPart", -1));
Partition tgtPart = new Partition(cb.getInt("tgtPart", -1));
Partition eStepPart = new Partition(cb.getInt('eStepPart', -1));
Partition mStepPart = new Partition(cb.getInt('mStepPart', -1));


/** MODEL PARAMETERS **/

int numGroups = cb.getInt('numGroups', -1);
boolean sq = cb.getBoolean('squared', true);


/* Initialize weights using random Gaussian values */
Random rand = new Random(311311);
Map<Kernel,Double> initWeights = new HashMap<Kernel,Double>();
double variance = cb.getDouble('weightVariance', 1.0);
double meanWordWeight = cb.getDouble('meanWordWeight', -1.0);
double meanFriendWeight = cb.getDouble('meanFriendWeight', -1.0);
double meanGroupWeight = cb.getDouble('meanGroupWeight', -1.0);
double meanPriorWeight = cb.getDouble('meanPriorWeight', -1.0);

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
for (GroundAtom w : words) {
	UniqueID wordID = (UniqueID) w.getArguments()[0];
	for (int g = 0; g < numGroups; g++) {
		initWeight = Math.max(meanWordWeight + rand.nextGaussian() * variance, 0.05);
		k = m.add rule: UsedWord(U,wordID) >> InGroup(U,g+1), weight: initWeight, squared: sq;
		initWeights.putAt(k, initWeight);
	}
}

/* Relates how groups post in discussions */
Set<GroundAtom> threads = Queries.getAllAtoms(eviDB, Discussion);
for (GroundAtom t : threads) {
	UniqueID threadID = (UniqueID) t.getArguments()[0];
	for (int g = 0; g < numGroups; g++) {
		initWeight = Math.max(meanGroupWeight + rand.nextGaussian() * variance, 0.05);
		k = m.add rule: InGroup(U,g+1) >> PostsIn(U,threadID), weight: initWeight, squared: sq;
		initWeights.put(k, initWeight);
	}
}

/* Priors */
for (int g = 0; g < numGroups; g++)
	m.add rule: ~InGroup(U,g+1), weight: meanPriorWeight, squared: sq;
	
/* Constraints */
m.add PredicateConstraint.Functional, on: InGroup;

/* Remember to close the DB! */
eviDB.close();

/* Print model */
//System.out.println(m.toString());


/** EXPERIMENT **/

/* Experiment parameters */
def numIters = cb.getInt('numIters', -1);

///* Performs EM */
//eStepToClose = [UsedHashTag, EvidenceInteraction, TargetInteraction] as Set
//mStepToClose =  [UsedHashTag, EvidenceInteraction] as Set
//for (int i = 0; i < numIters; i++) {
//    /* E step: infers cluster membership */
//    db = data.getDatabase(eStepClusterPart, eStepToClose, backgroundPart, targetInteractionsPart);
//    MPEInference mpe = new MPEInference(m, db, cb);
//    mpe.mpeInference();
//    mpe.close();
//    db.close();
//    
//    /* M step: optimizes parameters */
//    rvDB = data.getDatabase(mStepPart, mStepToClose, backgroundPart);
//    obsvDB = data.getDatabase(eStepClusterPart, [InCluster, TargetInteraction] as Set, targetInteractionsPart);
//    WeightLearningApplication wl = new MaxLikelihoodMPE(m, rvDB, obsvDB, cb);
////    WeightLearningApplication wl = new MaxPseudoLikelihood(m, rvDB, obsvDB, cb);
//    wl.learn();
//    wl.close();
//    rvDB.close();
//    obsvDB.close();
//    
//}
//
//System.out.println("Completed clustering.");
////System.out.println(m);
//for (Kernel kernel : m.getKernels())
//    System.out.println(kernel.toString() + " (Initial: " + initialWeights.get(kernel).toString() + ")");



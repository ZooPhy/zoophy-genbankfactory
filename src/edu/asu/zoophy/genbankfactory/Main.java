package edu.asu.zoophy.genbankfactory;

import java.io.File;
import java.lang.ProcessBuilder.Redirect;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.asu.zoophy.genbankfactory.database.GenBankFactory;
import edu.asu.zoophy.genbankfactory.database.GenBankRecordDAOInt;
import edu.asu.zoophy.genbankfactory.database.GenBankRecordSqlDAO;
import edu.asu.zoophy.genbankfactory.database.funnel.VirusFunnel;
import edu.asu.zoophy.genbankfactory.index.Indexer;
import edu.asu.zoophy.genbankfactory.utils.normalizer.date.DateNormalizer;
import edu.asu.zoophy.genbankfactory.utils.normalizer.gene.GeneNormalizer;
import edu.asu.zoophy.genbankfactory.utils.normalizer.gene.HantaNormalizer;
import edu.asu.zoophy.genbankfactory.utils.normalizer.gene.ProductChecker;
import edu.asu.zoophy.genbankfactory.utils.normalizer.gene.WNVNormalizer;
import edu.asu.zoophy.genbankfactory.utils.pH1N1.PH1N1Inserter;
import edu.asu.zoophy.genbankfactory.utils.predictor.PredictorInserter;
import edu.asu.zoophy.genbankfactory.utils.taxonomy.inserter.HostAligner;
import edu.asu.zoophy.genbankfactory.utils.taxonomy.inserter.HostNormalizer;
import edu.asu.zoophy.genbankfactory.utils.taxonomy.inserter.TaxonomyInserter;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider.RP_PROVIDED_RESOURCES;

/**
 * Main class when executing the data jump via .jar
 * Originally the main method was in the GenBankFactory class, but for some reason java suddenly stopped being able to find it in there.
 * @author demetri
 */
public class Main {

	private static GenBankRecordDAOInt dao;
	private static String filter = null;
	private static Logger log = Logger.getLogger("Main");

	/**
	 * Main method for running the GenBankFactor
	 * @param args refer to help method
	 */
	public static void main(String[] args) {
		try {
			GenBankFactory gbFact;
			TaxonomyInserter taxo = null;
	    	if (args.length < 1) {
	    		log.log(Level.SEVERE, "ERROR! Please specify arguments. Use \"help\" for jar argument instructions.");
	    		System.exit(1);
	    	}
	    	else if (args[0].equalsIgnoreCase("help")) {
	    		help();
	    		System.exit(0);
	    	}
	    	else if (args.length < 5 && args[0].equalsIgnoreCase("dump")) {
	    		if (args.length > 3) {
	    			if (args[2].equals("-f") && args.length == 4) {
	    				filter = args[3];
	    			}
	    			else {
	    				throw new Exception("Invalid command line arguments! Use \"help\" for jar argument instructions.");
	    			}
	    		}
	    		gbFact = GenBankFactory.getInstance();
	 			if (args.length == 1) {
	 				log.warning("WARNING: the DB will not be created/cleared for the data dump. Appending existing DB data.");
	 				TaxonomyInserter.downloadNewTree(gbFact.getProperty("TaxDumpURL"), gbFact.getProperty("TaxDumpFolder"));
					taxo = new TaxonomyInserter(gbFact.getProperty("TaxDumpFolder"));
					taxo.insertTaxo();
	 			}
	 			else if(args[1].equalsIgnoreCase("clean")) {
					//Can be changed to GenBankRecordHibernateDAO to switch concrete implementation from SQL to Hibernate//
			    	dao = new GenBankRecordSqlDAO();
					dao.clearTables(); //we should be dumping into a fresh DB, so this is mainly for testing//
					//insert/align taxonomy//
					TaxonomyInserter.downloadNewTree(gbFact.getProperty("TaxDumpURL"), gbFact.getProperty("TaxDumpFolder"));
					taxo = new TaxonomyInserter(gbFact.getProperty("TaxDumpFolder"));
					taxo.insertTaxo();
	 			}
				else if (args[1].equalsIgnoreCase("create")) {
					//Can be changed to GenBankRecordHibernateDAO to switch concrete implementation from SQL to Hibernate//
			    	dao = new GenBankRecordSqlDAO();
					dao.createTables();
					//insert/align taxonomy//
					TaxonomyInserter.downloadNewTree(gbFact.getProperty("TaxDumpURL"), gbFact.getProperty("TaxDumpFolder"));
					taxo = new TaxonomyInserter(gbFact.getProperty("TaxDumpFolder"));
					taxo.insertTaxo();
					PredictorInserter.insertData();
				}
				else {
					throw new Exception("Invalid command line arguments! Use \"help\" for jar argument instructions.");
				}
	 			//Parse Records and Dump//
				gbFact.getFiles(filter);
				//Update Host TaxonIDs//
				HostNormalizer hostNorm = new HostAligner();
				hostNorm.updateHosts();
				//update dates//
				DateNormalizer dateNorm = DateNormalizer.getInstance();
	 			dateNorm.normalizeDates();
	 			//Update GeoName Locations//
				runGeonameUpdater();
				//Identify pH1N1 sequences//
				PH1N1Inserter.updateSequences(gbFact.getProperty("PH1N1List"));
				//Create Big Index//
				Indexer indexer = new Indexer(gbFact.getProperty("BigIndex"));
				indexer.index();
				//Collect possible missing Genes//
				HantaNormalizer hantaNorm = new HantaNormalizer();
				hantaNorm.normalizeSegments();
				WNVNormalizer wnvNorm = new WNVNormalizer();
	    		wnvNorm.normalizeNotes();
	    		ProductChecker poductCheck = ProductChecker.getInstance();
	    		poductCheck.checkProducts();
				//clear small DB//
				log.info("Switching to Small DB");
				gbFact.switchDB(gbFact.getProperty("SmallDB"));
				dao.clearTables();
				taxo = new TaxonomyInserter(gbFact.getProperty("TaxDumpFolder"));
				taxo.insertTaxo();
	 			//funnel to UI DB//
	 			VirusFunnel funnel = new VirusFunnel(gbFact.getProperty("BigIndex"), gbFact.getProperty("SmallDB"), gbFact.getProperty("BigDB"));
	 			funnel.funnel();
				//update gene names in UI DB//
				GeneNormalizer gn = GeneNormalizer.getInstance();
				gn.normalizeGeneNames(); //only to be used on small UI DB
				//Create UI Index//
				indexer = new Indexer(gbFact.getProperty("SmallIndex"));
				indexer.index();
	    	}
	    	else if (args.length < 2 && args[0].equalsIgnoreCase("index")) {
	 			log.warning("Only Indexing the current DB, no data dumping will occur.");
	    		gbFact = GenBankFactory.getInstance();
	    		TaxonomyInserter.downloadNewTree(gbFact.getProperty("TaxDumpURL"), gbFact.getProperty("TaxDumpFolder"));
	    		Indexer indexer = new Indexer(gbFact.getProperty("BigIndex"));
				indexer.index();
	    	}
	    	else if (args.length < 2 && args[0].equalsIgnoreCase("funnel")) {
	 			log.warning("Funneling big DB to small UI DB...");
	    		gbFact = GenBankFactory.getInstance();
	    		//clear small DB//
				log.info("Switching to Small DB");
				gbFact.switchDB(gbFact.getProperty("SmallDB"));
				dao.clearTables();
				TaxonomyInserter.downloadNewTree(gbFact.getProperty("TaxDumpURL"), gbFact.getProperty("TaxDumpFolder"));
				taxo = new TaxonomyInserter(gbFact.getProperty("TaxDumpFolder"));
				taxo.insertTaxo();
	 			//funnel to UI DB//
	 			VirusFunnel funnel = new VirusFunnel(gbFact.getProperty("BigIndex"), gbFact.getProperty("SmallDB"), gbFact.getProperty("BigDB"));
	 			funnel.funnel();
				//update gene names in UI DB//
				GeneNormalizer gn = GeneNormalizer.getInstance();
				gn.normalizeGeneNames(); //only to be used on small UI DB
				//Create UI Index//
				Indexer indexer = new Indexer(gbFact.getProperty("SmallIndex"));
				indexer.index();
	    	}
	    	else if (args.length < 3 && args[0].equalsIgnoreCase("normalize")) {
	    		if (args[1].equalsIgnoreCase("host")) {
	    			gbFact = GenBankFactory.getInstance();
		 			HostNormalizer hn = new HostAligner();
					hn.updateHosts();
	    		}
	    		else if (args[1].equalsIgnoreCase("location")) {
	    			gbFact = GenBankFactory.getInstance();
	    			runGeonameUpdater();
	    		}
	    		else if (args[1].equalsIgnoreCase("gene")) {
	    			gbFact = GenBankFactory.getInstance();
		 			GeneNormalizer gn = GeneNormalizer.getInstance();
		 			gn.normalizeGeneNames();
	    		}
	    		else if (args[1].equalsIgnoreCase("date")) {
	    			gbFact = GenBankFactory.getInstance();
		 			DateNormalizer dn = DateNormalizer.getInstance();
		 			dn.normalizeDates();
	    		}
	    		else if (args[1].equalsIgnoreCase("ph1n1")) {
	    			gbFact = GenBankFactory.getInstance();
	    			PH1N1Inserter.updateSequences(gbFact.getProperty("PH1N1List"));
	    		}
	    		else {
	    			throw new Exception("Invalid command line arguments! Use \"help\" for jar argument instructions.");
	    		}
	    	}
	    	else {
	    		log.log(Level.SEVERE, "ERROR! Unrecognized command. Use \"help\" for jar argument instructions.");
	    	}
	    	log.info("GenBankFactory completed.");
			System.exit(0);
		}
		catch(Exception e) {
			log.log(Level.SEVERE, "ERROR running GenBankFactory: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	/**
	 * Runs Tasnia's GeonameUpdater to normalize Geoname Locations
	 * @throws Exception
	 */
	private static void runGeonameUpdater() throws Exception {
		String updaterDir = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("geoname.updater.dir");
		File updaterFolder = new File(updaterDir);
		if (!(updaterFolder.isDirectory() && updaterFolder.exists())) {
			log.log(Level.SEVERE, "GeonameUpdater Error: Updater Directory is invalid: "+updaterDir);
			throw new Exception("GeonameUpdater Error: Updater Directory is invalid: "+updaterDir);
		}
		try {
			ProcessBuilder pb = new ProcessBuilder("java", "-Xmx4G", "-Xms1G", "-jar", updaterDir+"gbmetadataupdater.jar").inheritIO().directory(updaterFolder);
			File geonameUpdaterLog = new File("gb_updater.log");
			pb.redirectOutput(Redirect.appendTo(geonameUpdaterLog));
			pb.redirectError(Redirect.appendTo(geonameUpdaterLog));
			log.info("Starting Process: "+pb.command().toString());
			Process pr = pb.start();
			pr.waitFor();
			if (pr.exitValue() != 0) {
				log.log(Level.SEVERE, "GeonameUpdater failed! with code: "+pr.exitValue());
				throw new Exception("GeonameUpdater failed! with code: "+pr.exitValue());
			}
			log.info("Geoname Updater Complete");
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "GeonameUpdater Error: "+e.getMessage());
			throw e;
		}
	}
	
	/**
	 * .jar argument instructions
	 */
	protected static void help() {
		StringBuilder str = new StringBuilder();
		str.append("GenBankFactory jar argument options:\n");
		str.append("\t'dump' - this will initiate a data dump. It is recomended to specify the dump process by adding one of the following arguments:\n");
		str.append("\t\t'create' - this will assume that the DB is empty and create all necesssary tables before starting the data dump.\n");
		str.append("\t\t'clean' - this will completely truncate and reset the database before starting the data dump.\n");
		str.append("\t\t'-f <filter>' - this may be added at the end of any previously described dump command to specify the specific file types to be dumped.\n");
		str.append("\t\t\tex. using the arguments 'dump clean -f gbvrl' will completely truncate and reset the database, then process and dump all GenBank files that contain 'gbvrl' in their filename.\n");
		str.append("\n\t'index' - this option will skip the data dump stage and only create a new index from existing data.\n");
		str.append("\n\t'normalize' - this option will skip the data dump stage and only normalize existing data.\n");
		str.append("\t\t'host' - this will normalize host taxonomy\n");
		str.append("\t\t'location' - this will normalize locations (not implemented yet)\n");
		str.append("\n\tIMPORTANT NOTES:\n");
		str.append("\t\tDouble check the GenBankFactory.local.properties file to make sure that all file paths and DB connection info is correct.\n");
		str.append("\t\tOnce you run a command, it will proceed without confirmation. Be careful running 'dump clean' in production.\n");
		str.append("\t\tAt the moment, only 1 filter may be used with '-f'. The filter is not regex, just an enhanced string.contains() that can handle null filters.\n");
		log.info(str.toString());
	}
	
}
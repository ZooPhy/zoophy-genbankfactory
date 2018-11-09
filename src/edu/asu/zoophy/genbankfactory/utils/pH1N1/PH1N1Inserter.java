package edu.asu.zoophy.genbankfactory.utils.pH1N1;

import java.io.File;
import java.sql.Connection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;

import org.apache.log4j.Logger;

import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBQuery;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;

public class PH1N1Inserter {
	
	private static Logger log = Logger.getLogger("PH1N1Inserter");
	private static Connection conn;
	private static DBQuery updatPH1N1Query;
	private final static String UPDATE_PH1N1 = "UPDATE \"Sequence_Details\" SET \"pH1N1\"=TRUE WHERE \"Accession\"=?;";

	public static void updateSequences(String pH1N1ListPath) throws Exception {
		Scanner scan = null;
		try {
			log.info("Starting pH1N1 update...");
			Set<String> pH1N1Accessions = new LinkedHashSet<String>(100000);
			File pH1N1List = new File(pH1N1ListPath);
			if (!pH1N1List.exists() || pH1N1List.isDirectory()) {
				throw new Exception("Invalid path to pH1N1 List: "+pH1N1ListPath);
			}
			scan = new Scanner(pH1N1List);
			while (scan.hasNextLine()) {
				pH1N1Accessions.add(scan.nextLine().trim());
			}
			Queue<String> uniquePH1N1Accessions = new LinkedList<String>(pH1N1Accessions);
			pH1N1Accessions.clear();
			updateDB(uniquePH1N1Accessions);
			uniquePH1N1Accessions.clear();
			log.info("pH1N1 update complete.");
		}
		catch (Exception e) {
			log.fatal( "ERROR PH1N1Inserter failed: "+e.getMessage());
			throw new Exception("ERROR PH1N1Inserter failed: "+e.getMessage());
		}
		finally {
			if (scan != null) {
				scan.close();
			}
		}
	}
	
	private static void updateDB(Queue<String> accessions) throws Exception {
		try {
			conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
	    }
	    catch(Exception e) {
	    	log.fatal( "Impossible to Initiate the Resources Provider:"+e.getMessage());
	    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
	    }
		updatPH1N1Query = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, UPDATE_PH1N1);
		int batchCount = 0;
		List<Object> queryParams;
		while (!accessions.isEmpty()) {
			queryParams = new LinkedList<Object>();
			String acc = accessions.remove().trim();
			if (acc.contains("*")) {
				acc = acc.substring(0, acc.indexOf("*"));
			}
			queryParams.add(acc);
			updatPH1N1Query.addBatch(queryParams);
			batchCount++;
			if (batchCount % 25000 == 0 || accessions.isEmpty()) {
				log.info("Running update PH1N1 batch...");
				updatPH1N1Query.executeBatch();
				updatPH1N1Query.close();
				log.info("Update PH1N1 batch complete.");
				if (!accessions.isEmpty()) {
					updatPH1N1Query = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, UPDATE_PH1N1);
				}
			}
		}
		log.info("Successfully identified "+batchCount+" pH1N1 records.");
	}

}

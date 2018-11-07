package edu.asu.zoophy.genbankfactory.utils.predictor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.asu.zoophy.genbankfactory.database.GenBankFactory;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBQuery;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;

public class PredictorInserter {
	
	private static GenBankFactory fact;
	private static final Logger log = Logger.getLogger("PredictorInserter");
	private static final String INSERT_PREDICTORS = "INSERT INTO \"Predictor\" (\"USPS_code\",\"State\",\"Key\",\"Value\",\"Year\") VALUES (?,?,?,?,?)";
	private static Connection conn = null;
	private static List<Object> queryParams;
	private static DBQuery query;
	private static BufferedReader br;
	private static String[] predictorOptions;
	private static List<List<String>> statePredictors;
	private static String csvDelimeter = ",";
	
	public static void insertData() throws Exception {
		try {
			fact = GenBankFactory.getInstance();
			String path = fact.getProperty("predictor.csv");
			log.info("Starting Predictor Insertion process with file: "+path);
			statePredictors = new LinkedList<List<String>>();
			String line = "";
			log.info("reading csv file...");
	        br = new BufferedReader(new FileReader(path));
	        line = br.readLine();
            predictorOptions = line.split(csvDelimeter);
    		while ((line = br.readLine()) != null) {
    			List<String> state = Arrays.asList(line.split(csvDelimeter));
    			statePredictors.add(state);
    		}
    		br.close();
    		log.info("finished reading csv file");
    		log.info("insterting to DB...");
    		try {
    			conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
    	    }
    	    catch (Exception e) {
    	    	log.fatal( "Impossible to Initiate the Resources Provider:"+e.getMessage());
    	    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
    	    }
    		query = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, INSERT_PREDICTORS);
    		for (List<String> state : statePredictors) {
    			String uspsCode = state.get(0);
    			String stateName = state.get(1);
    			for (int i = 2; i < state.size(); i++) {
    				String key = predictorOptions[i];
    				Integer year = null;
    				if (key.contains("_")) {
    					year = Integer.parseInt(key.split("_")[1]);
    					key = key.split("_")[0];
    				}
    				Double value;
    				try {
    					value = Double.parseDouble(state.get(i));
    				}
    				catch (Exception e) {
    					log.warn("Non numberic value: "+state.get(i));
    					value = null;
    				}
    				addPredictorBatch(uspsCode,stateName,key,value,year);
    			}
    		}
    		statePredictors.clear();
    		log.info("batch starting...");
    		query.executeBatch();
    		query.close();
    		log.info("batch finished");
    		log.info("finished insterting to DB");
		}
		catch (Exception e) {
			log.fatal( "ERROR inserting predictors " + e.getMessage());
		}
	}

	private static void addPredictorBatch(String uspsCode, String stateName, String key, Double value, Integer year) throws Exception {
		queryParams = new LinkedList<Object>();
		queryParams.add(uspsCode);
		queryParams.add(stateName);
		queryParams.add(key);
		queryParams.add(value);
		queryParams.add(year);
		query.addBatch(queryParams);
		queryParams.clear();
	}
}

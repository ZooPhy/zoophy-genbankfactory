package edu.asu.zoophy.gbmetadataupdater.controller;


import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.asu.zoophy.gbmetadataupdater.GBMetadataUpdater;
import edu.asu.zoophy.gbmetadataupdater.db.DBQuery;
import edu.asu.zoophy.gbmetadataupdater.utils.ResourceProvider;
import edu.asu.zoophy.gbmetadataupdater.utils.ResourceProvider.RP_PROVIDED_RESOURCES;


/**
 * 
 * The main controller for running the program
 * @author ttahsin
 *
 */
public class GBLargeLatLngUpdaterController implements ControllerInt {
	public enum Dir {
		S("-"), N(""), E(""), W("-");
		String val;
		private Dir(String value) {
			this.val = value;
		}
		public String getVal() {
			return val;
		}
		public static boolean contains(String cur) {
			try {
				Dir.valueOf(cur);
				return true;
			} catch(Exception e) {
				return false;
			}
		}
	}
	final private Logger log = Logger.getLogger(GBMetadataUpdater.class);
	private final static String SELECT_COUNT = "Select count(*) as total from \"Features\" where \"Key\"=\'lat_lon\'";// and \"Accession\" in (\'DQ317692\',\'KC662619\',\'KC662620\',\'KC662621\',\'KC677766\',\'KC677756\',\'KC677757\',\'KC677758\',\'KC677759\',\'KC677760\',\'KC677761\',\'KC677762\',\'KC677763\',\'KC677764\',\'KC677765\',\'KC677767\',\'KC677768\',\'KC677769\',\'KC677770\',\'KC677771\',\'KC677772\',\'KC709814\',\'KC709815\',\'KC709816\',\'KC709817\',\'KC709818\',\'KC709819\',\'KC753354\',\'KC753355\',\'KC753356\',\'KC753357\',\'KC753358\',\'KC753359\',\'KC753360\',\'KC753361\',\'KC753362\',\'KC753363\')";// where \"Location\" is null";

	private final static String SELECT_ALL_ACCESSIONS = "Select \"Accession\", \"Key\", \"Value\" from \"Features\" where \"Key\"=\'lat_lon\'";// and \"Accession\" in (\'DQ317692\',\'KC662619\',\'KC662620\',\'KC662621\',\'KC677766\',\'KC677756\',\'KC677757\',\'KC677758\',\'KC677759\',\'KC677760\',\'KC677761\',\'KC677762\',\'KC677763\',\'KC677764\',\'KC677765\',\'KC677767\',\'KC677768\',\'KC677769\',\'KC677770\',\'KC677771\',\'KC677772\',\'KC709814\',\'KC709815\',\'KC709816\',\'KC709817\',\'KC709818\',\'KC709819\',\'KC753354\',\'KC753355\',\'KC753356\',\'KC753357\',\'KC753358\',\'KC753359\',\'KC753360\',\'KC753361\',\'KC753362\',\'KC753363\')";// where \"Location\" is null";
//	private final static String SELECT_NULL_ACCESSIONS = "Select \"Accession\", \"Strain\", \"Isolate\", \"Organism\", \"Definition\" from \"Sequence_Details\" where \"Accession\"=?";
//	private final static String SELECT_COUNTRY = "Select \"Location\", \"Latitude\", \"Longitude\" from \"Location_GenBank\" where \"Accession\"=?";
//	private final static String INSERT_METADATA = "Insert into \"Location_Geoname\" Values (?,?,?,?,?)";
	private final static String INSERT_METADATA = "Update \"Location_GenBank\" "
			+ "Set \"Latitude\"=?, \"Longitude\"=?"
			+ "Where \"Accession\"=?";
	long numOrigGeoCoded = 0;
	long numCurGeoCoded = 0;
	HashMap<String, Long> sourceCount = new HashMap<String, Long>();
	@Override
	public void run() throws Exception{
		log.info("Starting controller");
		//initializing/declaring db-related objects
		DBQuery query = null;
		DBQuery query0 = null;
	//	DBQuery query1 = null;
//		DBQuery query2 = null;
		DBQuery query3 = null;
		ResultSet result = null;
		ResultSet result0 = null;
//		ResultSet result1 = null;
//		ResultSet result2 = null;
		List<Object> queryParams = new LinkedList<Object>();
		List<Object> queryParams0 = new LinkedList<Object>();
//		List<Object> queryParams1 = new LinkedList<Object>();
//		List<Object> queryParams2 = new LinkedList<Object>();
		List<Object> queryParams3 = new LinkedList<Object>();
		try {
			//query for retrieving the total number of unprocessed records
			query = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_COUNT, queryParams);
			//query for retrieving all unprocessed accession numbers
			query0 = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_ALL_ACCESSIONS, queryParams0);
			//query for retrieving pertinent record metadata given an accession number
	//		query1 = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_ALL_ACCESSIONS, queryParams1);
			//query for retrieving value of "country" field in the record and checking whether the record includes the latitude/longitudes of the location
//			query2 = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_COUNTRY, queryParams2);
			//query for updating the geospatial metadata of a record given it's accession number
			query3 = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_INSERT_BATCH, INSERT_METADATA);
			result = query.executeSelect_MultiRows();
			int numTotal = 0;
			if(result.next()) {
				numTotal = result.getInt("total");
			}
			log.info("Num total is ["+numTotal+"]");
		//	LuceneSearcher searcher = new LuceneSearcher();
			log.info("Retrieving all unprocessed accessions");
			result0 = query0.executeSelect_MultiRows();
			int numProcessed = 0;
			while(result0.next()) {
				String accession = result0.getString(1);
				String lat_lon = result0.getString(3);
				log.info("Processing acession ["+accession+"]");
				String[] values = lat_lon.split(" ");
				if(values.length!=4) {
					log.info("Values length don't match! Use regex for ["+accession+"]");
					continue;
				}
				String latitude = values[0];
				String longitude = values[2];
				if(!isDouble(latitude)||!isDouble(longitude)) {
					log.info("Latitude/Longitude not double. Use regex for [" + accession+"]");
					continue;
				}
				String latDir = values[1];
				String lonDir = values[3];
				if(!Dir.contains(latDir)||!Dir.contains(lonDir)) {
					log.info("Directional values not correct. Use regex for ["+accession+"]");
					continue;
				}
				latitude = Dir.valueOf(latDir).val+latitude;
				longitude = Dir.valueOf(lonDir).val+ longitude;
				if(!isDouble(latitude)||!isDouble(longitude)) {
					log.info("Latitude/Longitude not double after conversion. Check [" + accession+"]");
					continue;
				}
				queryParams3.clear();
				queryParams3.add(Double.parseDouble(latitude));
				queryParams3.add(Double.parseDouble(longitude));
				queryParams3.add(accession);
				this.numOrigGeoCoded++;
				query3.addBatch(queryParams3);
				numProcessed++;
				//log.info("Processed record ["+accession+"] ("+numProcessed+" out of "+numTotal+")");
				if(numProcessed%100==0||numProcessed==numTotal) {
					query3.executeBatch();
					log.info("Executed. Total records completed: ["+numProcessed+"]");
				}
			} 

			query3.executeBatch();
			log.info("Executed. Total records completed: ["+numProcessed+"]");
			query.close();
			query0.close();
	//		query1.close();
	//		query2.close();
			query3.close();
		}catch (Exception e ) {
			 log.fatal(e.getMessage());
			 throw new Exception("Exiting controller due to Exception "+e.getLocalizedMessage());
			 
		}
	}
	
	/**
	 * Checks if a given string can be parsed as double
	 * @param s the string value to check
	 * @return true, if s is a double, false otherwise
	 */
	public static boolean isDouble(String s) {
	    try { 
	       Double.parseDouble(s); 
	    } catch(NumberFormatException e) { 
	        return false; 
	    } catch(NullPointerException e) {
	        return false;
	    }
	    // only got here if we didn't return false
	    return true;
	}

}

package edu.asu.zoophy.gbmetadataupdater.controller;


import java.sql.ResultSet;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.asu.zoophy.gblocationupdater.time.DateParts;
import edu.asu.zoophy.gblocationupdater.time.TimeNormalizer;
import edu.asu.zoophy.gbmetadataupdater.GBMetadataUpdater;
import edu.asu.zoophy.gbmetadataupdater.db.DBQuery;
import edu.asu.zoophy.gbmetadataupdater.disambiguator.Disambiguator;
import edu.asu.zoophy.gbmetadataupdater.utils.ResourceProvider;
import edu.asu.zoophy.gbmetadataupdater.utils.ResourceProvider.RP_PROVIDED_RESOURCES;
public class GBTimeNormalizerController implements ControllerInt{
	final private Logger log = Logger.getLogger(GBMetadataUpdater.class);
	private final static String SELECT_COUNT = "Select count(*) as total from \"Sequence_Details\" LEFT JOIN \"Date_Collection\" ON \"Sequence_Details\".\"Accession\"=\"Date_Collection\".\"Accession\" WHERE \"Date_Collection\".\"Accession\" IS NULL";
	private final static String SELECT_NULL_ACCESSIONS = "Select \"Sequence_Details\".\"Accession\" from \"Sequence_Details\" LEFT JOIN \"Date_Collection\" ON \"Sequence_Details\".\"Accession\"=\"Date_Collection\".\"Accession\" WHERE \"Date_Collection\".\"Accession\" IS NULL";
	private final static String SELECT_ALL_ACCESSIONS = "Select \"Accession\", \"Strain\", \"Isolate\", \"Organism\", \"Definition\", \"Collection_Date\" from \"Sequence_Details\" where \"Accession\"=?";
	private final static String INSERT_METADATA = "Insert into \"Date_Collection\"(\"Accession\", \"Date\", \"Day\", \"Month\", \"Year\", \"NormDate\", \"Accuracy\")"
			+ " Values (?,?,?,?,?,?,?)";

	
	/*private final static String SELECT_COUNT = "Select count(*) as total from \"Sequence_Details\" where \"Accession\" not in (Select \"Accession\" from \"Date_Collection\")";
	private final static String SELECT_NULL_ACCESSIONS = "Select \"Accession\" from \"Sequence_Details\" where \"Accession\" not in (Select \"Accession\" from \"Date_Collection\")";
	private final static String SELECT_ALL_ACCESSIONS = "Select \"Accession\", \"Strain\", \"Isolate\", \"Organism\", \"Definition\", \"Collection_Date\" from \"Sequence_Details\" where \"Accession\"=?";
	private final static String INSERT_METADATA = "Insert into \"Date_Collection\" Values (?,?,?)";
	*/
	/*
	private final static String INSERT_METADATA = "Update \"Location_Geoname_3\" "
			+ "Set \"Geoname_ID\"=?, \"Location\"=?, \"Latitude\"=?, \"Longitude\"=?, \"Type\"=?, \"Country\"=? "
			+ "Where \"Accession\"=?";*/
	long numOrigGeoCoded = 0;
	long numCurGeoCoded = 0;
	HashMap<String, Long> sourceCount = new HashMap<String, Long>();
	
	@Override
	public void run() throws Exception{
		log.info("Starting controller");
		//initializing/declaring db-related objects
		DBQuery countQuery = null;
		DBQuery accQuery = null;
		DBQuery detailsQuery = null;
		DBQuery inserQuery = null;
		//Disambiguator disambiguator = new Disambiguator();
		ResultSet countAcc = null;
		ResultSet allAcc = null;
		ResultSet details = null;
		//ResultSet result2 = null;
		List<Object> countParams = new LinkedList<Object>();
		List<Object> accParams = new LinkedList<Object>();
		List<Object> detailsParams = new LinkedList<Object>();
		List<Object> insertParams = new LinkedList<Object>();
		try {
			//query for retrieving the total number of unprocessed records
			countQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_COUNT, countParams);
			//query for retrieving all unprocessed accession numbers
			accQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_NULL_ACCESSIONS, accParams);
			//query for retrieving pertinent record metadata given an accession number
			detailsQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_ALL_ACCESSIONS, detailsParams);
			//query for updating the geospatial metadata of a record given it's accession number
			inserQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_INSERT_BATCH, INSERT_METADATA);
			countAcc = countQuery.executeSelect_MultiRows();
			int numTotal = 0;
			if(countAcc.next()) {
				numTotal = countAcc.getInt("total");
			}
			log.info("Num total is ["+numTotal+"]");
			TimeNormalizer tn = new TimeNormalizer();
			log.info("Retrieving all unprocessed accessions");
			allAcc = accQuery.executeSelect_MultiRows();
			int numProcessed = 0;
			while(allAcc.next()) {
				String accession = allAcc.getString(1);
				log.info("Processing acession ["+accession+"]");
				if(accession.equals("KC690147")) {
					log.info("check");
				}
				detailsParams.clear();
				detailsParams.add(accession);
				details = detailsQuery.executeSelect_MultiRows();	
				if(details.next()) {
					if(!accession.equals(details.getString(1))) {
						log.info("need to check");
					}
					String strain = details.getString(2);
					if(strain==null) {
						strain="null";
					}
					String isolate = details.getString(3);
					if(isolate==null) {
						isolate="null";
					}
					String organism = details.getString(4);
					if(organism==null) {
						organism="null";
					}
					String definition = details.getString(5);
					if(definition==null) {
						definition="null";
					}
					String date = details.getString(6);
					if(date==null) {
						date = "";
					}
					DateParts normalizedDate = null;
					
					if(date.length()>0) {
						normalizedDate = tn.getNormDateAll(date);
					} else {
						normalizedDate = new DateParts("", "", "", "1000-01-01", 7);
					}
					if(normalizedDate.accuracy==7 && definition.toLowerCase().contains("influenza")) {
						String year = tn.extractYear(organism, strain);
						if(year.length()==4) {
							normalizedDate.year=Integer.parseInt(year);
							normalizedDate.accuracy=4;
							normalizedDate.date = year+normalizedDate.date.substring(4, normalizedDate.date.length());
						}
					}
					LocalDate normDate = LocalDate.parse(normalizedDate.date);
					//log.info(normalizedDate);
					insertParams.clear();
					insertParams.add(accession);
					insertParams.add(date);
					insertParams.add(normalizedDate.day);
					insertParams.add(normalizedDate.month);
					insertParams.add(normalizedDate.year);
					insertParams.add(normDate);
					insertParams.add(normalizedDate.accuracy);
					inserQuery.addBatch(insertParams);
					numProcessed++;
					log.info("Processed record ["+accession+"] ("+numProcessed+" out of "+numTotal+")");
					if(numProcessed%1000==0||numProcessed==numTotal) {
						inserQuery.executeBatch();
						log.info("Executed. Total records completed: ["+numProcessed+"]");
					}
				}
			} 
			
			countQuery.close();
			accQuery.close();
			detailsQuery.close();
			inserQuery.close();
		}catch (Exception exep ) {
			 log.fatal(exep.getMessage());
			 throw new Exception("Exiting controller due to Exception "+exep.getLocalizedMessage());
			 
		}
	}
	

}

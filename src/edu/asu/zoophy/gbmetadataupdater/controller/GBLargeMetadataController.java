package edu.asu.zoophy.gbmetadataupdater.controller;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.asu.zoophy.gbmetadataupdater.GBMetadataUpdater;
import edu.asu.zoophy.gbmetadataupdater.db.DBQuery;
import edu.asu.zoophy.gbmetadataupdater.disambiguator.Disambiguator;
import edu.asu.zoophy.gbmetadataupdater.geonamelucene.LuceneSearcher;
import edu.asu.zoophy.gbmetadataupdater.metadataextractor.GBMetadata;
import edu.asu.zoophy.gbmetadataupdater.metadataextractor.RecordLocationExtractor;
import edu.asu.zoophy.gbmetadataupdater.metadataextractor.RecordLocationExtractorNonInfluenza;
import edu.asu.zoophy.gbmetadataupdater.utils.ResourceProvider;
import edu.asu.zoophy.gbmetadataupdater.utils.ResourceProvider.RP_PROVIDED_RESOURCES;


/**
 * 
 * The main controller for running the program
 * @author ttahsin
 *
 */
public class GBLargeMetadataController implements ControllerInt {
	final private Logger log = Logger.getLogger(GBMetadataUpdater.class);
	private final static String SELECT_COUNT = "Select count(*) as total from \"Location_Geoname\" where \"Location\" is null";
	private final static String SELECT_NULL_ACCESSIONS = "Select \"Accession\" from \"Location_Geoname\" where \"Location\" is null";
	private final static String SELECT_ALL_ACCESSIONS = "Select \"Accession\", \"Strain\", \"Isolate\", \"Organism\", \"Definition\" from \"Sequence_Details\" where \"Accession\"=?";
	private final static String SELECT_COUNTRY = "Select \"Location\", \"Latitude\", \"Longitude\" from \"Location_GenBank\" where \"Accession\"=?";
	private final static String INSERT_METADATA = "Update \"Location_Geoname\" "
			+ "Set \"Geoname_ID\"=?, \"Location\"=?, \"Latitude\"=?, \"Longitude\"=?, \"Type\"=?, \"Country\"=? "
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
		DBQuery query1 = null;
		DBQuery query2 = null;
		DBQuery query3 = null;
		Disambiguator disambiguator = new Disambiguator();
		ResultSet result = null;
		ResultSet result0 = null;
		ResultSet result1 = null;
		ResultSet result2 = null;
		List<Object> queryParams = new LinkedList<Object>();
		List<Object> queryParams0 = new LinkedList<Object>();
		List<Object> queryParams1 = new LinkedList<Object>();
		List<Object> queryParams2 = new LinkedList<Object>();
		List<Object> queryParams3 = new LinkedList<Object>();
		try {
			//query for retrieving the total number of unprocessed records
			query = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_COUNT, queryParams);
			//query for retrieving all unprocessed accession numbers
			query0 = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_NULL_ACCESSIONS, queryParams0);
			//query for retrieving pertinent record metadata given an accession number
			query1 = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_ALL_ACCESSIONS, queryParams1);
			//query for retrieving value of "country" field in the record and checking whether the record includes the latitude/longitudes of the location
			query2 = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_COUNTRY, queryParams2);
			//query for updating the geospatial metadata of a record given it's accession number
			query3 = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_INSERT_BATCH, INSERT_METADATA);
			log.info("query: "+ query.toString());
			result = query.executeSelect_MultiRows();
			int numTotal = 0;
			if(result.next()) {
				numTotal = result.getInt("total");
			}
			log.info("Num total is ["+numTotal+"]");
			LuceneSearcher searcher = new LuceneSearcher();
			RecordLocationExtractor rle = new RecordLocationExtractor(searcher);
			RecordLocationExtractorNonInfluenza rlen = new RecordLocationExtractorNonInfluenza(searcher);
			log.info("Retrieving all unprocessed accessions");
			result0 = query0.executeSelect_MultiRows();
			int numProcessed = 0;
			while(result0.next()) {
				String accession = result0.getString(1);
				queryParams1.clear();
				queryParams1.add(accession);
				result1 = query1.executeSelect_MultiRows();	
				if(result1.next()) {
					if(!accession.equals(result1.getString(1))) {
						log.info("need to check");
					}
					String strain = result1.getString(2);
					if(strain==null) {
						strain="null";
					}
					String isolate = result1.getString(3);
					if(isolate==null) {
						isolate="null";
					}
					String organism = result1.getString(4);
					if(organism==null) {
						organism="null";
					}
					String definition = result1.getString(5);
					if(definition==null) {
						definition="null";
					}
					queryParams2.clear();
					queryParams2.add(accession);
					result2 = query2.executeSelect_MultiRows();
					String country = "null";
					String latitude = "";
					String longitude="";
					if(result2.next()) {
						country  = result2.getString(1);
						if(country==null||country.trim().toLowerCase().equals("unknown")) {
							country = "null";
						}
						latitude = result2.getString(2);
						longitude = result2.getString(3);
					}
					//check if latitude and longitude values exist in GenBank for the record, 
					//if present update table with existing values without further processing the record
					//otherwise extract and disambiguate geospatial metadata from different fields in the record
					if(latitude!=null && latitude.length()>0 && isDouble(latitude) && Double.parseDouble(latitude)!=0 && longitude!=null && longitude.length()>0 && !isDouble(longitude) && Double.parseDouble(longitude)!=0) {
						queryParams3.clear();
						//queryParams3.add(accession);
						queryParams3.add(-1);
						queryParams3.add(country);
						queryParams3.add(Double.parseDouble(latitude));
						queryParams3.add(Double.parseDouble(longitude));
						queryParams3.add(accession);
						this.numOrigGeoCoded++;
						query3.addBatch(queryParams3);
					} else {
						GBMetadata gbm;
						//treat records for influenza viruses differently since their strains are formatted in a specific manner 
						if(definition.toLowerCase().contains("influenza")) {
							gbm = rle.extractGBLocation(country, strain, isolate, organism);
						} else {
							gbm = rlen.extractGBLocation(country, strain, isolate, organism);
						}
						disambiguator.disambiguate(gbm, searcher);
						queryParams3.clear();
						int id = Integer.parseInt(gbm.getID());
						queryParams3.add(id);
						queryParams3.add(gbm.toString());
						queryParams3.add(gbm.getLat());
						queryParams3.add(gbm.getLng());
						queryParams3.add(gbm.getMostSpecificFcode());
						queryParams3.add(gbm.getCountryName());
						queryParams3.add(accession);
						query3.addBatch(queryParams3);
						if(id!=-1) {
							this.numCurGeoCoded++;
						}
						String[] sources = gbm.getAllSources();
						for(String source: sources) {
							source = source.trim().toLowerCase();
							Long count = sourceCount.get(source);
							if(count==null) {
								sourceCount.put(source, (long) 1);
							} else {
								sourceCount.put(source, count+1);
							}
						}
					}
					numProcessed++;
					if(numProcessed%5000==0||numProcessed==numTotal) {
						query3.executeBatch();
						log.info("Executed Query. Total records completed: "+numProcessed+ " out of " + numTotal );
					}
					
				}
			} 
			StringBuilder sb = new StringBuilder();
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date date = new Date();
			sb.append(dateFormat.format(date)); 
			sb.append("Num originally geocoded:"+this.numOrigGeoCoded+"\n");
			sb.append("Num geocoded here:"+this.numCurGeoCoded+"\n");
			sb.append("Printing source count:\n");
			log.info("Num originally geocoded ["+this.numOrigGeoCoded+"]");
			log.info("Num geocoded here [" + this.numCurGeoCoded+"]");
			log.info("Printing source count:");
			for(Entry<String, Long> e: this.sourceCount.entrySet()) {
				log.info("\t"+e.getKey()+":"+e.getValue());
				sb.append(e.getKey()+":"+e.getValue()+"\n");
			}
			BufferedWriter writer = new BufferedWriter(new FileWriter("statsN.txt", true));
			writer.write(sb.toString());
			writer.close();
			query.close();
			query0.close();
			query1.close();
			query2.close();
			query3.close();
		}catch (Exception exep ) {
			StringBuilder sb = new StringBuilder();
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date date = new Date();
			sb.append(dateFormat.format(date)); 
			sb.append("Num originally geocoded:"+this.numOrigGeoCoded+"\n");
			sb.append("Num geocoded here:"+this.numCurGeoCoded+"\n");
			sb.append("Printing source count:\n");
			log.info("Num originally geocoded ["+this.numOrigGeoCoded+"]");
			log.info("Num geocoded here [" + this.numCurGeoCoded+"]");
			log.info("Printing source count:");
			for(Entry<String, Long> e: this.sourceCount.entrySet()) {
				log.info("\t"+e.getKey()+":"+e.getValue());
				sb.append(e.getKey()+":"+e.getValue()+"\n");
			}
			BufferedWriter writer = new BufferedWriter(new FileWriter("statsN.txt", true));
			writer.write(sb.toString());
			writer.close();
			 log.fatal(exep.getMessage());
			 throw new Exception("Exiting controller due to Exception "+exep.getLocalizedMessage());
			 
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

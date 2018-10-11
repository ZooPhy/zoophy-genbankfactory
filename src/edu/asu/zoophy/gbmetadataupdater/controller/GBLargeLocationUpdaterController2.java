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
public class GBLargeLocationUpdaterController2 implements ControllerInt {
	final private Logger log = Logger.getLogger(GBMetadataUpdater.class);
	private final static String SELECT_COUNT = "Select count(*) as total from \"Sequence_Details\" where \"Accession\" not in (Select \"Accession\" from \"Location_Geoname_3\")";
	private final static String SELECT_NULL_ACCESSIONS = "Select \"Accession\" from \"Sequence_Details\" where \"Accession\" not in (Select \"Accession\" from \"Location_Geoname_3\")";
	private final static String SELECT_ALL_ACCESSIONS = "Select \"Accession\", \"Strain\", \"Isolate\", \"Organism\", \"Definition\", \"Comment\" from \"Sequence_Details\" where \"Accession\"=?";
	private final static String SELECT_NOTE = "Select \"Value\" from \"Features\" where \"Key\"=\'note\' and \"Accession\"=?";
	private final static String SELECT_COUNTRY = "Select \"Location\", \"Latitude\", \"Longitude\" from \"Location_GenBank\" where \"Accession\"=?";
	private final static String INSERT_METADATA = "Insert into \"Location_Geoname_3\" Values (?,?,?,?,?,?,?)";
	long numOrigGeoCoded = 0;
	long numCurGeoCoded = 0;
	HashMap<String, Long> sourceCount = new HashMap<String, Long>();
	@Override
	public void run() throws Exception{
		log.info("Starting controller");
		//initializing/declaring db-related objects
		DBQuery countQuery = null;
		DBQuery accessionQuery = null;
		DBQuery metadataQuery = null;
		DBQuery countryQuery = null;
		DBQuery noteQuery = null;
		DBQuery insertQuery = null;
		Disambiguator disambiguator = new Disambiguator();
		ResultSet countResult = null;
		ResultSet accessions = null;
		ResultSet metadata = null;
		ResultSet locMetadata = null;
		ResultSet noteMetadata = null;
		List<Object> countParam = new LinkedList<Object>();
		List<Object> accParam = new LinkedList<Object>();
		List<Object> metadataParam = new LinkedList<Object>();
		List<Object> countryParam = new LinkedList<Object>();
		List<Object> insertParams = new LinkedList<Object>();
		try {
			//query for retrieving the total number of unprocessed records
			countQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_COUNT, countParam);
			//query for retrieving all unprocessed accession numbers
			accessionQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_NULL_ACCESSIONS, accParam);
			//query for retrieving pertinent record metadata given an accession number
			metadataQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_ALL_ACCESSIONS, metadataParam);
			//query for retrieving value of "country" field in the record and checking whether the record includes the latitude/longitudes of the location
			countryQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_COUNTRY, countryParam);
			
			//query for retrieving value of "note" field in the record 
			noteQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_NOTE, countryParam);

			//query for updating the geospatial metadata of a record given it's accession number
			insertQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_INSERT_BATCH, INSERT_METADATA);
			countResult = countQuery.executeSelect_MultiRows();
			int numTotal = 0;
			if(countResult.next()) {
				numTotal = countResult.getInt("total");
			}
			log.info("Num total is ["+numTotal+"]");
			LuceneSearcher searcher = new LuceneSearcher();
			RecordLocationExtractor rle = new RecordLocationExtractor(searcher);
			RecordLocationExtractorNonInfluenza rlen = new RecordLocationExtractorNonInfluenza(searcher);
			log.info("Retrieving all unprocessed accessions");
			accessions = accessionQuery.executeSelect_MultiRows();
			int numProcessed = 0;
			while(accessions.next()) {
				String accession = accessions.getString(1);
				log.info("Processing acession ["+accession+"]");
				metadataParam.clear();
				metadataParam.add(accession);
				metadata = metadataQuery.executeSelect_MultiRows();	
				if(metadata.next()) {
					if(!accession.equals(metadata.getString(1))) {
						log.info("need to check");
					}
					String strain = metadata.getString(2);
					if(strain==null) {
						strain="null";
					}
					String isolate = metadata.getString(3);
					if(isolate==null) {
						isolate="null";
					}
					String organism = metadata.getString(4);
					if(organism==null) {
						organism="null";
					}
					String definition = metadata.getString(5);
					if(definition==null) {
						definition="null";
					}
					String comment = metadata.getString(6);
					if(comment==null) {
						comment="null";
					}
					countryParam.clear();
					countryParam.add(accession);
					locMetadata = countryQuery.executeSelect_MultiRows();
					String country = "null";
					String latitude = "";
					String longitude="";
					if(locMetadata.next()) {
						country  = locMetadata.getString(1);
						if(country==null||country.trim().toLowerCase().equals("unknown")) {
							country = "null";
						}
						latitude = locMetadata.getString(2);
						longitude = locMetadata.getString(3);
					}
					String note = "null";
					noteMetadata = noteQuery.executeSelect_MultiRows();
					if(noteMetadata.next()) {
						String temp = metadata.getString(1);
						if(temp!=null && temp.length()>0) {
							note = temp;
						}
					}
					//check if latitude and longitude values exist in GenBank for the record, 
					//if present update table with existing values without further processing the record
					//otherwise extract and disambiguate geospatial metadata from different fields in the record
					if(latitude!=null && latitude.length()>0 && isDouble(latitude) && Double.parseDouble(latitude)!=0 && longitude!=null && longitude.length()>0 && isDouble(longitude) && Double.parseDouble(longitude)!=0) {
						//queryParams3.clear();
						//queryParams3.add(accession);
						//queryParams3.add(-1);
						//queryParams3.add(country);
						//queryParams3.add(Double.parseDouble(latitude));
						//queryParams3.add(Double.parseDouble(longitude));
						//queryParams3.add(accession);
						this.numOrigGeoCoded++;
						//query3.addBatch(queryParams3);
					} //else {
						GBMetadata gbm;
						//treat records for influenza viruses differently since their strains are formatted in a specific manner 
						if(definition.toLowerCase().contains("influenza")) {
							gbm = rle.extractGBLocation(country, strain, isolate, organism, comment, note);
						} else {
							gbm = rlen.extractGBLocation(country, strain, isolate, organism, comment, note);
						}
						disambiguator.disambiguate(gbm, searcher);
						insertParams.clear();
						int id = Integer.parseInt(gbm.getID());
						insertParams.add(accession);
						insertParams.add(id);
						insertParams.add(gbm.toString());
						insertParams.add(gbm.getLat());
						insertParams.add(gbm.getLng());
						insertParams.add(gbm.getMostSpecificFcode());
						insertParams.add(gbm.getCountryName());
						insertQuery.addBatch(insertParams);
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
					//}
					numProcessed++;
					log.info("Processed record ["+accession+"] ("+numProcessed+" out of "+numTotal+")");
					if(numProcessed%1000==0||numProcessed==numTotal) {
						insertQuery.executeBatch();
						log.info("Executed. Total records completed: ["+numProcessed+"]");
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
			BufferedWriter writer = new BufferedWriter(new FileWriter("statsNN.txt", true));
			writer.write(sb.toString());
			writer.close();
			countQuery.close();
			accessionQuery.close();
			metadataQuery.close();
			countryQuery.close();
			insertQuery.close();
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
			BufferedWriter writer = new BufferedWriter(new FileWriter("statsNN.txt", true));
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

package edu.asu.zoophy.gbmetadataupdater.controller;



import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.ResultSet;
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
/*
 * A class for testing the program for a specific set of records (useful when processing the records throws an Exception
 * @author Tasnia
 */
public class GBLargeMetadataTestController implements ControllerInt {
	final private Logger log = Logger.getLogger(GBMetadataUpdater.class);
	private final static String SELECT_COUNT = "Select count(*) as total from \"Location_Geoname\" where \"Location\" is null";
	private final static String SELECT_NULL_ACCESSIONS = "Select \"Accession\" from \"Location_Geoname_2\" where \"Location\" is null";
	private final static String SELECT_ALL_ACCESSIONS = "Select \"Accession\", \"Strain\", \"Isolate\", \"Organism\", \"Definition\"  from \"Sequence_Details\" where \"Accession\"=?";
	private final static String SELECT_COUNTRY = "Select \"Location\", \"Latitude\", \"Longitude\" from \"Location_GenBank\" where \"Accession\"=?";
//	private final static String INSERT_METADATA = "Insert into \"Location_Geoname\" Values (?,?,?,?,?)";
	private final static String INSERT_METADATA = "Update \"Location_Geoname\" "
			+ "Set \"Geoname_ID\"=?, \"Location\"=?, \"Latitude\"=?, \"Longitude\"=?, \"Type\"=?, \"Country\"=? "
			+ "Where \"Accession\"=?";
	long numOrigGeoCoded = 0;
	long numCurGeoCoded = 0;
	HashMap<String, Long> sourceCount = new HashMap<String, Long>();
	@Override
	public void run() throws Exception{
		log.info("Starting controller");
		//DBQuery query = null;
		DBQuery query0 = null;
		DBQuery query1 = null;
		DBQuery query2 = null;
		DBQuery query3 = null;
		Disambiguator disambiguator = new Disambiguator();
		//ResultSet result = null;
		//ResultSet result0 = null;
		ResultSet result1 = null;
		ResultSet result2 = null;
		List<Object> queryParams = new LinkedList<Object>();
		List<Object> queryParams0 = new LinkedList<Object>();
		List<Object> queryParams1 = new LinkedList<Object>();
		List<Object> queryParams2 = new LinkedList<Object>();
		List<Object> queryParams3 = new LinkedList<Object>();
		try {
			//query = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_COUNT, queryParams);
			//query0 = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_NULL_ACCESSIONS, queryParams0);
			query1 = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_ALL_ACCESSIONS, queryParams1);
			query2 = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_COUNTRY, queryParams2);
			query3 = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_INSERT_BATCH, INSERT_METADATA);
		//	result = query.executeSelect_MultiRows();
			int numTotal = 0;
		//	if(result.next()) {
		//		numTotal = result.getInt("total");
		//	}
		//	log.info("Num total is ["+numTotal+"]");
			LuceneSearcher searcher = new LuceneSearcher();
			RecordLocationExtractor rle = new RecordLocationExtractor(searcher);
			RecordLocationExtractorNonInfluenza rlen = new RecordLocationExtractorNonInfluenza(searcher);
		//	log.info("Retrieving all unprocessed accessions");
		//	result0 = query0.executeSelect_MultiRows();
		
	//		String[] accessions = {"AB523769","AB685364","AB753479","AF127992","AF238274","AF299753","AF325474","AF372393","AF548495","AJ404065","AJ968496","AM273360","AM709656","AY272068","AY371472","AY551600","AY672898","AY734492","AY827311","AY901685","C21625","CY017444","CY043231","CY075671","CY081613","CY108847","CY109801","CY130054","CY136141","CY140061","CY149069","CY171072","CY186575","D45215","DQ061747","DQ102495","DQ463566","DQ484497","DQ878753","EF161166","EF418439","EU281706","EU551817","EU698889","EU925526","EU932343","FJ649021","FJ653166","FJ692205","FJ713532","FJ713735","FN398429","FR669852","GQ180206","GQ211371","GQ918133","GU728123","HQ159608","HQ668836","HQ669420","JF979149","JN040812","JN155679","JN642964","JN650143","JN713624","JQ066760","JQ248385","JQ315101","JQ513749","JQ825145","JQ926510","JX219526","JX414070","JX425483","JX625643","JX913658","KC117010","KC506437","KC762670","KF181330","KF535168","KJ416556","KJ600776","KJ953041","KM048835","KM581055","KM879903","KP287466","KP416709","KP864082","KR075877","KR153862","KR860975","KT370422","KT735558","KT919087","M58421","S65744","U36113"};
		String[] accessions = {"JN696337"};
			int numProcessed = 0;
			numTotal = accessions.length;
			//	while(result0.next()) {
			while(numProcessed<accessions.length) {
				//String accession = result0.getString(1);
				String accession=accessions[numProcessed];
				log.info("Processing acession ["+accession+"]");
				queryParams1.clear();
				queryParams1.add(accession);
				result1 = query1.executeSelect_MultiRows();	
				if(result1.next()) {
					if(!accession.equals(result1.getString(1))) {
						System.out.println("need to check");
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
					//System.out.println(accession+"\t"+strain+"\t"+isolate.toString()+"\t"+organism+"\t"+country+"\n");
					if(latitude!=null && latitude.length()>0 && isDouble(latitude) && Double.parseDouble(latitude)!=0 && longitude!=null && longitude.length()>0 && !isDouble(longitude) && Double.parseDouble(longitude)!=0) {
						queryParams3.clear();
						//queryParams3.add(accession);
						queryParams3.add(-1);
						queryParams3.add(country);
						this.numOrigGeoCoded++;
						queryParams3.add(Double.parseDouble(latitude));
						queryParams3.add(Double.parseDouble(longitude));
						queryParams3.add(accession);
						query3.addBatch(queryParams3);
					} else {
						GBMetadata gbm;
						if(definition.toLowerCase().contains("influenza")) {
							gbm = rle.extractGBLocation(country, strain, isolate, organism);
						} else {
							gbm = rlen.extractGBLocation(country, strain, isolate, organism);
						}					
						log.info(gbm.toString());
						disambiguator.disambiguate(gbm, searcher);
						queryParams3.clear();
						//queryParams3.add(accession);
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
					log.info("Processed record ["+accession+"] ("+numProcessed+" out of "+numTotal+")");
					if(numProcessed%100==0||numProcessed==numTotal) {
						query3.executeBatch();
						log.info("Executed. Total records completed: ["+numProcessed+"]");
					}
					
				}
			} 
			StringBuilder sb = new StringBuilder();
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
			BufferedWriter writer = new BufferedWriter(new FileWriter("stats.txt"));
			writer.write(sb.toString());
			writer.close();
		//	query.close();
		//	query0.close();
			query1.close();
			query2.close();
			query3.close();
		}catch (Exception e ) {
			 log.fatal(e.getMessage());
			 throw new Exception("Exiting controller due to Exception "+e.getLocalizedMessage());
			 
		}
	}
	
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

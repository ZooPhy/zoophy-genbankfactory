package edu.asu.zoophy.gbmetadataupdater.controller;

import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

import edu.asu.zoophy.gbmetadataupdater.db.DBQuery;
import edu.asu.zoophy.gbmetadataupdater.disambiguator.Disambiguator;
import edu.asu.zoophy.gbmetadataupdater.geonamelucene.LuceneSearcher;
import edu.asu.zoophy.gbmetadataupdater.metadataextractor.GBMetadata;
import edu.asu.zoophy.gbmetadataupdater.metadataextractor.RecordLocationExtractor;
import edu.asu.zoophy.gbmetadataupdater.utils.ResourceProvider;
import edu.asu.zoophy.gbmetadataupdater.utils.ResourceProvider.RP_PROVIDED_RESOURCES;

public class GBMetadataController implements ControllerInt {
	private final static String SELECT_ALL_ACCESSIONS = "Select \"Accession\", \"Strain\", \"Isolate\", \"Organism\" from \"Sequence_Details\"";
	private final static String SELECT_COUNTRY = "Select \"Location\", \"Latitude\", \"Longitude\" from \"Location_GenBank\" where \"Accession\"=?";
	private final static String INSERT_METADATA = "Insert into \"Location_Geoname\" Values (?,?,?,?,?)";
	
	@Override
	public void run() throws Exception {
		DBQuery query1 = null;
		DBQuery query2 = null;
		DBQuery query3 = null;
		Disambiguator disambiguator = new Disambiguator();
		ResultSet result1 = null;
		ResultSet result2 = null;
		List<Object> queryParams1 = new LinkedList<Object>();
		List<Object> queryParams2 = new LinkedList<Object>();
		List<Object> queryParams3 = new LinkedList<Object>();
		try {
			query1 = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_ALL_ACCESSIONS, queryParams1);
			query2 = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_COUNTRY, queryParams2);
			query3 = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_INSERT_BATCH, INSERT_METADATA);
			LuceneSearcher searcher = new LuceneSearcher();
			RecordLocationExtractor rle = new RecordLocationExtractor(searcher);
			result1 = query1.executeSelect_MultiRows();
			while(result1.next()) {
				String accession = result1.getString(1);
				if(accession==null) {
					accession="null";
				}
				if(accession.equals("M73775")) {
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
				queryParams2.clear();
				queryParams2.add(accession);
				result2 = query2.executeSelect_MultiRows();
				String country = "null";
				String latitude = "";
				String longitude="";
				if(result2.next()) {
					country  = result2.getString(1);
					if(country==null) {
						country = "null";
					}
					latitude = result2.getString(2);
					longitude = result2.getString(3);
				}
				System.out.println(accession+"\t"+strain+"\t"+isolate.toString()+"\t"+organism+"\t"+country+"\n");
				if(latitude!=null && latitude.length()>0 && longitude!=null && longitude.length()>0) {
					queryParams3.clear();
					queryParams3.add(accession);
					queryParams3.add("-1");
					queryParams3.add(country);
					queryParams3.add(latitude);
					queryParams3.add(longitude);
					query3.addBatch(queryParams3);
				} else {
					GBMetadata gbm= rle.extractGBLocation(country, strain, isolate, organism);
					disambiguator.disambiguate(gbm, searcher);
					queryParams3.clear();
					queryParams3.add(accession);
					queryParams3.add(Integer.parseInt(gbm.getID()));
					queryParams3.add(gbm.toString());
					queryParams3.add(gbm.getLat());
					queryParams3.add(gbm.getLng());
					query3.addBatch(queryParams3);
				}
			} 
			query3.executeBatch();
		}catch (Exception e ) {
			 System.out.println(e.getMessage());
			 throw e;
		}
	}

}

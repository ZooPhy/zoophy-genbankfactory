package edu.asu.zoophy.genbankfactory.utils.geonames;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import edu.asu.zoophy.genbankfactory.database.PossibleLocation;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider.RP_PROVIDED_RESOURCES;

public class ExtractGeonames {
	private final Logger log = Logger.getLogger("ExtractGeonames");
	private static ExtractGeonames eg = null;
	private static Connection conn = null;
	private static String SELECT_QUERY = "select * from \"Location_Geoname\" where \"Geoname_ID\" is not null  ";
	private static String UPDATE_QUERY= "update \"Location_Geoname\" SET \"Location\" = ?, \"Latitude\" = ?, \"Longitude\" = ?, \"Type\" = ?, \"Country\" = ?, \"State\" = ? where \"Accession\" = ? ";
	private ExtractGeonames() throws Exception {
		try {
			conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
	    }
	    catch (Exception e) {
	    	log.fatal( "Impossible to Initiate the Resources Provider:"+e.getMessage());
	    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
	    }
	}

	public static ExtractGeonames getInstance() throws Exception {
		if (eg == null) {
			eg = new ExtractGeonames();
		}
		return eg;
	}

	public void extractGeonames() {

		String luceneIndexLocation = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("geonames.index.location");
		String luceneMappingFile = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("geonames.mapping.file");
		
		
		List<PossibleLocation> input = new ArrayList<PossibleLocation>();
		try {
			PreparedStatement selectQuery = conn.prepareStatement(SELECT_QUERY);
			ResultSet rs = selectQuery.executeQuery();
			
			while (rs.next()) {
				PossibleLocation obj = new PossibleLocation();
				obj.setAccession( rs.getString(1));
				obj.setGeonameId(rs.getInt(2));
				input.add(obj);
			}
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		log.info("size of list :" + input.size());
		
		PreparedStatement preparedStatement = null;
		int count = 0;
		try {
			HashMap<Integer,PossibleLocation> geoLocation = new HashMap<Integer,PossibleLocation>();
			
			LuceneSearcher luceneIndexer  = new LuceneSearcher(luceneIndexLocation,luceneMappingFile);
			
			preparedStatement = conn.prepareStatement(UPDATE_QUERY);
			for (PossibleLocation row :input ) {
				if (row.getGeonameId() != -1 ) {  
					
					if (!geoLocation.containsKey(row.getGeonameId())) {
					
						Result result = luceneIndexer.searchIndex("GeonameId:"+ String.valueOf(row.getGeonameId())  , 1, true);
						PossibleLocation obj = new PossibleLocation();
						if (result.getRecords().size() !=0 ) {
							
							if (result.getRecords().get(0).get("Latitude") != null )
								obj.setLatitude(Double.parseDouble(result.getRecords().get(0).get("Latitude")) );
							if (result.getRecords().get(0).get("Longitude") != null )
								obj.setLongitude(Double.parseDouble(result.getRecords().get(0).get("Longitude")) );
							if (result.getRecords().get(0).get("Name") != null)
								obj.setLocation(result.getRecords().get(0).get("Name").split("\\(")[0].trim());
							if (result.getRecords().get(0).get("Country") != null )
								obj.setCountry(result.getRecords().get(0).get("Country").split("\\(")[0].trim());
							if (result.getRecords().get(0).get("State") != null )
								obj.setState(result.getRecords().get(0).get("State").split("\\(")[0].trim());
							obj.setType(result.getRecords().get(0).get("Code"));
						
						}
						geoLocation.put(row.getGeonameId(), obj);	
					} 	
					preparedStatement.setString(1, geoLocation.get(row.getGeonameId()).getLocation());
					preparedStatement.setDouble(2, geoLocation.get(row.getGeonameId()).getLatitude());
					preparedStatement.setDouble(3, geoLocation.get(row.getGeonameId()).getLongitude());
					preparedStatement.setString(4, geoLocation.get(row.getGeonameId()).getType());
					preparedStatement.setString(5, geoLocation.get(row.getGeonameId()).getCountry());
					preparedStatement.setString(6, geoLocation.get(row.getGeonameId()).getState());
					preparedStatement.setString(7, row.getAccession());
					
					preparedStatement.addBatch();
					count = count + 1;
					
					if( count == 50000 ) {
						count = 0;
						preparedStatement.executeBatch();
						preparedStatement = conn.prepareStatement(UPDATE_QUERY);
						log.info("Updated 50000 records in Location Geoname Table. " );	
					}
				}
			}
			if (count > 0) {
				preparedStatement.executeBatch();
				log.info("Updated remaining records in Location Geoname Table. " );
			}
		} catch (SQLException e) {
			log.fatal("Error occured in extractGeonames: " + e.getMessage());
			e.printStackTrace();
		}catch(Exception e) {
			e.printStackTrace();
			log.fatal("Error occured in extractGeonames: " + e.getMessage());
		}finally {
		    if (preparedStatement != null) {
		        try {
		        	preparedStatement.close();
		        } catch (SQLException e) {
		        	log.fatal("Error occured while closing preparedStatement : " + e.getMessage());
		        }
		    }
		}
	
		
	}

}

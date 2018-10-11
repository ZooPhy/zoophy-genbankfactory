package edu.asu.zoophy.genbankfactory.utils.extractstate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.asu.zoophy.genbankfactory.database.PossibleLocation;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;

public class ExtractState {
	private final Logger log = Logger.getLogger("StateFormatter");
	private static ExtractState es = null;
	private static Connection conn = null;
	private static String SELECT_QUERY = "select * from \"Location_Geoname\" where \"Type\" = 'ADM1'  ";
	private static String UPDATE_QUERY= "update \"Location_Geoname\" SET \"State\" = ? where \"Accession\" = ? ";
	private ExtractState() throws Exception {
		try {
			conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
	    }
	    catch (Exception e) {
	    	log.log(Level.SEVERE, "Impossible to Initiate the Resources Provider:"+e.getMessage());
	    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
	    }
	}

	public static ExtractState getInstance() throws Exception {
		if (es == null) {
			es = new ExtractState();
		}
		return es;
	}
	
	public  void extractState () {

		List<PossibleLocation> input = new ArrayList<PossibleLocation>();
		try {
			PreparedStatement selectQuery = conn.prepareStatement(SELECT_QUERY);
			ResultSet rs = selectQuery.executeQuery();
			
			while (rs.next()) {
				PossibleLocation obj = new PossibleLocation();
				obj.setAccession( rs.getString(1));
				obj.setLocation(rs.getString(3));
				obj.setCountry(rs.getString(7));
				input.add(obj);
			}
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		log.log(Level.INFO,"size of list :" + input.size());
		
		int count = 0;
		try {
			PreparedStatement updateQueryStmt = conn.prepareStatement(UPDATE_QUERY);
			for (int i = 0 ; i < input.size();i++) {
				
				String state = "";
				if ( input.get(i).getLocation() != "")
					state = input.get(i).getLocation().split(",")[0];
					// update Author_institution					
					updateQueryStmt.setString(1, state);
					updateQueryStmt.setString(2, input.get(i).getAccession());
					updateQueryStmt.addBatch();
					
					count = count + 1;
					if( count == 10000 ) {
						count = 0;
						updateQueryStmt.executeBatch();
						updateQueryStmt = conn.prepareStatement(UPDATE_QUERY);
						log.log(Level.INFO,"ExtractState: Updated 10000 records ");
					}
				}
			
			updateQueryStmt.executeBatch();
			
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
}

	

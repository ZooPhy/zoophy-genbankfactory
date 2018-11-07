package edu.asu.zoophy.genbankfactory.utils.formatter.date;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.asu.zoophy.genbankfactory.database.Sequence;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;

public class DateFormatter {
	private final Logger log = Logger.getLogger("DateFormatter");
	private static DateFormatter df = null;
	private static Connection conn = null;
	private static String SELECT_QUERY = "select * from \"Sequence_Details\" where \"Collection_Date\" is not NULL  ";
	private static String UPDATE_QUERY= "update \"Sequence_Details\" SET \"Normalized_Date\" = ? where \"Accession\" = ? ";
	private DateFormatter() throws Exception {
		try {
			conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
	    }
	    catch (Exception e) {
	    	log.fatal( "Impossible to Initiate the Resources Provider:"+e.getMessage());
	    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
	    }
	}

	public static DateFormatter getInstance() throws Exception {
		if (df == null) {
			df = new DateFormatter();
		}
		return df;
	}

	public  void formatDate() {
		
		List<Sequence> input = new ArrayList<Sequence>();
		try {
			
			PreparedStatement selectQuery = conn.prepareStatement(SELECT_QUERY);
			ResultSet rs = selectQuery.executeQuery();
			
			while (rs.next()) {
				Sequence obj = new Sequence();
				obj.setAccession( rs.getString(1));
				obj.setCollection_date(rs.getString(7));
				input.add(obj);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		log.info("size of list :" + input.size());
			
		DateParser dateParser = new DateParser();
		int count = 0;
		try {
			PreparedStatement updateQueryStmt = conn.prepareStatement(UPDATE_QUERY);
			for (int i = 0 ; i < input.size();i++) {
							
					updateQueryStmt.setString(1, dateParser.normalizeDate(input.get(i).getCollection_date()));
					updateQueryStmt.setString(2, input.get(i).getAccession());
					updateQueryStmt.addBatch();
					
					count = count + 1;
					if( count == 10000 ) {
						count = 0;
						updateQueryStmt.executeBatch();
						updateQueryStmt = conn.prepareStatement(UPDATE_QUERY);
						log.info("DateFormatter: Updated 10000 records  ");
					}
					
				}
			
			updateQueryStmt.executeBatch();
						
			} catch (SQLException e) {	
				e.printStackTrace();
			}
	
	}

}

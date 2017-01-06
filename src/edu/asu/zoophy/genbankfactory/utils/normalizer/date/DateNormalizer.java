package edu.asu.zoophy.genbankfactory.utils.normalizer.date;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBQuery;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;

public class DateNormalizer {
	private final Logger log = Logger.getLogger("DateNormalizer");
	private static DateNormalizer dn = null;
	private static Connection conn = null;
	private static final String UPDATE_DATES = "UPDATE \"Sequence_Details\" SET \"Collection_Date\"=? WHERE \"Accession\"=?";
	private static final String PULL_DATES = "SELECT \"Accession\", \"Strain\", \"Isolate\", \"Organism\", \"Definition\" FROM \"Sequence_Details\" WHERE \"Collection_Date\" IS NULL ORDER BY \"Accession\" ASC";
	private Queue<TempDate> dates;
	private List<Object> queryParams;
	private static final int BATCH_SIZE = 25000;
	private int batch_count;
	private DBQuery pullQuery;
	private DBQuery updateQuery;
	
	private int totalDates;

	private DateNormalizer() throws Exception {
		try {
			conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
	    }
	    catch (Exception e) {
	    	log.log(Level.SEVERE, "Impossible to Initiate the Resources Provider:"+e.getMessage());
	    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
	    }
	}

	public static DateNormalizer getInstance() throws Exception {
		if (dn == null) {
			dn = new DateNormalizer();
		}
		return dn;
	}

	public void normalizeDates() {
		try {
			getDates();
			updateDates();
		} 
		catch (Exception e) {
			log.log(Level.SEVERE, "Failed Date Normalization: "+e.getMessage());
		}
	}
	
	private void getDates() throws Exception {
		log.info("Retreiving dates...");
		dates = new LinkedList<TempDate>();
		queryParams = new LinkedList<Object>();
		pullQuery = new DBQuery(conn, DBQuery.QT_SELECT_MULTIPLE_ROWS, PULL_DATES, queryParams);
		ResultSet rs = pullQuery.executeSelect_MultiRows();
		totalDates = 0;
		while (rs.next()) {
			totalDates++;
			TempDate td = new TempDate();
			td.setAccession(rs.getString("Accession"));
			td.setIsolate(rs.getString("Isolate"));
			td.setStrain(rs.getString("Strain"));
			td.setOrganism(rs.getString("Organism"));
			td.setDefinition(rs.getString("Definition"));
			if (td.checkForDate()) {
				dates.add(td);
			}
		}
		pullQuery.close();
		rs.close();
		log.info(dates.size() + " dates found out of "+totalDates+" dates retreived.");
	}

	private void updateDates() throws Exception {
		log.info("Updating dates...");
		batch_count = 0;
		updateQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, UPDATE_DATES);
		while (!dates.isEmpty()) {
			queryParams = new LinkedList<Object>();
			TempDate td = dates.remove();
			queryParams.add(td.getDate());
			queryParams.add(td.getAccession());
			updateQuery.addBatch(queryParams);
			batch_count++;
			if (batch_count >= BATCH_SIZE || dates.isEmpty()) {
				updateQuery.executeBatch();
				updateQuery.close();
				log.info("Dates batch inserted.");
				batch_count = 0;
				if (!dates.isEmpty()) {
					updateQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, UPDATE_DATES);
				}
			}
		}
		log.info("Dates updated.");
	}

}

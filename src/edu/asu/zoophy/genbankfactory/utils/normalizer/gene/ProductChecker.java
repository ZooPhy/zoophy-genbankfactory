package edu.asu.zoophy.genbankfactory.utils.normalizer.gene;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.asu.zoophy.genbankfactory.utils.taxonomy.GenBankTree;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBQuery;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;

public class ProductChecker {
	
	private static final Logger log = Logger.getLogger("ProductChecker");
	private final String PULL_ACCS_MISSING_GENES = "SELECT \"Sequence_Details\".\"Accession\" FROM \"Sequence_Details\" WHERE NOT EXISTS(SELECT \"Gene_Name\" FROM \"Gene\" WHERE \"Gene\".\"Accession\"=\"Sequence_Details\".\"Accession\")";
	private final String PULL_PRODUCTS = "SELECT \"Value\" FROM \"Features\" WHERE \"Key\"='product' AND \"Accession\"=?";
	private final String INSERT_PRODUCTS = "INSERT INTO \"Gene\"(\"Gene_ID\",\"Accession\",\"Gene_Name\",\"Itv\") VALUES(default,?,?,'PRODUCT');";
	private Connection conn = null;
	private List<Object> queryParams;
	private DBQuery query;
	private ResultSet rs;
	private static ProductChecker checker = null;
	GenBankTree gbt;
	
	public static ProductChecker getInstance() throws Exception {
		if (checker == null) {
			checker = new ProductChecker();
		}
		return checker;
	}
	
	private ProductChecker() throws Exception {
		try {
			conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
	    }
	    catch (Exception e) {
	    	log.fatal( "Impossible to Initiate the Resources Provider:"+e.getMessage());
	    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
	    }
	}
	
	public void checkProducts() throws Exception {
		log.info("checking products for hidden genes...");
		Set<String> accs = new HashSet<String>();
		queryParams = new LinkedList<Object>();
		query = new DBQuery(conn, DBQuery.QT_SELECT_MULTIPLE_ROWS, PULL_ACCS_MISSING_GENES, queryParams);
		rs = query.executeSelect_MultiRows();
		queryParams.clear();
		while (rs.next()) {
			accs.add(rs.getString("Accession"));
		}
		query.close();
		rs.close();
		Queue<String> targetAccessions = new LinkedList<String>(accs);
		accs.clear();
		while (!targetAccessions.isEmpty()) {
			String acc = targetAccessions.remove();
			try {
				checkProduct(acc);
				log.info("checked: "+acc);
			}
			catch (Exception e) {
				log.fatal( "Error checking the product for "+acc+": "+e.getMessage());
			}
		}
		accs.clear();
		log.info("products checked");
	}

	private void checkProduct(String acc) throws Exception {
		queryParams.clear(); 
		queryParams.add(acc);
		query = new DBQuery(conn, DBQuery.QT_SELECT_MULTIPLE_ROWS, PULL_PRODUCTS, queryParams);
		rs = query.executeSelect_MultiRows();
		queryParams.clear(); 
		List<String> prods = new LinkedList<String>();
		while (rs.next()) {
			prods.add(rs.getString("Value"));
		}
		query.close();
		rs.close();
		query = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, INSERT_PRODUCTS);
		for (String prod : prods) {
			queryParams.add(acc);
			queryParams.add(prod);
			query.addBatch(queryParams);
			queryParams.clear();
		}
		query.executeBatch();
		query.close();
		prods.clear();
	}
	
}

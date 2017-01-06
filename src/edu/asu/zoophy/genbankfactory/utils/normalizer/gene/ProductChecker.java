package edu.asu.zoophy.genbankfactory.utils.normalizer.gene;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.asu.zoophy.genbankfactory.database.GenBankFactory;
import edu.asu.zoophy.genbankfactory.utils.taxonomy.GenBankTree;
import edu.asu.zoophy.genbankfactory.utils.taxonomy.Node;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBQuery;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;

public class ProductChecker {
	//find all accessions that don't have genes listed//
	private static final Logger log = Logger.getLogger("ProductChecker");
	private final String PULL_ACCS_MISSING_GENES = "SELECT \"Sequence_Details\".\"Accession\" FROM \"Sequence_Details\" WHERE NOT EXISTS(SELECT \"Gene_Name\" FROM \"Gene\" WHERE \"Gene\".\"Accession\"=\"Sequence_Details\".\"Accession\")";
	private final String PULL_PRODUCTS = "SELECT \"Value\" FROM \"Features\" WHERE \"Key\"='product' AND \"Accession\"=?";
	private final String INSERT_PRODUCTS = "INSERT INTO \"Gene\"(\"Gene_ID\",\"Accession\",\"Gene_Name\",\"Itv\") VALUES(default,?,?,'PRODUCT');";
	private Connection conn = null;
	private List<Object> queryParams;
	private DBQuery query;
	private ResultSet rs;
	private static ProductChecker checker = null;
	@SuppressWarnings("unused")
	private static GenBankFactory fact = GenBankFactory.getInstance();
	private List<String> missingFlu;
	private Set<String> fluTaxons;
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
	    	log.log(Level.SEVERE, "Impossible to Initiate the Resources Provider:"+e.getMessage());
	    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
	    }
	}
	
	public void checkProducts() throws Exception {
//		log.info("finding flu");
//		fluFinder();
//		log.info("exiting");
//		System.exit(0);
//		log.log(Level.SEVERE, "SHOULD NOT REACH HERE");
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
		for (String acc : accs) {
			try {
				checkProduct(acc);
				log.info("checked: "+acc);
			}
			catch (Exception e) {
				log.log(Level.SEVERE, "Error checking the product for "+acc+": "+e.getMessage());
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
	
	/**
	 * testing only
	 */
	@SuppressWarnings("unused")
	private void fluFinder() {
		String fluAfinder = "SELECT \"Accession\", \"Tax_ID\", \"Organism\" FROM \"Sequence_Details\" WHERE \"Organism\" LIKE 'Influenza A%' OR \"Definition\" LIKE'Influenza A%';";
		String fluBfinder = "SELECT \"Accession\", \"Tax_ID\", \"Organism\" FROM \"Sequence_Details\" WHERE \"Organism\" LIKE 'Influenza B%' OR \"Definition\" LIKE'Influenza B%';";
		String fluCfinder = "SELECT \"Accession\", \"Tax_ID\", \"Organism\" FROM \"Sequence_Details\" WHERE \"Organism\" LIKE 'Influenza C%' OR \"Definition\" LIKE'Influenza C%';";
		queryParams = new LinkedList<Object>();
		missingFlu = new LinkedList<String>();
		fluTaxons = new HashSet<String>();
		Map<String,String> accs = new HashMap<String,String>(500000);
		BufferedWriter out;
		try {
			gbt = GenBankTree.getInstance();
			out = new BufferedWriter(new FileWriter("missingFluTaxons.txt"));
			query = new DBQuery(conn, DBQuery.QT_SELECT_MULTIPLE_ROWS, fluAfinder, queryParams);
			rs = query.executeSelect_MultiRows();
			queryParams.clear();
			while (rs.next()) {
				String taxOrganism;
				try {
					String type = rs.getString("Organism");
					type = type.substring(type.indexOf("(H"));
					type = type.substring(1, type.indexOf(")"));
					taxOrganism = rs.getInt("Tax_ID") + " | " + type;
				}
				catch (Exception e) {
					taxOrganism = rs.getInt("Tax_ID") + " | " + rs.getString("Organism");
				}
				accs.put(rs.getString("Accession"),taxOrganism);
			}
			query.close();
			rs.close();
//			fact.switchDB("GenBankViruses_UI");
//			try {
//				conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
//		    }
//		    catch (Exception e) {
//		    	log.log(Level.SEVERE, "Impossible to Initiate the Resources Provider:"+e.getMessage());
//		    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
//		    }
			Iterator<Entry<String,String>> it = accs.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry<String,String> pair = it.next();
		        checkIfMarkedInDB(pair);
		    }
			accs.clear();
			log.warning("Found "+missingFlu.size()+" missing FluA accessions under "+fluTaxons.size()+" different tax ids.");
			for (String taxOrganism : fluTaxons) {
				out.write(taxOrganism);
				out.newLine();
			}
			fluTaxons.clear();
//			log.info("inserting missing records...");
//			fact.switchDB("GenBankViruses_Aug22");
//			SmallDbFiller.fillDB(missingFlu);
//			log.info("missing records inserted");
			missingFlu.clear();
			//flu b//
//			fact.switchDB("GenBankViruses_Aug22");
//			try {
//				conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
//		    }
//		    catch (Exception e) {
//		    	log.log(Level.SEVERE, "Impossible to Initiate the Resources Provider:"+e.getMessage());
//		    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
//		    }
			query = new DBQuery(conn, DBQuery.QT_SELECT_MULTIPLE_ROWS, fluBfinder, queryParams);
			rs = query.executeSelect_MultiRows();
			queryParams.clear();
			while (rs.next()) {
				String taxOrganism = rs.getInt("Tax_ID") + " | " + "FluB";
				accs.put(rs.getString("Accession"),taxOrganism);
			}
			query.close();
			rs.close();
//			fact.switchDB("GenBankViruses_UI");
//			try {
//				conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
//		    }
//		    catch (Exception e) {
//		    	log.log(Level.SEVERE, "Impossible to Initiate the Resources Provider:"+e.getMessage());
//		    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
//		    }
			it = accs.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry<String,String> pair = it.next();
		        checkIfMarkedInDB(pair);
		    }
			accs.clear();
			log.warning("Found "+missingFlu.size()+" missing FluB accessions under "+fluTaxons.size()+" different tax ids.");
			
			for (String taxOrganism : fluTaxons) {
				out.write(taxOrganism);
				out.newLine();
			}
			
			fluTaxons.clear();
//			log.info("inserting missing records...");
//			fact.switchDB("GenBankViruses_Aug22");
//			SmallDbFiller.fillDB(missingFlu);
//			log.info("missing records inserted");
			missingFlu.clear();
			//flu c
//			fact.switchDB("GenBankViruses_Aug22");
//			try {
//				conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
//		    }
//		    catch (Exception e) {
//		    	log.log(Level.SEVERE, "Impossible to Initiate the Resources Provider:"+e.getMessage());
//		    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
//		    }
			query = new DBQuery(conn, DBQuery.QT_SELECT_MULTIPLE_ROWS, fluCfinder, queryParams);
			rs = query.executeSelect_MultiRows();
			queryParams.clear();
			while (rs.next()) {
				String taxOrganism = rs.getInt("Tax_ID") + " | " + "FluC";
				accs.put(rs.getString("Accession"),taxOrganism);
			}
			query.close();
			rs.close();
//			fact.switchDB("GenBankViruses_UI");
//			try {
//				conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
//		    }
//		    catch (Exception e) {
//		    	log.log(Level.SEVERE, "Impossible to Initiate the Resources Provider:"+e.getMessage());
//		    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
//		    }
			it = accs.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry<String,String> pair = it.next();
		        checkIfMarkedInDB(pair);
		    }
			accs.clear();
			log.warning("Found "+missingFlu.size()+" missing FluC accessions under "+fluTaxons.size()+" different tax ids.");
			
			for (String taxOrganism : fluTaxons) {
				out.write(taxOrganism);
				out.newLine();
			}
			
			fluTaxons.clear();
//			log.info("inserting missing records...");
//			fact.switchDB("GenBankViruses_Aug22");
//			SmallDbFiller.fillDB(missingFlu);
//			log.info("missing records inserted");
			missingFlu.clear();
			out.close();
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "flu finder failed" + e.getMessage());
		}
	}

	private void checkIfMarkedInDB(Entry<String, String> pair) throws Exception {
		//TODO: just have it check the GBtree. no need to do DB queries...
		int tax = Integer.parseInt(pair.getValue().split(" | ")[0]);
		Node n = gbt.getNode(tax);
		if (n == null) {
			missingFlu.add(pair.getKey());
			fluTaxons.add(pair.getValue());
		}
//		String fluAchecker = "SELECT COUNT(\"Accession\") as count FROM \"Sequence_Details\" WHERE \"Accession\"=?;";
//		queryParams.clear();
//		queryParams.add(pair.getKey());
//		query = new DBQuery(conn, DBQuery.QT_SELECT_MULTIPLE_ROWS, fluAchecker, queryParams);
//		rs = query.executeSelectedRow();
//		if (rs.getLong("count") == 0) {
//			missingFlu.add(pair.getKey());
//			fluTaxons.add(pair.getValue());
//		}
//		queryParams.clear();
//		query.close();
		
//		rs.close();
	}
	
}

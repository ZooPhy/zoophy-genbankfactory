package edu.asu.zoophy.genbankfactory.utils.normalizer.lineage;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.asu.zoophy.genbankfactory.database.GenBankFactory;
import edu.asu.zoophy.genbankfactory.database.GenBankRecord;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBQuery;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;

/**
 * Runs the Influenza B Lineage Normalization
 * @author developerdemetri
 */
public class InfluenzaBLineageNormalizer {

	private Logger log;
	private GenBankFactory fac;
	private static final int BATCH_SIZE = 25000;
	private int batchCount;
	private int totalFluB;
	private int normalizedFluB;
	private Directory indexDirectory = null;
	private IndexSearcher indexSearcher = null;
	private IndexReader reader = null;
	private QueryParser queryParser = null;
	private Query query = null;
	private TopDocs docs = null;
	private final String BIG_INDEX_LOCATION;
	private Connection conn;
	private final String UPDATE_LINEAGE = "UPDATE \"Sequence_Details\" SET \"Lineage\"=? WHERE \"Accession\"=?";
	private DBQuery updateLineageQuery; 
	
	
	public InfluenzaBLineageNormalizer(String bigIndex) {
		log = Logger.getLogger("InfluenzaBLineageNormalizer");
		batchCount = 0;
		totalFluB = 0;
		normalizedFluB = 0;
		BIG_INDEX_LOCATION = bigIndex;
		fac = GenBankFactory.getInstance();
	}
	
	/**
	 * Runs the Influenza B Lineage Normalization
	 */
	public void run() {
		try {
			log.info("Starting Influenza B Lineage Normalization...");
			conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
			updateLineageQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, UPDATE_LINEAGE);
			Queue<GenBankRecord> records = loadFluBRecords();
			totalFluB = records.size();
			GenBankRecord rec = records.poll();
			String lineage = null;
			while (rec != null) {
				lineage = InfluenzaBLineageDetector.assignLineage(rec);
				if (lineage.equals("Victoria") || lineage.equals("Yamagata")) {
					normalizedFluB++;
				}
				addLineageToBatch(rec.getAccession(), lineage);
				rec = records.poll();
			}
			executeLineageToBatch();
			updateLineageQuery.close();
			log.info("Successfully assigned "+normalizedFluB+" out of "+totalFluB+" lineages.");
			log.info("Finished Influenza B Lineage Normalization.");
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Failed Influenza B Lineage Normalization: "+e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Loads Influenza B Records
	 * @return Queue of Influenza B GenBankRecords
	 * @throws IOException 
	 * @throws ParseException 
	 */
	private Queue<GenBankRecord> loadFluBRecords() throws IOException, ParseException {
		try {
			Queue<GenBankRecord> records = new LinkedList<GenBankRecord>();
			indexDirectory = FSDirectory.open(Paths.get(BIG_INDEX_LOCATION));
			reader = DirectoryReader.open(indexDirectory);
			indexSearcher = new IndexSearcher(reader);
			queryParser = new QueryParser("Accession", new KeywordAnalyzer());
			query = queryParser.parse("TaxonID:197912");
	  		docs = indexSearcher.search(query, 1000);//TODO change to higher number
	  		GenBankRecord rec;
	  		for (ScoreDoc scoreDoc : docs.scoreDocs) {
		         Document doc = indexSearcher.doc(scoreDoc.doc);
		         rec = fac.findFullRecord(doc.get("Accession"));
		         records.add(rec);
	  	    }
	  		reader.close();
	  		indexDirectory.close();
			return records;
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Failed to load Influenza B records: "+e.getMessage());
			throw e;
		}
	}
	
	/**
	 * Adds Accession,Lineage pair to SQL update batch
	 * @param accession Accession of record to update
	 * @param lineage Assigned Lineage of record
	 * @throws Exception 
	 */
	private void addLineageToBatch(String accession, String lineage) throws Exception {
		try {
			List<Object> queryParams = new LinkedList<Object>();
			queryParams.add(lineage);
			queryParams.add(accession);
			updateLineageQuery.addBatch(queryParams);
			batchCount++;
			if (batchCount >= BATCH_SIZE) {
				executeLineageToBatch();
			}
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Failed to load Influenza B records: "+e.getMessage());
			throw e;
		}
	}
	
	/**
	 * Executes Accession,Lineage SQL update batch
	 * @throws Exception 
	 */
	private void executeLineageToBatch() throws Exception {
		if (batchCount > 0) {
			updateLineageQuery.executeBatch();
			updateLineageQuery.close();
			updateLineageQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, UPDATE_LINEAGE);
			batchCount = 0;
		}
	}
	
}

package edu.asu.zoophy.genbankfactory.utils.normalizer.gene;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.asu.zoophy.genbankfactory.database.GenBankFactory;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBQuery;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;

/**
 * Hantavirus records tend to label the Gene as the Segment feature, therefore that feature will be checked
 * before running Product checker.
 * @author devdemetri
 */
public class HantaNormalizer {

	private Logger log = Logger.getLogger("HantaSegmentNormalizer");
	private GenBankFactory fact = null;
	private Directory indexDirectory = null;
	private IndexSearcher indexSearcher = null;
	private IndexReader reader = null;
	private QueryParser queryParser = null;
	private Query pullQuery = null;
	private TopDocs docs = null;
	private Set<String> accs;
	private final String RETRIEVE_SEGMENT = "SELECT \"Value\" FROM \"Features\" WHERE \"Accession\"=? AND \"Key\"='segment'";
	private final String INSERT_SEGMENTS = "INSERT INTO \"Gene\"(\"Gene_ID\",\"Accession\",\"Gene_Name\",\"Itv\") VALUES(default,?,?,'SEGMENT')";
	private DBQuery insertQuery = null; 

	
	public HantaNormalizer() throws IOException {
		fact = GenBankFactory.getInstance();
		indexDirectory = FSDirectory.open(Paths.get(fact.getProperty("BigIndex")));
		reader = DirectoryReader.open(indexDirectory);
		indexSearcher = new IndexSearcher(reader);
		queryParser = new QueryParser("Accession", new KeywordAnalyzer());
	}

	/**
	 * 
	 * @throws Exception
	 */
	public void normalizeSegments() throws Exception {
		try {
			Connection conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
			insertQuery =new DBQuery(conn, DBQuery.QT_INSERT_BATCH, INSERT_SEGMENTS);
			log.info("Finding Hanta Accessions with missing Genes...");
			pullQuery = queryParser.parse("TaxonID:11598");
			docs = indexSearcher.search(pullQuery, 1000000);
			accs = new LinkedHashSet<String>(docs.scoreDocs.length, 0.9f);
			for (ScoreDoc scoreDoc : docs.scoreDocs) {
		         Document doc = indexSearcher.doc(scoreDoc.doc);
		         if (doc.getFields("Gene").length == 0) {	
		        	 accs.add(doc.get("Accession"));
		         }
		    }
			List<String> targetAccs = new LinkedList<String>(accs);
			accs.clear();
			log.info("Proccessing Segments...");
			processSegments(targetAccs);
			log.info("Inserting Segments...");
			insertQuery.executeBatch();
			insertQuery.close();
			log.info("Segment Normalizer complete.");
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Hantavirus segment normalization failed: "+e.getMessage());
			throw e;
		}
		finally {
			if (reader != null) {
				reader.close();
			}
			if (indexDirectory != null) {
				indexDirectory.close();
			}
		}
	}

	/**
	 * 
	 * @param targetAccs
	 * @throws Exception
	 */
	private void processSegments(List<String> targetAccs) throws Exception {
		Connection conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
		int counter = 0;
		for (String acc : targetAccs) {
			List<Object> queryParams = new LinkedList<Object>();
			queryParams.add(acc);
			DBQuery pull_segment_query = new DBQuery(conn, DBQuery.QT_SELECT_ONE_ROW, RETRIEVE_SEGMENT, queryParams);
			ResultSet rs = null;
			try {
				rs = pull_segment_query.executeSelectedRow();
				rs.next();
				String segment = rs.getString("Value");
				if (segment != null) {
					segment = segment.trim();
					List<Object> insertParams = new LinkedList<Object>();
					insertParams.add(acc);
					insertParams.add(segment);
					insertQuery.addBatch(insertParams);
					counter++;
					log.info("segment found for: "+acc+" : "+segment);
				}
			}
			catch (SQLException sqle) {
				log.warning("SQL Error proccessing segmentsfor: "+acc+" "+sqle.getMessage());
			}
			finally {
				if (rs != null) {
					rs.close();
				}
				pull_segment_query.close();
			}
		}
		log.info("Hantavirus Genes found in Segment features: "+counter);
	}

}

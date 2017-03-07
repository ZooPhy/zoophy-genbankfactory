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
 * WNV records often have a 'notes' feature that lists the genes encoded. A large portion should be complete genomes as well.
 * @author devdemetri
 */
public class WNVNormalizer {

	private Logger log = Logger.getLogger("WNVNormalizer");
	private GenBankFactory fact = null;
	private Directory indexDirectory = null;
	private IndexSearcher indexSearcher = null;
	private IndexReader reader = null;
	private QueryParser queryParser = null;
	private Query pullQuery = null;
	private TopDocs docs = null;
	private Set<String> accs;
	private final String RETRIEVE_NOTE = "SELECT \"Value\" FROM \"Features\" WHERE \"Accession\"=? AND \"Key\"='note'";
	private final String INSERT_NOTES = "INSERT INTO \"Gene\"(\"Gene_ID\",\"Accession\",\"Gene_Name\",\"Itv\") VALUES(default,?,?,'NOTE')";
	private DBQuery insertQuery = null; 


	public WNVNormalizer() throws IOException {
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
	public void normalizeNotes() throws Exception {
		try {
			Connection conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
			insertQuery =new DBQuery(conn, DBQuery.QT_INSERT_BATCH, INSERT_NOTES);
			log.info("Finding WNV Accessions with missing Genes...");
			pullQuery = queryParser.parse("TaxonID:11082");
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
			log.info("Proccessing Notes...");
			processNotes(targetAccs);
			log.info("Inserting Notes...");
			insertQuery.executeBatch();
			insertQuery.close();
			log.info("WNV Notes Normalizer complete.");
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "WNV note normalization failed: "+e.getMessage());
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
	private void processNotes(List<String> targetAccs) throws Exception {
		Connection conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
		int counter = 0;
		final int total = targetAccs.size();
		Set<String> uniqueNotes = new LinkedHashSet<String>(100);
		while (!targetAccs.isEmpty()) {
			String acc = targetAccs.remove(0);
			List<Object> queryParams = new LinkedList<Object>();
			queryParams.add(acc);
			DBQuery pullNotesQuery = new DBQuery(conn, DBQuery.QT_SELECT_ONE_ROW, RETRIEVE_NOTE, queryParams);
			ResultSet rs = null;
			try {
				rs = pullNotesQuery.executeSelect_MultiRows();
				while (rs.next()) {
					String note = rs.getString("Value");
					if (note != null) {
						note = cleanNote(note);
						if (note != null && !note.isEmpty()) {
							List<Object> insertParams = new LinkedList<Object>();
							insertParams.add(acc);
							insertParams.add(note);
							insertQuery.addBatch(insertParams);
							counter++;
							uniqueNotes.add(note);
						}
					}
				}
			}
			catch (SQLException sqle) {
				log.warning("SQL Error proccessing segmentsfor: "+acc+" "+sqle.getMessage());
			}
			finally {
				if (rs != null) {
					rs.close();
				}
				pullNotesQuery.close();
			}
		}
		for (String note : uniqueNotes) {
			log.info(note);
		}
		log.info("WNV Genes found in Note features: "+counter+" out of "+total);
		
	}

	/**
	 * @param note
	 * @return relevant Gene data from note feature
	 */
	private String cleanNote(String note) {
		// TODO Auto-generated method stub
		return null;
	}
	
	/*
	Patterns to check:
		encodes *
		contains *
		NS*
		E
		M
		pre M
		* M protein
		;* NS5
		PrM
		C-PrM-E
		PreM
		C
		envelope
		membrane
		pre-membrane
		capsid
		includes * 
	 */
	
}

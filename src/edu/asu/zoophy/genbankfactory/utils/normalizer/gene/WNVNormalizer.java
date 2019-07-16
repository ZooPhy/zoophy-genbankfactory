package edu.asu.zoophy.genbankfactory.utils.normalizer.gene;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
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
	private final String RETRIEVE_NOTE = "SELECT \"Value\" FROM \"Features\" WHERE \"Accession\"=? AND \"Key\"='note'";
	private final String INSERT_NOTES = "INSERT INTO \"Gene\"(\"Gene_ID\",\"Accession\",\"Gene_Name\",\"Itv\") VALUES(default,?,?,'NOTE')";
	private DBQuery insertQuery = null;
	private Set<String> allowedExactNotes;


	public WNVNormalizer() throws IOException {
		fact = GenBankFactory.getInstance();
		indexDirectory = FSDirectory.open(Paths.get(fact.getProperty("BigIndex")));
		reader = DirectoryReader.open(indexDirectory);
		indexSearcher = new IndexSearcher(reader);
		queryParser = new QueryParser("Accession", new StandardAnalyzer());
		allowedExactNotes = new HashSet<String>();
		allowedExactNotes.add("e");
		allowedExactNotes.add("m");
		allowedExactNotes.add("c");
		allowedExactNotes.add("envelope");
		allowedExactNotes.add("pre m");
		allowedExactNotes.add("prem");
		allowedExactNotes.add("prm");
		allowedExactNotes.add("pre m");
		allowedExactNotes.add("membrane");
		allowedExactNotes.add("pre-membrane");
		allowedExactNotes.add("capsid");
		allowedExactNotes.add("caspid");
		allowedExactNotes.add("premembrane");
		allowedExactNotes.add("ns");
		allowedExactNotes.add("env");
		allowedExactNotes.add("core");
		allowedExactNotes.add("prM/M");
		allowedExactNotes.add("non-structural");
		allowedExactNotes.add("nonstructural");
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
			pullQuery = queryParser.parse("OrganismID:11082");
			docs = indexSearcher.search(pullQuery, 1000000);
			Set<String> accs = new LinkedHashSet<String>(docs.scoreDocs.length, 0.9f);
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
			log.fatal( "WNV note normalization failed: "+e.getMessage());
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
		while (!targetAccs.isEmpty()) {
			String acc = targetAccs.remove(0);
			List<Object> queryParams = new LinkedList<Object>();
			queryParams.add(acc);
			DBQuery pullNotesQuery = new DBQuery(conn, DBQuery.QT_SELECT_ONE_ROW, RETRIEVE_NOTE, queryParams);
			ResultSet rs = null;
			try {
				rs = pullNotesQuery.executeSelect_MultiRows();
				while (rs.next()) {
					String rawNote = rs.getString("Value");
					if (rawNote != null) {
						List<String> notes = cleanNote(rawNote);
						if (notes != null && !notes.isEmpty()) {
							for (String note : notes) {
								List<Object> insertParams = new LinkedList<Object>();
								insertParams.add(acc);
								insertParams.add(note);
								insertQuery.addBatch(insertParams);
							}
							counter++;
						}
					}
				}
			}
			catch (SQLException sqle) {
				log.warn("SQL Error proccessing segmentsfor: "+acc+" "+sqle.getMessage());
			}
			finally {
				if (rs != null) {
					rs.close();
				}
				pullNotesQuery.close();
			}
		}
		log.info("WNV Genes found in Note features: "+counter+" out of "+total);
	}

	/**
	 * @param note
	 * @return relevant Gene data from note feature
	 */
	private List<String> cleanNote(String note) {
		note = note.trim();
		if (note.isEmpty()) {
			return null;
		}
		Set<String> noteGenes = new HashSet<String>(8);
		if (allowedExactNotes.contains(note.toLowerCase())) {
			noteGenes.add(note);
		}
		else if (note.startsWith("includes")) {
			note = note.substring(note.indexOf("includes")+"includes".length()).trim();
			String[] chunks;
			if (note.contains(",")) {
				chunks = note.split(",");
			}
			else if (note.contains(";")) {
				chunks = note.split(";");
			}
			else {
				chunks = note.split(" ");
			}
			for (String chunk : chunks) {
				chunk = chunk.trim();
				if (chunk.startsWith("NS")) {
					chunk = "NS";
				}
				if (allowedExactNotes.contains(chunk.toLowerCase())) {
					noteGenes.add(chunk);
				}
			}
		}
		else if (note.startsWith("encodes")) {
			note = note.substring(note.indexOf("encodes")+"encodes".length()).trim();
			String[] chunks;
			if (note.contains(",")) {
				chunks = note.split(",");
			}
			else if (note.contains(";")) {
				chunks = note.split(";");
			}
			else {
				chunks = note.split(" ");
			}
			for (String chunk : chunks) {
				chunk = chunk.trim();
				if (chunk.startsWith("NS")) {
					chunk = "NS";
				}
				if (allowedExactNotes.contains(chunk.toLowerCase())) {
					noteGenes.add(chunk);
				}
			}
		}
		else if (note.startsWith("contains")) {
			note = note.substring(note.indexOf("contains")+"contains".length()).trim();
			String[] chunks;
			if (note.contains(",")) {
				chunks = note.split(",");
			}
			else if (note.contains(";")) {
				chunks = note.split(";");
			}
			else {
				chunks = note.split(" ");
			}
			for (String chunk : chunks) {
				chunk = chunk.trim();
				if (chunk.startsWith("NS")) {
					chunk = "NS";
				}
				if (allowedExactNotes.contains(chunk.toLowerCase())) {
					noteGenes.add(chunk);
				}
			}
		}
		else if (note.contains("putative;")) {
			note = note.substring(note.indexOf("putative;")+"putative;".length()).trim();
			if (allowedExactNotes.contains(note.toLowerCase())) {
				noteGenes.add(note);
			}
		}
		else if (note.equalsIgnoreCase("C-PrM-E")) {
			noteGenes.add("C");
			noteGenes.add("PrM");
			noteGenes.add("E");
		}
		else {
			String[] chunks = note.split(" ");
			String chunk = chunks[0].trim();
			if (chunk.startsWith("NS")) {
				chunk = "NS";
			}
			else if (chunk.endsWith(";")) {
				chunk = chunk.substring(chunk.lastIndexOf(";"));
			}
			if (chunk.equalsIgnoreCase("C-prM-E")) {
				noteGenes.add("C");
				noteGenes.add("prM");
				noteGenes.add("E");
			}
			else if (chunk.equalsIgnoreCase("C-NS1")) {
				noteGenes.add("C");
				noteGenes.add("NS");
			}
			else if (allowedExactNotes.contains(chunk.toLowerCase())) {
				noteGenes.add(chunk);
				if (chunks.length > 1 && chunks[1].equalsIgnoreCase("and")) {
					for (int i = 2; i < chunks.length; i++) {
						chunk = chunks[i];
						if (chunk.startsWith("NS")) {
							chunk = "NS";
						}
						else if (chunk.endsWith(";")) {
							chunk = chunk.substring(chunk.lastIndexOf(";"));
						}
						if (allowedExactNotes.contains(chunk.toLowerCase())) {
							noteGenes.add(chunk);
						}
					}
				}
			}
			else if (note.endsWith("NS5") || note.endsWith("NS3")) {
				noteGenes.add("NS");
			}
			else if (note.endsWith("protein-envelope")) {
				noteGenes.add("envelope");
			}
			else if (note.endsWith("protein-premembrane")) {
				noteGenes.add("premembrane");
			}
			else if (note.endsWith("C/pre-M/E")) {
				noteGenes.add("C");
				noteGenes.add("pre-M");
				noteGenes.add("E");
			}
			else if (note.equalsIgnoreCase("contain envelope protein")) {
				noteGenes.add("envelope");
			}
			else {
				return null;
			}
		}
		return new LinkedList<String>(noteGenes);
	}

}

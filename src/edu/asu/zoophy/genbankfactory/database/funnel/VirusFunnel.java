package edu.asu.zoophy.genbankfactory.database.funnel;

import java.nio.file.Paths;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.Logger;

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

public class VirusFunnel {
	
	private Logger log = Logger.getLogger("VirusFunnel");
	private Directory indexDirectory = null;
	private Queue<String> usableAccs = null;
	private HashSet<String> accs = null;
	private IndexSearcher indexSearcher = null;
	private IndexReader reader = null;
	private QueryParser queryParser = null;
	private Query query = null;
	private TopDocs docs = null;
	private final String BIG_INDEX_LOCATION;
	private SmallDBFiller filler = null;
	private int funnelled = 0;
	
	public VirusFunnel(String bigIndex, String smallDB, String largeDB) {
		BIG_INDEX_LOCATION = bigIndex;
		filler = new SmallDBFiller(smallDB, largeDB);
	}
	
	public void funnel() throws Exception {
		try {
			log.info("Starting Virus Funnel...");
			indexDirectory = FSDirectory.open(Paths.get(BIG_INDEX_LOCATION));
			reader = DirectoryReader.open(indexDirectory);
			indexSearcher = new IndexSearcher(reader);
			queryParser = new QueryParser("Accession", new KeywordAnalyzer());
			accs = new HashSet<String>();
			funnelled = 0;
			//WNV//
			query = queryParser.parse("TaxonID:11082");
			docs = indexSearcher.search(query, 1000000);
			for (ScoreDoc scoreDoc : docs.scoreDocs) {
		         Document doc = indexSearcher.doc(scoreDoc.doc);
		         accs.add(doc.get("Accession"));
			}
		    log.info("Funneling "+(accs.size()-funnelled)+" WNV records...");
		    funnelled = accs.size();
		    //Zika//
			query = queryParser.parse("TaxonID:64320");
			docs = indexSearcher.search(query, 1000000);
			for(ScoreDoc scoreDoc : docs.scoreDocs) {
		         Document doc = indexSearcher.doc(scoreDoc.doc);
		         accs.add(doc.get("Accession"));
			}
		    log.info("Funneling "+(accs.size()-funnelled)+" Zika records...");
		    funnelled = accs.size();
			//Ebola//
			query = queryParser.parse("TaxonID:186536");
			docs = indexSearcher.search(query, 1000000);
			for (ScoreDoc scoreDoc : docs.scoreDocs) {
		         Document doc = indexSearcher.doc(scoreDoc.doc);
		         accs.add(doc.get("Accession"));
		    }
		    log.info("Funneling "+(accs.size()-funnelled)+" Ebola records...");
		    funnelled = accs.size();
		    //Hanta//
			query = queryParser.parse("TaxonID:11598");
			docs = indexSearcher.search(query, 1000000);
			for (ScoreDoc scoreDoc : docs.scoreDocs) {
		         Document doc = indexSearcher.doc(scoreDoc.doc);
		         accs.add(doc.get("Accession"));
		    }
		    log.info("Funneling "+(accs.size()-funnelled)+" Hanta records...");
		    funnelled = accs.size();
		    //Rabies//
			query = queryParser.parse("TaxonID:11292");
			docs = indexSearcher.search(query, 1000000);
			for(ScoreDoc scoreDoc : docs.scoreDocs) {
		         Document doc = indexSearcher.doc(scoreDoc.doc);
		         accs.add(doc.get("Accession"));
		    }
		    log.info("Funneling "+(accs.size()-funnelled)+" Rabies records...");
		    funnelled = accs.size();
			//Flu A//
			query = queryParser.parse("TaxonID:197911");
			docs = indexSearcher.search(query, 1000000);
			for (ScoreDoc scoreDoc : docs.scoreDocs) {
		         Document doc = indexSearcher.doc(scoreDoc.doc);
		         accs.add(doc.get("Accession"));
		    }
		    log.info("Funneling "+(accs.size()-funnelled)+" Flu A records...");
		    funnelled = accs.size();
	    	//Flu B//
	  		query = queryParser.parse("TaxonID:197912");
	  		docs = indexSearcher.search(query, 1000000);
	  		for (ScoreDoc scoreDoc : docs.scoreDocs) {
	  	         Document doc = indexSearcher.doc(scoreDoc.doc);
	  	         accs.add(doc.get("Accession"));
	  	    }
	  	    log.info("Funneling "+(accs.size()-funnelled)+" Flu B records...");
	  	    funnelled = accs.size();
	  	    //Flu C//
	  		query = queryParser.parse("TaxonID:197913");
	  		docs = indexSearcher.search(query, 1000000);
	  		for (ScoreDoc scoreDoc : docs.scoreDocs) {
	  	         Document doc = indexSearcher.doc(scoreDoc.doc);
	  	         accs.add(doc.get("Accession"));
	  	    }
	  	    log.info("Funneling "+(accs.size()-funnelled)+" Flu C records...");
	  	    funnelled = accs.size();
	  	    //Flu Dump
		    usableAccs = new LinkedList<String>(accs);
		    accs.clear();
		    log.info("Funneling "+usableAccs.size()+" Total records...");
			filler.fillDB(usableAccs);
			usableAccs.clear();
			funnelled = 0;
		}
		catch (Exception e) {
			log.fatal( "ERROR funneling viruses to small DB: "+e.getMessage());
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
	
}

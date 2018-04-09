package edu.asu.zoophy.gbmetadataupdater.gene;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.FSDirectory;


public class GeneLuceneQuery {

	public static void main(String[] args) {
		GeneLuceneQuery a = new GeneLuceneQuery(); 
		System.out.println(a.searchSymbolGivenGeneID("3654619"));
		System.out.println(a.searchSymbolGivenGeneID("1485890"));
		System.out.println(a.searchGeneIDGivenSymbolAndTaxID("vcd30", "10243"));
		System.out.println(a.searchGeneIDGivenSymbolAndTaxID("na", "679278"));
		System.out.println(a.searchIDForAcc("AF041837"));
		System.out.println(a.searchParentSpeciesGivenTaxID("679278"));
	
	}
	String indexPath1; 
	String indexPath2;
	String indexPath3;
	//String indexPath4;
	IndexSearcher geneSearcher;
	IndexSearcher geneAccSearcher;
	IndexSearcher taxSearcher;
	//IndexSearcher fullTaxSearcher;
	Analyzer analyzer;
	
	public GeneLuceneQuery() {
		this.indexPath1 = "C:/Users/ttasn/Documents/ASU/Research/Zoophy/Data/virusgene";
		this.indexPath2 = "C:/Users/ttasn/Documents/ASU/Research/Zoophy/Data/GeneAccessionMap";
	//	this.indexPath3 = "C:/Users/ttasn/Documents/ASU/Research/Zoophy/Data/taxcat";
		this.indexPath3 = "C:/Users/ttasn/Documents/ASU/Research/Zoophy/Data/taxonomy";
		this.analyzer=new KeywordAnalyzer();
		try {
			this.geneSearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath1))));
			this.geneAccSearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath2))));
			this.taxSearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath3))));
		//	this.fullTaxSearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath4))));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String searchParentSpeciesGivenTaxID(String taxID) {
		try {
			Query q = new QueryParser("tax_id", analyzer).parse("\""+taxID.toLowerCase()+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			taxSearcher.search(q, collector);
			TopDocs docs = taxSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = taxSearcher.doc(docid);
			//    System.out.println("Taxid:"+doc.get("parent_tax_id"));
			    return doc.get("parent_tax_id");
			}	
			return "";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";	
	}
	
	/*
	public String searchParentGivenTaxID(String taxID) {
		try {
			Query q = new QueryParser("tax_id", analyzer).parse("\""+taxID.toLowerCase()+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			fullTaxSearcher.search(q, collector);
			TopDocs docs = fullTaxSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = fullTaxSearcher.doc(docid);
			//    System.out.println("Taxid:"+doc.get("parent_tax_id"));
			    return doc.get("parent_tax_id");
			}	
			return "";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";	
	}*/
	
	public Set<String> searchTaxIDsGivenParent(String taxID) {
		Set<String> taxIDs = new HashSet<String>();
		try {
			Query q = new QueryParser("parent_tax_id", analyzer).parse("\""+taxID.toLowerCase()+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			taxSearcher.search(q, collector);
			TopDocs docs = taxSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;

			// retrieve each matching document from the ScoreDoc arry
			for(int i=0; i<hits.length; i++) {
			    int docid = hits[i].doc;
			    Document doc = taxSearcher.doc(docid);
			//    System.out.println("Taxid:"+doc.get("parent_tax_id"));
			    taxIDs.add(doc.get("tax_id"));
			}	
			return taxIDs;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return taxIDs;	
	}
	
	
	public String searchRankGivenTaxID(String taxID) {
		try {
			Query q = new QueryParser("tax_id", analyzer).parse("\""+taxID.toLowerCase()+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			taxSearcher.search(q, collector);
			TopDocs docs = taxSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = taxSearcher.doc(docid);
			//    System.out.println("Taxid:"+doc.get("parent_tax_id"));
			    return doc.get("rank");
			}	
			return "";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";	
	}
	
	public String searchGeneIDGivenSymbolAndTaxID(String geneName, String taxID) {
		try {
			Query q = new QueryParser("Symbol", analyzer).parse("\""+geneName.toLowerCase()+"\" AND tax_id:\""+taxID+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			geneSearcher.search(q, collector);
			TopDocs docs = geneSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = geneSearcher.doc(docid);
			    return doc.get("GeneID");
			}	
			return "";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	
	public String searchGeneIDGivenSynAndTaxID(String geneName, String taxID) {
		try {
			Query q = new QueryParser("Synonym", analyzer).parse("\""+geneName.toLowerCase()+"\" AND tax_id:\""+taxID+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			geneSearcher.search(q, collector);
			TopDocs docs = geneSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = geneSearcher.doc(docid);
			    return doc.get("GeneID");
			}	
			return "";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	public String searchSymbolGivenGeneID(String ID) {
		try {
			Query q = new QueryParser("GeneID", analyzer).parse("\""+ID.toLowerCase()+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			geneSearcher.search(q, collector);
			TopDocs docs = geneSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = geneSearcher.doc(docid);
		//	    System.out.println("Taxid:"+doc.get("#tax_id"));
			    return doc.get("Symbol");
			}	
			return "";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	
	public String searchSymbolGivenGeneNameAndTaxID(String name, String taxID) {
		try {
			Query q = new QueryParser("Synonym", analyzer).parse("\""+name.toLowerCase()+"\" AND tax_id:\""+taxID+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			geneSearcher.search(q, collector);
			TopDocs docs = geneSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = geneSearcher.doc(docid);
			    return doc.get("Symbol");
			}	
			return "";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	
	
	public Set<String> searchSymbolGivenGeneName(String name) {
		Set<String> symbols = new HashSet<String>();
		try {
			Query q = new QueryParser("Synonym", analyzer).parse("\""+name.toLowerCase()+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			geneSearcher.search(q, collector);
			TopDocs docs = geneSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			
			// retrieve each matching document from the ScoreDoc arry
			for(int i=0; i<hits.length;i++) {
			    int docid = hits[i].doc;
			    Document doc = geneSearcher.doc(docid);
			    symbols.add(doc.get("Symbol"));
			}	
			return symbols;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return symbols;
	}
	
	public boolean isGeneSymbol(String name) {
		try {
			Query q = new QueryParser("Symbol", analyzer).parse("\""+name.toLowerCase()+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			geneSearcher.search(q, collector);
			TopDocs docs = geneSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			if(hits.length>0) {
				return true;
			}
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean isSyn(String name) {
		try {
			Query q = new QueryParser("Synonym", analyzer).parse("\""+name.toLowerCase()+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			geneSearcher.search(q, collector);
			TopDocs docs = geneSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			if(hits.length>0) {
				return true;
			}
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	public String searchIDForAcc(String accession) {
		try {
			Query q = new QueryParser("genomic_nucleotide_accession_version", analyzer).parse("\""+accession+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			geneAccSearcher.search(q, collector);
			TopDocs docs = geneAccSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = geneAccSearcher.doc(docid);
			    return doc.get("GeneID");
			}	
			return "";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	
}

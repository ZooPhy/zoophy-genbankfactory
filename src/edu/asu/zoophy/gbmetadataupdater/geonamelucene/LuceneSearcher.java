package edu.asu.zoophy.gbmetadataupdater.geonamelucene;

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

import edu.asu.zoophy.gbmetadataupdater.metadataextractor.ADM1;
import edu.asu.zoophy.gbmetadataupdater.metadataextractor.Country;
import edu.asu.zoophy.gbmetadataupdater.metadataextractor.LocationPart;



/**
 * A class for searching three lucene idexes:
 * 1) countryIndex which is created using a list of countries
 * 2) adm1Index which is created using all adm1-level location names along with their possible alternate names in GeoNames database
 * 3) geoIndex which is created using all locations in GeoNames database (excluding alternate names)
 * @author tasnia
 *
 */
public class LuceneSearcher {

	String indexPath; 
	IndexSearcher countrySearcher;
	IndexSearcher adm1Searcher;
	IndexSearcher locationSearcher;
	Analyzer analyzer;
	public static void main(String[] args) throws Exception {
		LuceneSearcher ls = new LuceneSearcher();
		Set<LocationPart> lp3s = ls.searchLocation("milan");
		HashSet<String> ccs=new HashSet<String>();
		for(LocationPart curLP: lp3s) {
			//System.out.println(curLP.getCcode());
			ccs.add(curLP.getCcode());
		}
		for(String cc: ccs) {
			
			System.out.println(cc);
		}
		/*
		LocationPart lp = ls.searchCountry("Afghanistan");
		System.out.println(lp.toString());
		LocationPart lp1 = ls.searchCountry("USA");
		System.out.println(lp1.toString());
		Set<LocationPart> lp2 = ls.searchAdm1("Texas");
		for(LocationPart curlp: lp2) {
			System.out.println(curlp.toString());
		}
		System.out.println(ls.mapCcode("Bangladesh", false));
		System.out.println(ls.mapCcode("GB", true)); */
		
	}
	
	public LuceneSearcher() {
		this.indexPath = System.getProperty("user.dir")+"/Resources/index";
		this.analyzer=new KeywordAnalyzer();
		try {
			this.countrySearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath+"/countryIndex"))));
			this.adm1Searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath+"/adm1Index"))));
			this.locationSearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath+"/geoIndex"))));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public LocationPart searchCountry( String country) {
		try {
	
		//	reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath+"/countryIndex")));
			Query q = new QueryParser("country", analyzer).parse("\""+country.toLowerCase().trim()+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			countrySearcher.search(q, collector);
			TopDocs docs = countrySearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			LocationPart c=null;
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = countrySearcher.doc(docid);
			    int i=0;
			    while(doc.get("country").contains(",")&&i<hits.length) {
			    	docid = hits[i].doc;
			    	doc = countrySearcher.doc(docid);
			    	i++;
			    }
		    	c = new Country(doc.get("country"), doc.get("ccode"), Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")));
			}
		    return c;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public LocationPart searchCountryGeoname( String country) {
		try {
			Query q = new QueryParser("name", analyzer).parse("\""+country.toLowerCase()+
					"\" AND (fcode:\"PCL\" OR fcode:\"PCLD\" OR fcode:\"PCLF\" OR fcode:\"PCLI\" "
					+ " OR fcode:\"PCLIX\" OR fcode:\"PCLS\")");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			locationSearcher.search(q, collector);
			TopDocs docs = locationSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			for (int i = 0; i < hits.length; i++) {
			    int docid = hits[i].doc;
			    Document doc = locationSearcher.doc(docid);
			    LocationPart c =  new Country(doc.get("geo_id"), doc.get("name"), doc.get("ccode"), doc.get("fcode"), Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")), Long.parseLong(doc.get("population")));
			    return c;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public LocationPart searchCcodeGeoname( String ccode) {
		try {
			Query q = new QueryParser("ccode", analyzer).parse("\""+ccode.toUpperCase()+
					"\" AND (fcode:\"PCL\" OR fcode:\"PCLD\" OR fcode:\"PCLF\" OR fcode:\"PCLI\" "
					+ " OR fcode:\"PCLIX\" OR fcode:\"PCLS\")");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			locationSearcher.search(q, collector);
			TopDocs docs = locationSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			for (int i = 0; i < hits.length; i++) {
			    int docid = hits[i].doc;
			    Document doc = locationSearcher.doc(docid);
			    LocationPart c =  new Country(doc.get("geo_id"), doc.get("name"), doc.get("ccode"), doc.get("fcode"), Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")), Long.parseLong(doc.get("population")));
			    return c;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public LocationPart searchCcode(String ccode) {
		try {
	
		//	reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath+"/countryIndex")));
			Query q = new QueryParser("ccode", analyzer).parse("\""+ccode+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			countrySearcher.search(q, collector);
			TopDocs docs = countrySearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			LocationPart c=null;
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = countrySearcher.doc(docid);
			    int i=0;
			    while(doc.get("country").contains(",")&&i<hits.length) {
			    	docid = hits[i].doc;
			    	doc = countrySearcher.doc(docid);
			    	i++;
			    }
		    	c = new Country(doc.get("country"), doc.get("ccode"), Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")));
			}
		    return c;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public String mapCcode(String queryString, boolean isCcode) {
		try {
			Query q;
			if(isCcode) {
				q = new QueryParser("ccode", analyzer).parse("\""+queryString+"\"");
			} else {
				q = new QueryParser("country", analyzer).parse("\""+queryString.toLowerCase()+"\"");
			}
			
			TotalHitCountCollector collector = new TotalHitCountCollector();
			countrySearcher.search(q, collector);
			TopDocs docs = countrySearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			String cur=null;
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = countrySearcher.doc(docid);
			    int i=0;
			    while(doc.get("country").contains(",")&&i<hits.length) {
			    	docid = hits[i].doc;
			    	doc = countrySearcher.doc(docid);
			    	i++;
			    }
			    if(isCcode) {
			    	cur = doc.get("country");
			    } else {
			    	cur = doc.get("ccode");
			    }
			};
			return cur;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
		
	public Set<LocationPart> searchAdm1(String adm1) {
		try {
			Set<LocationPart> l=null;
			Query q = new QueryParser("adm1", analyzer).parse("\""+adm1.toLowerCase()+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			adm1Searcher.search(q, collector);
			TopDocs docs = adm1Searcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			if(hits.length>0) {
				l = new HashSet<LocationPart>();
			}
			for (int i = 0; i < hits.length; i++) {
			    int docid = hits[i].doc;
			    Document doc = adm1Searcher.doc(docid);
			    LocationPart a = new ADM1(doc.get("geo_id"), doc.get("adm1"), doc.get("ccode"), doc.get("acode"), Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")), Integer.parseInt(doc.get("population")));
			    l.add(a);
			}	
			return l;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public Set<LocationPart> searchADM1Code(String adm1) {
		try {
			Set<LocationPart> l=null;
			Query q = new QueryParser("acode", analyzer).parse("\""+adm1.toLowerCase()+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			adm1Searcher.search(q, collector);
			TopDocs docs = adm1Searcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			if(hits.length>0) {
				l = new HashSet<LocationPart>();
			}
			for (int i = 0; i < hits.length; i++) {
			    int docid = hits[i].doc;
			    Document doc = adm1Searcher.doc(docid);
			    LocationPart a = new ADM1(doc.get("geo_id"), doc.get("adm1"), doc.get("ccode"), doc.get("acode"), Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")), Integer.parseInt(doc.get("population")));
			    l.add(a);
			}	
			return l;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public LocationPart searchADM1CodeGivenCcode(String adm1code, String ccode) {
		try {
	
		//	reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath+"/countryIndex")));
			Query q = new QueryParser("acode", analyzer).parse("\""+adm1code+"\" AND ccode:\""+ccode+"\"");

			TotalHitCountCollector collector = new TotalHitCountCollector();
			adm1Searcher.search(q, collector);
			TopDocs docs = adm1Searcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			LocationPart l=null;
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = adm1Searcher.doc(docid);
			    l = new ADM1(doc.get("geo_id"), doc.get("adm1"), doc.get("ccode"), doc.get("acode"), Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")), Integer.parseInt(doc.get("population")));
			}
		    return l;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public String mapADM1code(String queryString, boolean isADM1code) {
		try {
			Query q;
			if(isADM1code) {
				q = new QueryParser("acode", analyzer).parse("\""+queryString+"\"");
			} else {
				q = new QueryParser("adm1", analyzer).parse("\""+queryString.toLowerCase()+"\"");
			}
			
			TotalHitCountCollector collector = new TotalHitCountCollector();
			adm1Searcher.search(q, collector);
			TopDocs docs = adm1Searcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			String cur=null;
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = adm1Searcher.doc(docid);
			    if(isADM1code) {
			    	cur = doc.get("adm1");
			    } else {
			    	cur = doc.get("acode");
			    }
			};
			return cur;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public LocationPart searchAdm1GivenCcode(String adm1, String ccode) {
		try {
			Query q = new QueryParser("adm1", analyzer).parse("\""+adm1.toLowerCase()+"\" AND ccode:\""+ccode+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			adm1Searcher.search(q, collector);
			TopDocs docs = adm1Searcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			LocationPart l=null;
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = adm1Searcher.doc(docid);
			    l = new ADM1(doc.get("geo_id"), doc.get("adm1"), doc.get("ccode"), doc.get("acode"), Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")), Integer.parseInt(doc.get("population")));
			}	
			return l;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public LocationPart searchAdm1GivenCcodeAndAdm1code(String adm1, String ccode, String adm1code) {
		try {
			Query q = new QueryParser("adm1", analyzer).parse("\""+adm1.toLowerCase()+"\" AND ccode:\""+ccode+"\" AND acode:\""+adm1code+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			adm1Searcher.search(q, collector);
			TopDocs docs = adm1Searcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			LocationPart l=null;
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = adm1Searcher.doc(docid);
			    l = new ADM1(doc.get("geo_id"), doc.get("adm1"), doc.get("ccode"), doc.get("acode"), Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")), Integer.parseInt(doc.get("population")));
			}	
			return l;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public LocationPart searchAdm1GivenID(String adm1, String id) {
		try {
			Query q = new QueryParser("adm1", analyzer).parse("\""+adm1.toLowerCase()+"\" AND geo_id:\""+id+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			adm1Searcher.search(q, collector);
			TopDocs docs = adm1Searcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			LocationPart l=null;
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = adm1Searcher.doc(docid);
			    l = new ADM1(doc.get("geo_id"), doc.get("adm1"), doc.get("ccode"), doc.get("acode"), Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")), Integer.parseInt(doc.get("population")));
			}	
			return l;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	

	public Set<LocationPart> searchLocation(String place) {
		try {
			Set<LocationPart> l = null;
			Query q = new QueryParser("name", analyzer).parse("\""+place.toLowerCase()+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			locationSearcher.search(q, collector);
			TopDocs docs = locationSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			if(hits.length>0) {
				l=new HashSet<LocationPart>();
			}
			// retrieve each matching document from the ScoreDoc arry
			for (int i = 0; i < hits.length; i++) {
			    int docid = hits[i].doc;
			    Document doc = locationSearcher.doc(docid);
			    LocationPart a = new LocationPart(doc.get("geo_id"), doc.get("name"), doc.get("ccode"), doc.get("fcode"), doc.get("a1code"), doc.get("a2code"),Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")), Long.parseLong(doc.get("population")), doc.get("fclass"));
			    l.add(a);
			}
			return l;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public LocationPart searchLocationGivenID(String place, String id) {
		try {
			Query q = new QueryParser("name", analyzer).parse("\""+place.toLowerCase()+"\" AND geo_id:\""+id+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			locationSearcher.search(q, collector);
			TopDocs docs = locationSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			LocationPart l=null;
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = locationSearcher.doc(docid);
			    l = new LocationPart(doc.get("geo_id"), doc.get("name"), doc.get("ccode"), doc.get("fcode"), doc.get("a1code"), doc.get("a2code"),Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")), Integer.parseInt(doc.get("population")), doc.get("fclass"));
			}			
			return l;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public LocationPart searchContinent(String place) {
		try {
			Query q = new QueryParser("name", analyzer).parse("\""+place.toLowerCase()+"\" AND fcode:\"CONT\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			locationSearcher.search(q, collector);
			TopDocs docs = locationSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			// retrieve each matching document from the ScoreDoc arry
			LocationPart l=null;
			if(hits.length>0) {
			    int docid = hits[0].doc;
			    Document doc = adm1Searcher.doc(docid);
			    l = new LocationPart(doc.get("geo_id"), doc.get("name"), doc.get("ccode"), doc.get("fcode"), doc.get("a1code"), doc.get("a2code"),Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")), Integer.parseInt(doc.get("population")), doc.get("fclass"));
			}			
			return l;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public Set<LocationPart> searchLocationGivenCcode(String place, String ccode) {
		try {
			Set<LocationPart> l=null;
			Query q = new QueryParser("name", analyzer).parse("\""+place.toLowerCase()+"\" AND ccode:\""+ccode+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			locationSearcher.search(q, collector);
			TopDocs docs = locationSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			if(hits.length>0) {
				l= new HashSet<LocationPart>();
			}
			// retrieve each matching document from the ScoreDoc arry
			for (int i = 0; i < hits.length; i++) {
			    int docid = hits[i].doc;
			    Document doc = locationSearcher.doc(docid);
			    LocationPart a = new LocationPart(doc.get("geo_id"), doc.get("name"), doc.get("ccode"), doc.get("fcode"), doc.get("a1code"), doc.get("a2code"),Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")), Integer.parseInt(doc.get("population")), doc.get("fclass"));
			    l.add(a);
			}
			return l;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public Set<LocationPart> searchLocationGivenADM1code(String place, String adm1code) {
		try {
			Set<LocationPart> l=null;
			Query q = new QueryParser("name", analyzer).parse("\""+place.toLowerCase()+"\" AND a1code:\""+adm1code+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			locationSearcher.search(q, collector);
			TopDocs docs = locationSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			if(hits.length>0) {
				l= new HashSet<LocationPart>();
			}
			// retrieve each matching document from the ScoreDoc arry
			for (int i = 0; i < hits.length; i++) {
			    int docid = hits[i].doc;
			    Document doc = locationSearcher.doc(docid);
			    LocationPart a = new LocationPart(doc.get("geo_id"), doc.get("name"), doc.get("ccode"), doc.get("fcode"), doc.get("a1code"), doc.get("a2code"),Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")), Integer.parseInt(doc.get("population")), doc.get("fclass"));
			    l.add(a);
			}
			return l;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public Set<LocationPart> searchLocationGivenCcodeAndAdm1code(String place, String ccode, String adm1code) {
		try {
			Set<LocationPart> l=null;
			Query q = new QueryParser("name", analyzer).parse("\""+place.toLowerCase()+"\" AND ccode:\""+ccode+"\"AND a1code:\""+adm1code+"\"");
			TotalHitCountCollector collector = new TotalHitCountCollector();
			locationSearcher.search(q, collector);
			TopDocs docs = locationSearcher.search(q, Math.max(1, collector.getTotalHits()));
			ScoreDoc[] hits = docs.scoreDocs;
			if(hits.length>0) {
				l= new HashSet<LocationPart>();
			}
			// retrieve each matching document from the ScoreDoc arry
			for (int i = 0; i < hits.length; i++) {
			    int docid = hits[i].doc;
			    Document doc = locationSearcher.doc(docid);
			    LocationPart a = new LocationPart(doc.get("geo_id"), doc.get("name"), doc.get("ccode"), doc.get("fcode"), doc.get("a1code"), doc.get("a2code"),Double.valueOf(doc.get("lat")), Double.valueOf(doc.get("lng")), Integer.parseInt(doc.get("population")), doc.get("fclass"));
			    l.add(a);
			}
			return l;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public void close() {
		try {
			countrySearcher.getIndexReader().close();
			adm1Searcher.getIndexReader().close();
			locationSearcher.getIndexReader().close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}

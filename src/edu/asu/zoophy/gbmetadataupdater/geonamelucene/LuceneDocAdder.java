package edu.asu.zoophy.gbmetadataupdater.geonamelucene;



import java.io.IOException;
import java.nio.file.Paths;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;


/**
 * A class for creating three lucene idexes:
 * 1) countryIndex which is created using a list of countries
 * 2) adm1Index which is created using all adm1-level location names along with their possible alternate names in GeoNames database
 * 3) geoIndex which is created using all locations in GeoNames database (excluding alternate names)c
 * @author tasnia
 *
 */
public class LuceneDocAdder {

	public static void main(String args[]) throws ParseException {
		LuceneDocAdder li = new LuceneDocAdder();
		li.index(System.getProperty("user.dir")+"/Resources/index");
	}

	public void index(String indexPath) throws ParseException {
		try {
			System.out.println("Indexing to directory '" + indexPath + "'...");
			Directory cdir = FSDirectory.open(Paths.get(indexPath+"/geoIndex"));
			Analyzer analyzer = new KeywordAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
			iwc.setOpenMode(OpenMode.APPEND);
			IndexWriter writer = new IndexWriter(cdir, iwc);
			indexGeoDoc(writer);
			writer.close();
		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() +
					"\n with message: " + e.getMessage());
		}
	}

	public void indexGeoDoc(IndexWriter writer) {
		Document doc = new Document();   
		Field idField = new StringField("geo_id", "6542283" , Field.Store.YES);
		Field name_utf8Field = new StringField("name_utf8", "Milan", Field.Store.YES);
		Field nameField = new StringField("name", "milan", Field.Store.YES);
		Field latField = new StoredField("lat", "45.46416");
		Field lngField = new  StoredField("lng", "9.19199");	
		Field fclassField = new StringField("fclass", "A", Field.Store.YES);
		Field fcodeField = new StringField("fcode", "ADM3", Field.Store.YES);
		Field ccodeField = new StringField("ccode", "IT", Field.Store.YES);
		Field adm1Field = new StringField("a1code", "09", Field.Store.YES);
		Field adm2Field = new StringField("a2code", "MI", Field.Store.YES);
		Field popField = new LongField("population", 1307495, Field.Store.YES);
		doc.add(idField);
		doc.add(name_utf8Field);
		doc.add(nameField);
		doc.add(latField);
		doc.add(lngField);
		doc.add(ccodeField);
		doc.add(adm1Field);
		doc.add(popField);
		doc.add(fclassField);
		doc.add(fcodeField);
		doc.add(adm2Field);
	
			try {
				writer.addDocument(doc);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	}
	public void indexCountryDoc(IndexWriter writer) throws IOException {
		Document doc = new Document();   
		Field idField = new StringField("iso_id", "638", Field.Store.YES);
		Field nameField = new StringField("country", "reunion", Field.Store.YES);
		Field ccodeField = new StringField("ccode", "RE" , Field.Store.YES);
		Field latField = new StoredField("lat", "-21.1");
		Field lngField = new  StoredField("lng", "55.6");			
		doc.add(idField);
		doc.add(nameField);
		doc.add(ccodeField);
		doc.add(latField);
		doc.add(lngField);
		writer.addDocument(doc);
		
	}

}

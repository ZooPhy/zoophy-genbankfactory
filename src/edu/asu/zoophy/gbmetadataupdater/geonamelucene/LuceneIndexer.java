package edu.asu.zoophy.gbmetadataupdater.geonamelucene;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

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
public class LuceneIndexer {

	public static void main(String args[]) {
		LuceneIndexer li = new LuceneIndexer();
		li.index(System.getProperty("user.dir")+"/Resources/index", System.getProperty("user.dir")+"/Resources/Lucene_src_files", true);
	}

	public void index(String indexPath, String docsPath, boolean create) {
		final Path docDir = Paths.get(docsPath);
		if (!Files.isReadable(docDir)) {
			System.out.println("Document directory '" +docDir.toAbsolutePath()+ "' does not exist or is not readable, please check the path");
			System.exit(1);
		}
		Date start = new Date();
		try {
			System.out.println("Indexing to directory '" + indexPath + "'...");
			Directory cdir = FSDirectory.open(Paths.get(indexPath+"/countryIndex"));
			Directory adir = FSDirectory.open(Paths.get(indexPath+"/adm1Index"));
			Directory gdir = FSDirectory.open(Paths.get(indexPath+"/geoIndex"));
			Analyzer analyzer = new KeywordAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
			if (create) {
				iwc.setOpenMode(OpenMode.CREATE);
			} else {
				// Add new documents to an existing index:
				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			}

			//iwc.setRAMBufferSizeMB(256.0);
			IndexWriter writer = new IndexWriter(cdir, iwc);
			indexCountryDocs(writer, docsPath);
			writer.close();
			iwc = new IndexWriterConfig(analyzer);
			if (create) {
				iwc.setOpenMode(OpenMode.CREATE);
			} else {
				// Add new documents to an existing index:
				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			}
			writer = new IndexWriter(adir, iwc);
			indexAdm1Docs(writer, docsPath);
			writer.close();
			iwc = new IndexWriterConfig(analyzer);
			if (create) {
				iwc.setOpenMode(OpenMode.CREATE);
			} else {
				// Add new documents to an existing index:
				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			}
			writer = new IndexWriter(gdir, iwc);
			indexGeoDocs(writer, docsPath);
			writer.close();
			// writer.forceMerge(1);
			Date end = new Date();
			System.out.println(end.getTime() - start.getTime() + " total milliseconds");
		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() +
					"\n with message: " + e.getMessage());
		}
	}

	public void indexCountryDocs(IndexWriter writer, String docsPath) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(docsPath+"/country.txt"));
			String line = reader.readLine();
			while(line!=null) {
				indexCountryDoc(writer, line);
				line = reader.readLine();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			
			if (reader != null) {
				try {
				reader.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
			
		}
	}

	public void indexAdm1Docs(IndexWriter writer, String docsPath) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(docsPath+"/adm1.txt"));
			String line = reader.readLine();
			while(line!=null) {
				indexAdm1Doc(writer, line);
				line = reader.readLine();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			
			if (reader != null) {
				try {
				reader.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
			
		}
	}

	public void indexGeoDocs(IndexWriter writer, String docsPath) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(docsPath+"/geolocation.txt"));
			String line = reader.readLine();
			while(line!=null) {
				indexGeoDoc(writer, line);
				line = reader.readLine();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			
			if (reader != null) {
				try {
				reader.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
			
		}
	}
	/** Indexes a country document */
	public void indexCountryDoc(IndexWriter writer, String line) throws IOException {
		Document doc = new Document();   
		String[] lineParts = line.split("\t");
		if(line.length()<5) {
			System.out.println("country file has invalid format");
			return;
		}
		Field idField = new StringField("iso_id", lineParts[0].replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field nameField = new StringField("country", lineParts[1].toLowerCase().replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field ccodeField = new StringField("ccode", lineParts[2].replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field latField = new StoredField("lat", lineParts[3].replaceAll("\\s+$", "").replaceAll("^\\s+", ""));
		Field lngField = new  StoredField("lng", lineParts[4].replaceAll("\\s+$", "").replaceAll("^\\s+", ""));			
		doc.add(idField);
		doc.add(nameField);
		doc.add(ccodeField);
		doc.add(latField);
		doc.add(lngField);
		if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
			// New index, so we just add the document (no old document can be there):
			System.out.println("adding country doc:"+lineParts[1]);
			writer.addDocument(doc);
		} else {
			System.out.println("updating mode not supported yet");
		}
	}

	/** Indexes an adm1 document */
	public void indexAdm1Doc(IndexWriter writer, String line) {
		Document doc = new Document();   
		String[] lineParts = line.split("\t");
		if(line.length()<10) {
			System.out.println("adm1 file has invalid format");
			return;
		}
		Field idField = new StringField("geo_id", lineParts[0].replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field name_utf8Field = new StringField("adm1_utf8", lineParts[1].replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field nameField = new StringField("adm1", lineParts[2].toLowerCase().replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field latField = new StoredField("lat", lineParts[3].replaceAll("\\s+$", "").replaceAll("^\\s+", ""));
		Field lngField = new  StoredField("lng", lineParts[4].replaceAll("\\s+$", "").replaceAll("^\\s+", ""));	
		Field ccodeField = new StringField("ccode", lineParts[7].replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field adm1Field = new StringField("acode", lineParts[8].replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field popField = new LongField("population", Long.valueOf(lineParts[9]), Field.Store.YES);
		doc.add(idField);
		doc.add(name_utf8Field);
		doc.add(nameField);
		doc.add(latField);
		doc.add(lngField);
		doc.add(ccodeField);
		doc.add(adm1Field);
		doc.add(popField);
		if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
			// New index, so we just add the document (no old document can be there):
			System.out.println("adding country doc:"+lineParts[2]);
			try {
				writer.addDocument(doc);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			System.out.println("updating mode not supported yet");
		}
	}

	public void indexGeoDoc(IndexWriter writer, String line) {
		Document doc = new Document();   
		String[] lineParts = line.split("\t");
		if(line.length()<11) {
			System.out.println("adm1 file has invalid format");
			return;
		}
		Field idField = new StringField("geo_id", lineParts[0].replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field name_utf8Field = new StringField("name_utf8", lineParts[1].replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field nameField = new StringField("name", lineParts[2].toLowerCase().replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field latField = new StoredField("lat", lineParts[3].replaceAll("\\s+$", "").replaceAll("^\\s+", ""));
		Field lngField = new  StoredField("lng", lineParts[4].replaceAll("\\s+$", "").replaceAll("^\\s+", ""));	
		Field fclassField = new StringField("fclass", lineParts[5].replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field fcodeField = new StringField("fcode", lineParts[6].replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field ccodeField = new StringField("ccode", lineParts[7].replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field adm1Field = new StringField("a1code", lineParts[8].replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field adm2Field = new StringField("a2code", lineParts[9].replaceAll("\\s+$", "").replaceAll("^\\s+", ""), Field.Store.YES);
		Field popField = new LongField("population", Long.valueOf(lineParts[10].replaceAll("\\s+$", "").replaceAll("^\\s+", "")), Field.Store.YES);
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
		if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
			// New index, so we just add the document (no old document can be there):
			System.out.println("adding loc doc:"+lineParts[2]);
			try {
				writer.addDocument(doc);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			System.out.println("updating mode not supported yet");
		}
	}
}

package edu.asu.zoophy.genbankfactory.index;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import edu.asu.zoophy.genbankfactory.database.GenBankRecord;
import edu.asu.zoophy.genbankfactory.database.GenBankRecordDAOInt;
import edu.asu.zoophy.genbankfactory.database.GenBankRecordSqlDAO;
import edu.asu.zoophy.genbankfactory.database.Gene;
import edu.asu.zoophy.genbankfactory.utils.normalizer.gene.GeneNormalizer;
import edu.asu.zoophy.genbankfactory.utils.taxonomy.GenBankTree;
import edu.asu.zoophy.genbankfactory.utils.taxonomy.GeoNameNode;
import edu.asu.zoophy.genbankfactory.utils.taxonomy.GeoNameTree;
import edu.asu.zoophy.genbankfactory.utils.taxonomy.Node;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBQuery;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;

/**
 * A special class to index the ACCESSIONS with terms and token of the document they appear in
 * (articles are not interesting by themselves, only the accession)
 * @author Davy/Demetrius
 */
public class Indexer {
	private static final Logger log = Logger.getLogger("Indexer");
	private static final int BATCH_SIZE = 50000;
	private static final String COUNT_ACCESSIONS = "SELECT COUNT(\"Accession\") FROM \"Sequence_Details\"";
	private final static String UPDATE_GEONAME_IDS = "UPDATE \"Location_Geoname\" SET \"Geoname_ID\"=? WHERE \"Accession\"=?";
	private final static String UPDATE_GEONAME_TYPES = "UPDATE \"Location_Geoname\" SET \"Type\"=? WHERE \"Accession\"=?";
	private final static String UPDATE_GEONAME_COUNTRIES = "UPDATE \"Location_Geoname\" SET \"Country\"=? WHERE \"Accession\"=?";
	private DBQuery updateGeonameIDsQuery;
	private DBQuery updateGeonameTypesQuery;
	private DBQuery updateGeonameCountriesQuery;
	static final protected GenBankRecordDAOInt dao = new GenBankRecordSqlDAO();
	private Connection conn = null;
//	private Corpus corpus = null;
	private IndexWriter writer = null;
	private GeneNormalizer gn;
	
	private int missingLocs = 0;
	private int unknownLocs = 0;
	private int countryMappedLocs = 0;
	private int tooSpecificCountryMappedLocs = 0;
	private int updatedTypes = 0;
	private int updatedCountries = 0;
	
	/**
	 * The current lucene document created before insertion
	 */
	private Document doc = null;
	/**
	 * Initialize the indexer given the parameters in the {@link PropertiesProvider} file.
	 */
	public Indexer(String indexPath) throws IndexerException {
		log.info("Indexer initializing...");
	    try {
			conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
	    }
	    catch (Exception e) {
	    	log.log(Level.SEVERE, "Impossible to Initiate the Resources Provider:"+e.getMessage());
	    	throw new IndexerException("Impossible to Initiate the Resources Provider:"+e.getMessage());
	    }
	    try {
			gn = GeneNormalizer.getInstance();
		}
	    catch (Exception e) {
			log.log(Level.SEVERE, "Could not get GeneNormalizer: "+e.getMessage());
		}
		//check the directory where the index will be opened
		File indexDir = new File(indexPath);
	    if (!indexDir.exists() || !indexDir.isDirectory()) {
	    	log.log(Level.SEVERE, indexDir + " does not exist");
	    	throw new IndexerException("The parameters for opening the Index are incorrect.");
	    }
	    else { //clean out old index if it exists
    	    File[] files = indexDir.listFiles();
    	    if (files != null) { 
    	        for(File f: files) {
    	        	f.delete();
    	        }
    	    }
	    }
	    createIndex(indexDir);
	    log.info("Indexer initialized with index:"+writer.getDirectory().toString());
	}
	
	/**
	 * Open the index in creation mode
	 * @parameter the file where to create the index
	 */
	protected void createIndex(File indexDir) throws IndexerException {
    	try {
    		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40, new KeywordAnalyzer());
    		writer = new IndexWriter(FSDirectory.open(indexDir), config);
        	log.info("Creation of a new index.");    		
    	}
    	catch(Exception e) {
    		log.log(Level.SEVERE, "Impossible to create the index:"+e.getMessage());
    		throw new IndexerException();
    	}
	}
	public void indexAccession(GenBankRecord record, GenBankTree gbTree, GeoNameTree geoTree) throws IndexerException {
		addAccessionInIndex(record, gbTree, geoTree);
	}
	/**
	 * Create and add the document in the lucene index for the accession
	 * @param accession the accession linked to 
	 */
	protected void addAccessionInIndex(GenBankRecord record, GenBankTree gbTree, GeoNameTree geoTree) throws IndexerException {
		doc = createDocument(record, gbTree, geoTree);
		try {
			writer.addDocument(doc);
		}
		catch (Exception e) {
			throw new IndexerException();
		}
	}
	/**
	 * Create a lucene document and fill all fields given the {@link jp.ac.toyota_ti.coin.wipefinder.server.document.Document} passed
	 * @return {org.apache.lucene.document.Document} the lucene document created
	 * @throws IndexerException
	 */
	protected Document createDocument(GenBankRecord record, GenBankTree gbTree, GeoNameTree geoTree) throws IndexerException {
		doc = null;
		doc = new Document();
		setAccession(record);//TODO: marker for this function//
		setOrganism(record, gbTree);
		if (record.getSequence().getCollection_date() != null) {
			setDate(record);
		}
		else {
			doc.add(new StringField("Date", "10000101", Field.Store.YES));
		}
		setHost(record, gbTree);
		if (record.getGenBankLocation() != null) {
			setGeographicLocation(record, geoTree);
		}
		else {
			doc.add(new StringField("Location", "Unknown", Field.Store.YES));
			doc.add(new StringField("Country", "Unknown", Field.Store.YES));
			doc.add(new StringField("CountryCode", "Unknown", Field.Store.YES));
			doc.add(new StringField("GeonameID", String.valueOf(geoTree.getRoot().getID()), Field.Store.YES));
			doc.add(new StringField("LocationType", "Unknown", Field.Store.YES));
			unknownLocs++;
		}
		if (record.getSequence().getPub() != null) {
			doc.add(new StringField("PubmedID", String.valueOf(record.getSequence().getPub().getPubId()), Field.Store.YES));
		}
		else {
			doc.add(new StringField("PubmedID", "n/a", Field.Store.YES));
		}
		return doc;
	}
	
	protected void setAccession(GenBankRecord record) {
		doc.add(new StringField("Accession", record.getAccession(), Field.Store.YES));		
	}
	/**
	 * Insert the organism, strain, and the genes related for the given record
	 * @param record
	 */
	protected void setOrganism(GenBankRecord record, GenBankTree gbTree) {
		if (record.getSequence().getOrganism() != null) {
			doc.add(new TextField("Organism", record.getSequence().getOrganism(), Field.Store.YES));
		}
		else {
			doc.add(new TextField("Organism", "Unknown", Field.Store.YES));
		}
		if (record.getSequence().getStrain() != null) {
			doc.add(new TextField("Strain", record.getSequence().getStrain(), Field.Store.YES));
		}
		else {
			doc.add(new TextField("Strain", "Unknown", Field.Store.YES));
		}
		if (record.getSequence().getDefinition() != null) {
			doc.add(new TextField("Definition", record.getSequence().getDefinition(), Field.Store.YES));
		}
		else {
			doc.add(new TextField("Definition", "Unknown", Field.Store.YES));
		}
		doc.add(new StringField("SegmentLength", String.valueOf(record.getSequence().getSegment_length()),Field.Store.YES));
		//add gene info//
		if (record.getGenes() != null) {
			for (Gene g : record.getGenes()) {
				if (g.getName() != null) {
					if (g.getName().equalsIgnoreCase("complete")) {
						doc.add(new StringField("Gene", "Complete", Field.Store.YES));
					}
					else {
						doc.add(new StringField("Gene", g.getName(), Field.Store.YES));
					}
				}
			}
			checkForFullGenome(record);
		}
		//add taxon info//
		if (record.getSequence().getTax_id() <= 1) {
			doc.add(new StringField("TaxonID", gbTree.getRoot().getID().toString(), Field.Store.YES));
		}
		else {
			try {
				Node node = gbTree.getNode(record.getSequence().getTax_id());
				if (node !=null) {//we should have most taxons in the tree...
					String cpts = node.getConcept();
					if (cpts.contains(" | ")) {
						for(String cpt: cpts.split(" \\| ")) {
							doc.add(new StringField("TaxonID", cpt, Field.Store.YES));
						}
					}
					else {
						doc.add(new StringField("TaxonID", cpts, Field.Store.YES));
					}
					doc.add(new StringField("TaxonID", node.getID().toString(), Field.Store.YES));
					List<Node> ancestors = node.getAncestors();
					for (Node ancestor: ancestors) {
						doc.add(new StringField("TaxonID", ancestor.getID().toString(), Field.Store.YES));
					}
				}
				else {
					doc.add(new StringField("TaxonID", String.valueOf(record.getSequence().getTax_id()), Field.Store.YES));
					doc.add(new StringField("TaxonID", gbTree.getRoot().getID().toString(), Field.Store.YES));
				}
			}
			catch (Exception e) {
				log.log(Level.SEVERE, "Error indexing taxonomy for taxon: " + record.getSequence().getTax_id());
				doc.add(new StringField("TaxonID", String.valueOf(record.getSequence().getTax_id()), Field.Store.YES));
				doc.add(new StringField("TaxonID", gbTree.getRoot().getID().toString(), Field.Store.YES));
			}
		}
		doc.add(new StringField("PH1N1", String.valueOf(record.getSequence().isPH1N1()), Field.Store.YES));
	}
	
	private void checkForFullGenome(GenBankRecord record) {
		try {
			List<String> genes = new LinkedList<String>();
			for (Gene g : record.getGenes()) {
				genes.add(g.getName());
			}
			List<String> fullGenome = gn.getFullGenomeList(record.getSequence().getTax_id());
			if (fullGenome != null && (genes.containsAll(fullGenome) || genes.contains("Complete"))) {
				doc.add(new StringField("Gene", "Complete", Field.Store.YES));
			}
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error checking for full genome: "+e.getMessage());
		}
	}
	/**
	 * Insert the host ID and all its parents ID (for further query)
	 * @param record
	 */
	protected void setHost(GenBankRecord record, GenBankTree gbTree) {
		if (record.getHost().getName() != null) {
			doc.add(new TextField("Host_Name", record.getHost().getName().toLowerCase(), Field.Store.YES));
		}
		else {
			doc.add(new TextField("Host_Name", "Unknown".toLowerCase(), Field.Store.YES));
		}
		if (record.getHost().getTaxon() <= 1 || record.getHost().getName() == null) {
			doc.add(new StringField("HostID", gbTree.getRoot().getID().toString(), Field.Store.YES));
		}
		else {
			try { 
				Node node = gbTree.getNode(record.getHost().getTaxon());
				if (node != null) {//should have most* taxons in our tree...
					String cpts = gbTree.getNode(record.getHost().getTaxon()).getConcept();
					if(cpts.contains(" | ")) {
						for(String cpt: cpts.split(" \\| ")) {
							doc.add(new TextField("Host", cpt.toLowerCase(), Field.Store.YES));
						}
					}
					else {
						doc.add(new TextField("Host", cpts.toLowerCase(), Field.Store.YES));
					}
					doc.add(new StringField("HostID", String.valueOf(record.getHost().getTaxon()), Field.Store.YES));
					List<Node> ancestors = node.getAncestors();
					for(Node ancestor: ancestors) {
						doc.add(new StringField("HostID", ancestor.getID().toString(), Field.Store.YES));
					}
				}
				else {
					doc.add(new StringField("TaxonID", String.valueOf(record.getHost().getTaxon()), Field.Store.YES));
					doc.add(new StringField("TaxonID", gbTree.getRoot().getID().toString(), Field.Store.YES));
				}
			}
			catch (Exception e) { 
				log.log(Level.SEVERE, "Error indexing taxonomy for host taxon: " + record.getHost().getTaxon());
				doc.add(new StringField("TaxonID", String.valueOf(record.getHost().getTaxon()), Field.Store.YES));
				doc.add(new StringField("TaxonID", gbTree.getRoot().getID().toString(), Field.Store.YES));
			}
		}
	}
	/**
	 * Insert the geolocalisation of the record based on the evidences found, Continent and country only
	 * @param record
	 */
	protected void setGeographicLocation(GenBankRecord record, GeoNameTree geoTree) {
		int id = (int) record.getGenBankLocation().getId();
		if (id < 1) {
			missingLocs++;
			if (record.getGenBankLocation().getLocation() != null) {
				String loc = record.getGenBankLocation().getLocation();
				if (geoTree.getCountryLookup().get(loc) != null) {
					id = geoTree.getCountryLookup().get(loc);
					countryMappedLocs++;
					try {
						List<Object> params = new LinkedList<Object>();
						params.add(id);
						params.add(record.getAccession());
						updateGeonameIDsQuery.addBatch(params);
					} 
					catch (SQLException e) {
						log.log(Level.SEVERE, "Issue updating geoname ID: "+e.getMessage());
					}
				}
			}
		}
		if (record.getGenBankLocation().getLocation() == null || record.getGenBankLocation().getLocation().equalsIgnoreCase("unknown")) {
			unknownLocs++;
		}
		GeoNameNode g;
		List<Integer> visitedPlaces = new LinkedList<Integer>();
		try {
			if (id > 0 && record.getGenBankLocation().getLocation() != null && !record.getGenBankLocation().getLocation().equalsIgnoreCase("unknown")) { // there's a place called Unknown that unknown places get normalized to
				String country_code = null;
				g = geoTree.getMapIDNodes().get(id);
				if (g == null && record.getGenBankLocation().getLocation() != null) {
					String loc = record.getGenBankLocation().getLocation();
					while (loc.contains(",") && g == null) {
						loc = loc.substring(loc.indexOf(',')+1).trim();
						if (geoTree.getCountryLookup().get(loc) != null) {
							country_code = loc; 
							g = geoTree.getMapIDNodes().get(geoTree.getCountryLookup().get(loc));
							tooSpecificCountryMappedLocs++;
							try {
								List<Object> params = new LinkedList<Object>();
								params.add(g.getID());
								params.add(record.getAccession());
								updateGeonameIDsQuery.addBatch(params);
							} 
							catch (SQLException e) {
								log.log(Level.SEVERE, "Issue updating Geoname ID: "+e.getMessage());
							}
						}
					}
				}
				if (g != null) {
					if (g.getConcept() != null) {
						doc.add(new TextField("Location", g.getConcept(), Field.Store.YES));
					}
					else {
						doc.add(new TextField("Location", "Unknown", Field.Store.YES));
					}
					if (g.getLocation() != null) {
						country_code = g.getLocation().getCountry();
						doc.add(new StringField("Latitude", String.valueOf(g.getLocation().getLatitude()), Field.Store.YES));
						doc.add(new StringField("Longitude", String.valueOf(g.getLocation().getLongitude()), Field.Store.YES));
						if (g.getLocation().getType() != null) {
							doc.add(new StringField("LocationType", g.getLocation().getType(), Field.Store.YES));
							if(record.getGenBankLocation().getType() == null) {
								List<Object> params = new LinkedList<Object>();
								params.add(g.getLocation().getType());
								params.add(record.getAccession());
								updateGeonameTypesQuery.addBatch(params);
								updatedTypes++;
							}
						}
						else {
							doc.add(new StringField("LocationType", "Unknown", Field.Store.YES));
						}
					}
					else {
						doc.add(new StringField("Latitude", String.valueOf(0.0), Field.Store.YES));
						doc.add(new StringField("Longitude", String.valueOf(0.0), Field.Store.YES));
						doc.add(new StringField("LocationType", "Unknown", Field.Store.YES));
					}
					if (g != (GeoNameNode) geoTree.getRoot() && g.getFather() == null) {
						g.setFather((GeoNameNode) geoTree.getRoot());
						if (g.getLocation() != null) {
							if (g.getLocation().getCountry() != null) {
								if (g.getLocation().getAdm1() != null) {
									String fullAdm = g.getLocation().getCountry() + "." + g.getLocation().getAdm1();
									try {
										int amdId = geoTree.getAdmLookup().get(fullAdm);
										GeoNameNode adm = geoTree.getMapIDNodes().get(amdId);
										if (g != adm && adm != null) {
											g.setFather(adm);
										}
									}
									catch (Exception e) {
										int countryId = geoTree.getCountryLookup().get(g.getLocation().getCountry());
										GeoNameNode country = geoTree.getMapIDNodes().get(countryId);
										if (g != country && country != null) {
											g.setFather(country);
										}
									}
								}
								else {
									int countryId = geoTree.getCountryLookup().get(g.getLocation().getCountry());
									GeoNameNode country = geoTree.getMapIDNodes().get(countryId);
									if (g != country && country != null) {
										g.setFather(country);
									}
								}
							}
						}
					}
					while (g != null) {
						if (visitedPlaces.contains(g.getID())) {
							if (g.getID() == 472755) { //weird russian location
								g = geoTree.getMapIDNodes().get(2017370);
							}
							else {
								log.log(Level.SEVERE, "Error loop Found for GeonameID: "+g.getID());
								g = null;
								doc.add(new StringField("GeonameID", String.valueOf(geoTree.getRoot().getID()), Field.Store.YES));
							}
						}
						else {
							visitedPlaces.add(g.getID());
							doc.add(new StringField("GeonameID", String.valueOf(g.getID()), Field.Store.YES));
							if (country_code == null) {
								if (g.getLocation() != null) {
									country_code = g.getLocation().getCountry();
								}
							}
							g = (GeoNameNode) g.getFather();
						}
					}
				}
				else {
					if (record.getGenBankLocation().getLocation() != null) {
						doc.add(new TextField("Location", record.getGenBankLocation().getLocation(), Field.Store.YES));
					}
					else {
						doc.add(new TextField("Location", "Unknown", Field.Store.YES));
					}
					doc.add(new StringField("GeonameID", String.valueOf(id), Field.Store.YES));
					doc.add(new StringField("GeonameID", String.valueOf(geoTree.getRoot().getID()), Field.Store.YES));
				}
				visitedPlaces.clear();
				if (country_code == null) {
					doc.add(new TextField("Country", "Unknown", Field.Store.YES));
				}
				else {
					try {
						String country_name = SimplifyCountry(geoTree.getMapIDNodes().get(geoTree.getCountryLookup().get(country_code)).getConcept());
						doc.add(new TextField("Country", country_name, Field.Store.YES));
						if (record.getGenBankLocation().getCountry() == null) {
							List<Object> params = new LinkedList<Object>();
							params.add(country_name);
							params.add(record.getAccession());
							updateGeonameCountriesQuery.addBatch(params);
							updatedCountries++;
						}
					}
					catch (Exception e) {
						log.log(Level.SEVERE, "ERROR setting country name for country code: "+country_code);
						doc.add(new TextField("Country", "Unknown", Field.Store.YES));
					}
					doc.add(new StringField("CountryCode", country_code, Field.Store.YES));
				}
			}
			else {
				if (record.getGenBankLocation().getLocation() != null) {
					doc.add(new TextField("Location", record.getGenBankLocation().getLocation(), Field.Store.YES));
				}
				else {
					doc.add(new StringField("Location", "Unknown", Field.Store.YES));
				}
				doc.add(new StringField("CountryCode", "Unknown", Field.Store.YES));
				doc.add(new TextField("Country", "Unknown", Field.Store.YES));
				doc.add(new StringField("GeonameID", String.valueOf(geoTree.getRoot().getID()), Field.Store.YES));
			}
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error indexing location for Geoname id: " + id + " " + e.getMessage());
			doc.add(new TextField("Location", "Unknown", Field.Store.YES));
			doc.add(new StringField("GeonameID", String.valueOf(id), Field.Store.YES));
			doc.add(new StringField("GeonameID", String.valueOf(geoTree.getRoot().getID()), Field.Store.YES));
		}
	}
	
	private String SimplifyCountry(String country_name) {
		if (country_name != null) {
			if (country_name.contains("Great Britain")) {
				country_name = "United Kingdom";
			}
			else if (country_name.equalsIgnoreCase("Russian Federation")) {
				country_name = "Russia";
			}
			else if (country_name.equalsIgnoreCase("Repubblica Italiana")) {
				country_name = "Italy";
			}
			else if (country_name.equalsIgnoreCase("Polynésie Française")) {
				country_name = "French Polynesia";
			}
			else if (country_name.equalsIgnoreCase("Lao People’s Democratic Republic")) {
				country_name = "Laos";
			}
			else if (country_name.equalsIgnoreCase("Argentine Republic")){
				country_name = "Argentina";
			}
			else if (country_name.equalsIgnoreCase("Portuguese Republic")){
				country_name = "Portugal";
			}
			else {
				if (country_name.contains("Republic of ")) {
					country_name = country_name.substring(country_name.indexOf("Republic of ")+12);
					if (country_name.contains("the ")) {
						country_name = country_name.substring(country_name.indexOf("the ")+4);
					}
				}
				else if (country_name.contains("Kingdom of ")) {
					country_name = country_name.substring(country_name.indexOf("Kingdom of ")+11);
					if (country_name.contains("the ")) {
						country_name = country_name.substring(country_name.indexOf("the ")+4);
					}
				}
				else if (country_name.contains("Union of ")) {
					country_name = country_name.substring(country_name.indexOf("Union of ")+9);
					if (country_name.contains("the ")) {
						country_name = country_name.substring(country_name.indexOf("the ")+4);
					}
				}
				else if (country_name.contains("State of ")) {
					country_name = country_name.substring(country_name.indexOf("State of ")+9);
					if (country_name.contains("the ")) {
						country_name = country_name.substring(country_name.indexOf("the ")+4);
					}
				}
				else if (country_name.contains("Commonwealth of ")) {
					country_name = country_name.substring(country_name.indexOf("Commonwealth of ")+16);
					if (country_name.contains("the ")) {
						country_name = country_name.substring(country_name.indexOf("the ")+4);
					}
				}
				else if (country_name.endsWith("Special Administrative Region")) {
					country_name = country_name.substring(0,country_name.indexOf("Special Administrative Region")-1);
				}
			}
		}
		else {
			country_name = "Unknown";
		}
		return country_name;
	}
	/**
	 * Insert the docID in the lucene
	 * @throws IndexerException
	 */
	protected void setDate(GenBankRecord record) {
		doc.add(new StringField("Date", formatDate(record), Field.Store.YES));
	}
	/**
	 * Format the date in Lucene format,
	 * @param record
	 * @return
	 * @exception return 10000101, that is 01 Jan 1000, the default date
	 */
	protected String formatDate(GenBankRecord record) {
		if (record.getSequence().getCollection_date()!=null) {
			try {
				String date = record.getSequence().getCollection_date().trim();
				if (date.matches("[0-9]{4}")) {
					return DateTools.dateToString(DateTools.stringToDate(date),Resolution.YEAR);
				}
				if (date.matches("[0-9]{1,2}-[A-Za-z]{3}-[0-9]{2}")) {
					String[] dateComponents = record.getSequence().getCollection_date().trim().split("-");
					if (dateComponents[0].length()==1) {
						dateComponents[0] = "0"+dateComponents[0];
					}
					Integer year = new Integer(dateComponents[2]);
					if (year > 16 && year < 100) {
						dateComponents[2] = "19"+dateComponents[2];
					}
					else if (year <= 16) {
						dateComponents[2] = "20"+dateComponents[2];
					}
					return DateTools.dateToString(DateTools.stringToDate(getYear(dateComponents[2])+getMonth(dateComponents[1])+getDay(dateComponents[0])),Resolution.DAY);
				}
				if (date.matches("[A-Za-z]{3}-[0-9]{2}")) {
					String[] dateComponents = record.getSequence().getCollection_date().trim().split("-");
					Integer year = new Integer(dateComponents[1]);
					if (year>16 && year < 100) {
						dateComponents[1] = "19"+dateComponents[1];
					}
					else if (year <= 16) {
						dateComponents[1] = "20"+dateComponents[1];
					}
					return DateTools.dateToString(DateTools.stringToDate(getYear(dateComponents[1])+getMonth(dateComponents[0])),Resolution.MONTH);
				}
				if (date.matches("[A-Za-z]{3}-[0-9]{4}")) {
					String[] dateComponents = record.getSequence().getCollection_date().trim().split("-");
					return DateTools.dateToString(DateTools.stringToDate(getYear(dateComponents[1])+getMonth(dateComponents[0])),Resolution.MONTH);
				}
				if (date.matches("[0-9]{1,2}-[A-Za-z]{3}-[0-9]{4}")) {
					String[] dateComponents = record.getSequence().getCollection_date().trim().split("-");
					if(dateComponents[0].length()==1) {
						dateComponents[0] = "0"+dateComponents[0];
					}
					return DateTools.dateToString(DateTools.stringToDate(getYear(dateComponents[2])+getMonth(dateComponents[1])+getDay(dateComponents[0])),Resolution.DAY);
				}
			}
			catch (Exception e) {
				log.log(Level.SEVERE, "Impossible to format the date [" + record.getSequence().getCollection_date().trim() + "] for the accession ["+record.getAccession()+"], insert the default date 1-1-1");			
			}
		}
		return "10000101";
	}
	protected String getDay(String day) {
		if(day.length() == 1)
			return "0"+day;
		return day;
	}
	protected String getYear(String year) {
		if(year.length()!=2) {
			return year;
		}
		if(year.startsWith("0")||year.startsWith("1")) {
			return "20"+year;
		}
		return "19"+year;
	}
	protected String getMonth(String month) {
		switch (month.toLowerCase()) {
			case "jan": return "01";
			case "feb": return "02";
			case "mar": return "03";
			case "apr": return "04";
			case "may": return "05";
			case "jun": return "06";
			case "jul": return "07";
			case "aug": return "08";
			case "sep": return "09";
			case "oct": return "10";
			case "nov": return "11";
			case "dec": return "12";
			default: return "-1";
		}
	}
	/**
	 * Close the index opened and return the number of documents within the index
	 */
	public void Close() {
		try {
		    log.info(writer.maxDoc()+" documents indexed.");
		    writer.close();
		    log.info("Index closed successfully.");
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Cannot close the index: "+e.getMessage());
		}
	}
	protected IndexWriter getWriter() {
		return writer;
	}
	/**
	 * Create the index based on the GenBank DB and Annotation DB available
	 * @param args
	 */
	public void index() throws IndexerException {
		GenBankTree gbTree;
		GeoNameTree geoTree;
		try {
			gbTree = GenBankTree.getInstance();
			geoTree = GeoNameTree.getInstance();
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "An error occurred when creating the Taxonomy/GeoName trees : "+e.getMessage());
			throw new IndexerException("An error occurred when creating the taxonomy tree : "+e.getMessage());			
		}
		PreparedStatement stm = null;
		ResultSet rs = null;
		int batchSize = BATCH_SIZE;
		long start = 0;
		long stop = 0;
		try	{
			stm = conn.prepareStatement(COUNT_ACCESSIONS);
			rs = stm.executeQuery();
			rs.next();
			stop = rs.getLong("count");
			List<GenBankRecord> records;
			GenBankRecord record;
			updateGeonameIDsQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, UPDATE_GEONAME_IDS);
			updateGeonameTypesQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, UPDATE_GEONAME_TYPES);
			updateGeonameCountriesQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, UPDATE_GEONAME_COUNTRIES);
			for (start = 0; start < stop; start+=batchSize) {
				log.info("Retreiving Records for Indexer");
				records = dao.getIndexableRecords(batchSize, start);
				log.info("Indexing Records");
				while (!records.isEmpty()) {
					record = records.remove(records.size()-1); 
					try {
						indexAccession(record, gbTree, geoTree);
					}
					catch (Exception e) {
						log.log(Level.SEVERE, "There was an error indexing " + record.getAccession() + ": " + e.getMessage());
					}
					record = null;
				}
				if (updatedTypes > 0) {
					log.info("Updating GeoName Type Batch.");
					updateGeonameTypesQuery.executeBatch();
					updateGeonameTypesQuery.close();
					updateGeonameTypesQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, UPDATE_GEONAME_TYPES);
					updatedTypes = 0;
				}
				if (updatedCountries > 0) {
					log.info("Updating GeoName Country Batch.");
					updateGeonameCountriesQuery.executeBatch();
					updateGeonameCountriesQuery.close();
					updateGeonameCountriesQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, UPDATE_GEONAME_COUNTRIES);
					updatedCountries = 0;
				}
				log.info("Records Batch Indexed successfully");
				records.clear();
			}
			if (updatedTypes > 0) {
				log.info("Last GeoName Type Batch.");
				updateGeonameTypesQuery.executeBatch();
				updatedTypes = 0;
			}
			updateGeonameTypesQuery.close();
			if (updatedCountries > 0) {
				log.info("Last GeoName Country Batch.");
				updateGeonameCountriesQuery.executeBatch();
				updatedCountries = 0;
			}
			updateGeonameCountriesQuery.close();
			log.info("Updating missing Geoname IDs...");
			updateGeonameIDsQuery.executeBatch();
			updateGeonameIDsQuery.close();
			log.info("Missing Geoname IDs updated.");
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "An error occurred when fetching the Accessions : "+e.getMessage());
			throw new IndexerException("An error occurred when fetching the Accessions : "+e.getMessage());
		}
		finally {
			try {
				if (rs!=null) {
					rs.close();
				}
				if (stm!=null) {
					stm.close();
				}
				this.Close();
			}
			catch (SQLException e) {
				log.log(Level.SEVERE, "Error occurs when closing the resources taken on the genbank DB, nothing done: "+e.getMessage());
			}
			log.info("Total Missing Locations: "+missingLocs);
			log.info("Country Mapped Locations: "+countryMappedLocs);
			log.info("Unknown Locations: "+unknownLocs);
			log.info("Locations with too specific ID successfully mapped to country: "+tooSpecificCountryMappedLocs);
		}
	}
}
package edu.asu.zoophy.genbankfactory.index;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

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
import jp.ac.toyota_ti.coin.wipefinder.server.utils.PropertiesProvider;
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
	// Constants
	private final static String DATE_UNKNOWN = "10000101";
	private final static String FIELD_UNKNOWN = "Unknown"; 
	private final static String FIELD_UNAVAILABLE = "n/a";
	
	private DBQuery updateGeonameIDsQuery;
	private DBQuery updateGeonameTypesQuery;
	private DBQuery updateGeonameCountriesQuery;
	static final protected GenBankRecordDAOInt dao = new GenBankRecordSqlDAO();
	private Connection conn = null;
//	private Corpus corpus = null;
	private IndexWriter writer = null;
	private Directory luceneDir = null;
	private GeneNormalizer gn;
	
	private int missingLocs = 0;
	private int unknownLocs = 0;
	private int countryMappedLocs = 0;
	private int tooSpecificCountryMappedLocs = 0;
	private int updatedTypes = 0;
	private int updatedCountries = 0;
	private int missingContinentCount = 0;
	private Set<Long> missingContinents;
	
	private final Set<Integer> continents;
	private final Set<Integer> naCountries;
	private final Set<Integer> saCountries;
	private final Set<Integer> afCountries;
	private final Set<Integer> asCountries;
	private final Set<Integer> euCountries;
	private final Set<Integer> ocCountries;
	
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
	    	log.fatal( "Impossible to Initiate the Resources Provider:"+e.getMessage());
	    	throw new IndexerException("Impossible to Initiate the Resources Provider:"+e.getMessage());
	    }
	    try {
			gn = GeneNormalizer.getInstance();
		}
	    catch (Exception e) {
			log.fatal( "Could not get GeneNormalizer: "+e.getMessage());
		}
	    // initialize continent sets
	    continents = new HashSet<Integer>(Arrays.asList(6255146,6255147,6255148,6255149,6255150,6255151,6255152));
	    asCountries = new HashSet<Integer>(Arrays.asList(6255147,1149361,174982,587116,1210997,290291,1820814,1252634,1547376,1814991,2078138,614540,1643084,294640,1269750,1282588,99237,130758,248816,1861060,1527747,1831722,1873107,1835841,285570,1522867,1655842,272103,1227603,1327865,2029969,1282028,1733045,1282988,286963,1694008,1168579,6254930,289688,102358,1880251,163843,1605651,1220409,1218197,298795,1668284,290557,1512440,1562822,69543));
	    afCountries = new HashSet<Integer>(Arrays.asList(6255146,2589581,3351879,2361809,433561,2395170,933860,239880,2260494,2287781,2233387,3374766,203312,223816,357994,338010,337996,2400553,2300660,2413451,2420477,2309096,2372248,192950,921929,2275384,932692,2215636,2542007,1062947,2453866,2378080,934292,927384,1036973,3355338,2440476,2328926,935317,49518,241170,366755,3370751,2403846,2245662,51537,7909807,2410758,934841,2434508,2363686,2464461,149590,226074,1024031,953987,2461445,895949,878675));
	    saCountries = new HashSet<Integer>(Arrays.asList(6255150,3865483,3923057,3469034,3895114,3686110,3658394,3474414,3378535,3437598,3932488,3382998,3439705,3625428));
	    naCountries = new HashSet<Integer>(Arrays.asList(6255149,3576396,3573511,8505032,3577279,3374084,3578476,3573345,7626844,3572887,3582678,6251999,3624060,3562981,7626836,3575830,3508796,3580239,3425505,3579143,3595528,3608932,3723988,3489940,3575174,3580718,3576468,3578421,3570311,3578097,3996063,3617476,3703430,3424932,4566966,3585968,7609695,3576916,3573591,6252001,3577815,3577718,4796775));
	    euCountries = new HashSet<Integer>(Arrays.asList(6255148,3041565,783754,2782113,661882,3277605,2802361,732800,630336,2658434,146669,3077311,2921044,2623032,453733,2510769,660013,2622320,3017382,2635167,3042362,2411586,390903,3202326,719819,2963597,3042225,2629691,3175395,3042142,3042058,597427,2960313,458258,2993457,617790,3194884,718075,2562770,2750405,3144096,798544,2264397,798549,6290252,0,2661886,3190538,607072,3057568,3168068,690791,3164670,831053));
	    ocCountries = new HashSet<Integer>(Arrays.asList(6255151,5880801,2077456,1899402,2205218,2081918,4043988,4030945,2080185,4041468,2139685,2155115,2110425,4036232,2186224,4030656,2088628,4030699,1559582,2103350,4031074,1966436,4032283,2110297,5854968,2134431,4034749,4034894));
	    missingContinents = new LinkedHashSet<Long>();
	    //check the directory where the index will be opened
		File indexDir = new File(indexPath);
	    if (!indexDir.exists() || !indexDir.isDirectory()) {
	    	log.fatal( indexDir + " does not exist");
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
	    createIndex(Paths.get(indexPath));
	    log.info("Indexer initialized with index at:"+indexPath);
	}
	
	/**
	 * Open the index in creation mode
	 * @parameter the file where to create the index
	 */
	protected void createIndex(Path indexDir) throws IndexerException {
    	try {
    		IndexWriterConfig config = new IndexWriterConfig(new KeywordAnalyzer());
    		luceneDir = FSDirectory.open(indexDir);
    		writer = new IndexWriter(luceneDir, config);
        	log.info("Creation of a new index.");    		
    	}
    	catch(Exception e) {
    		log.fatal( "Impossible to create the index:"+e.getMessage());
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
		doc = new Document();
		setAccession(record);//TODO: marker for this function//
		setOrganism(record, gbTree);
		if (record.getSequence().getCollection_date() != null) {
			setDate(record);
		}
		else {
			doc.add(new StringField("Date", DATE_UNKNOWN , Field.Store.YES));
		}
		// add normalized date
		if (record.getSequence().getNormalizaed_date() != null) {
			doc.add(new StringField("NormalizedDate", formatNormalizedDate(record), Field.Store.YES));
		}
		else {
			doc.add(new StringField("Date", DATE_UNKNOWN, Field.Store.YES));
		}
		setHost(record, gbTree);
		if (record.getGenBankLocation() != null) {
			setGeographicLocation(record, geoTree);
		}
		else {
			doc.add(new TextField("Location", FIELD_UNKNOWN, Field.Store.YES));
			doc.add(new StringField("Country", FIELD_UNKNOWN, Field.Store.YES));
			doc.add(new StringField("CountryCode", FIELD_UNKNOWN, Field.Store.YES));
			doc.add(new StringField("GeonameID", String.valueOf(geoTree.getRoot().getID()), Field.Store.YES));
			doc.add(new StringField("LocationType",FIELD_UNKNOWN, Field.Store.YES));
			unknownLocs++;
		}
		// adding state field
		if (record.getGenBankLocation().getState() != null) {
			doc.add(new StringField("State", record.getGenBankLocation().getState(), Field.Store.YES)) ;
		}
		else {
			doc.add(new StringField("State", FIELD_UNKNOWN, Field.Store.YES));
		}
				
		if (record.getSequence().getPub() != null) {
			doc.add(new StringField("PubmedID", String.valueOf(record.getSequence().getPub().getPubId()), Field.Store.YES));
		}
		else {
			doc.add(new StringField("PubmedID", FIELD_UNAVAILABLE, Field.Store.YES));
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
			doc.add(new TextField("Organism", FIELD_UNKNOWN, Field.Store.YES));
		}
		if (record.getSequence().getStrain() != null) {
			doc.add(new TextField("Strain", record.getSequence().getStrain(), Field.Store.YES));
		}
		else {
			doc.add(new TextField("Strain",FIELD_UNKNOWN, Field.Store.YES));
		}
		if (record.getSequence().getDefinition() != null) {
			doc.add(new TextField("Definition", record.getSequence().getDefinition(), Field.Store.YES));
		}
		else {
			doc.add(new TextField("Definition",FIELD_UNKNOWN, Field.Store.YES));
		}
		String segmentLength = String.valueOf(record.getSequence().getSegment_length());
		while (segmentLength.length() < 5) {
			segmentLength = "0"+segmentLength;
		}
		doc.add(new StringField("SegmentLength", segmentLength,Field.Store.YES));
		
		//add gene info
		/* First add the genes in index and check if any of the gene contains complete
		 * 		if yes => then set the flat True 
		 * After this check if definition contains "complete" word
		 * 		if yes = > then add Gene Complete in index and end.
		 * 		if no => if flag isn't true check for condition if all the genes belong the genomelist that is represented by taxon 		
		 * 			if yes => set flat to true
		 * Finally check for condition that if completeFlag is true and definition doesn't contain partial
		 * 		then add Gene Complete to index otherwise end
		 * 
		 */
		try {
			if (record.getGenes() != null) {
				List<String> genes = new LinkedList<String>();
				Boolean completeFlag = false;
				for (Gene g : record.getGenes()) {
					if (g.getName() != null) {
						if (g.getName().equalsIgnoreCase("complete")) {
							completeFlag = true;
						}
						else {
							doc.add(new StringField("Gene", g.getName(), Field.Store.YES));
						}
					genes.add(g.getName());
					}
				}
				if (!record.getSequence().getDefinition().toLowerCase().contains("influenza")) {
					
					if (record.getSequence().getDefinition().toLowerCase().contains("complete genome")) {
						doc.add(new StringField("Gene", "Complete", Field.Store.YES));
					} else { 
					
						if(!completeFlag) {
							List<String> fullGenome = gn.getFullGenomeList(record.getSequence().getTax_id());
							if (fullGenome != null && (genes.containsAll(fullGenome) )) {
								completeFlag = true;
							}
						}
							
						if (completeFlag && !record.getSequence().getDefinition().contains("partial") ) {
							doc.add(new StringField("Gene", "Complete", Field.Store.YES));	
						}
					}
				}
			}
			
		}
		catch (Exception e) {
			log.warn( "Error adding gene infor of  "+ record.getAccession()  +e.getMessage());
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
				log.fatal( "Error indexing taxonomy for taxon: " + record.getSequence().getTax_id());
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
			log.fatal( "Error checking for full genome: "+e.getMessage());
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
			doc.add(new TextField("Host_Name", FIELD_UNKNOWN.toLowerCase(), Field.Store.YES));
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
				log.fatal( "Error indexing taxonomy for host taxon: " + record.getHost().getTaxon());
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
						log.fatal( "Issue updating geoname ID: "+e.getMessage());
					}
				}
			}
		}
		if (record.getGenBankLocation().getLocation() == null || record.getGenBankLocation().getLocation().equalsIgnoreCase("unknown")) {
			unknownLocs++;
		}
		GeoNameNode g;
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
								log.fatal( "Issue updating Geoname ID: "+e.getMessage());
							}
						}
					}
				}
				if (g != null) {
					if (g.getConcept() != null) {
						doc.add(new TextField("Location", g.getConcept(), Field.Store.YES));
					}
					else {
						doc.add(new TextField("Location", FIELD_UNKNOWN, Field.Store.YES));
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
							doc.add(new StringField("LocationType", FIELD_UNKNOWN, Field.Store.YES));
						}
					}
					else {
						doc.add(new StringField("Latitude", String.valueOf(0.0), Field.Store.YES));
						doc.add(new StringField("Longitude", String.valueOf(0.0), Field.Store.YES));
						doc.add(new StringField("LocationType", FIELD_UNKNOWN, Field.Store.YES));
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
					Set<Integer> gIDs = new LinkedHashSet<Integer>();
					while (g != null) {
						if (gIDs.contains(g.getID())) {
							if (g.getID() == 472755) { //weird Russian location
								g = geoTree.getMapIDNodes().get(2017370);
							}
							else {
								log.fatal( "Error loop Found for GeonameID: "+g.getID());
								g = null;
								gIDs.add(geoTree.getRoot().getID());
							}
						}
						else {
							gIDs.add(g.getID());
							if (country_code == null) {
								if (g.getLocation() != null) {
									country_code = g.getLocation().getCountry();
								}
							}
							g = (GeoNameNode) g.getFather();
						}
					}
					if (Collections.disjoint(gIDs, continents)) {
						if (country_code != null) {
							Integer countryID = geoTree.getCountryLookup().get(country_code);
							if (countryID != null) {
								gIDs.add(countryID);
							}
						}
						if (Collections.disjoint(gIDs, continents)) {
							Integer contID = assignContinent(gIDs);
							if (contID.intValue() == 1) {
								missingContinents.add(record.getGenBankLocation().getId());
								missingContinentCount++;
							}
							gIDs.add(contID);
						}
					}
					for (Integer gID : gIDs) {
						doc.add(new StringField("GeonameID", String.valueOf(gID), Field.Store.YES));
					}
					gIDs.clear();
				}
				else {
					if (record.getGenBankLocation().getLocation() != null) {
						doc.add(new TextField("Location", record.getGenBankLocation().getLocation(), Field.Store.YES));
					}
					else {
						doc.add(new TextField("Location", FIELD_UNKNOWN, Field.Store.YES));
					}
					doc.add(new StringField("GeonameID", String.valueOf(id), Field.Store.YES));
					doc.add(new StringField("GeonameID", String.valueOf(geoTree.getRoot().getID()), Field.Store.YES));
				}
				if (country_code == null) {
					doc.add(new TextField("Country", FIELD_UNKNOWN, Field.Store.YES));
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
						log.fatal( "ERROR setting country name for country code: "+country_code);
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
					doc.add(new TextField("Location", FIELD_UNKNOWN, Field.Store.YES));
				}
				doc.add(new StringField("CountryCode", FIELD_UNKNOWN, Field.Store.YES));
				doc.add(new TextField("Country", FIELD_UNKNOWN, Field.Store.YES));
				doc.add(new StringField("GeonameID", String.valueOf(geoTree.getRoot().getID()), Field.Store.YES));
			}
			
			
		}
		catch (Exception e) {
			log.fatal( "Error indexing location for Geoname id: " + id + " " + e.getMessage());
			doc.add(new TextField("Location", FIELD_UNKNOWN, Field.Store.YES));
			doc.add(new StringField("GeonameID", String.valueOf(id), Field.Store.YES));
			doc.add(new StringField("GeonameID", String.valueOf(geoTree.getRoot().getID()), Field.Store.YES));
		}
	}
	
	/**
	 * Assigns continent to locations missing Continent (27% of total) in their tree
	 * @param gIDs
	 * @return GeonameID of assigned Continent
	 */
	private Integer assignContinent(Set<Integer> gIDs) {
		if (!Collections.disjoint(gIDs, naCountries)) {
			return 6255149;
		}
		else if (!Collections.disjoint(gIDs, saCountries)) {
			return 6255150; 
		}
		else if (!Collections.disjoint(gIDs, asCountries)) {
			return 6255147;
		}
		else if (!Collections.disjoint(gIDs, afCountries)) {
			return 6255146;
		}
		else if (!Collections.disjoint(gIDs, euCountries)) {
			return 6255148;
		}
		else if (!Collections.disjoint(gIDs, ocCountries)) {
			return 6255151;
		}
		else {
			return 1;
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
	 * @return Lucene formatted date
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
				log.fatal( "Impossible to format the date [" + record.getSequence().getCollection_date().trim() + "] for the accession ["+record.getAccession()+"], insert the default date 1-1-1");			
			}
		}
		return "10000101";
	}
	protected String formatNormalizedDate(GenBankRecord record) {
		if (record.getSequence().getNormalizaed_date() != null) {
			try {
				String date = record.getSequence().getNormalizaed_date().trim();
				if (date.matches("[0-9]{4}")) {
					return DateTools.dateToString(DateTools.stringToDate(date),Resolution.YEAR);
				}
				if (date.matches("[0-9]{1,2}-[A-Za-z]{3}-[0-9]{2}")) {
					String[] dateComponents = record.getSequence().getNormalizaed_date().trim().split("-");
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
					String[] dateComponents = record.getSequence().getNormalizaed_date().trim().split("-");
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
					String[] dateComponents = record.getSequence().getNormalizaed_date().trim().split("-");
					return DateTools.dateToString(DateTools.stringToDate(getYear(dateComponents[1])+getMonth(dateComponents[0])),Resolution.MONTH);
				}
				if (date.matches("[0-9]{1,2}-[A-Za-z]{3}-[0-9]{4}")) {
					String[] dateComponents = record.getSequence().getNormalizaed_date().trim().split("-");
					if(dateComponents[0].length()==1) {
						dateComponents[0] = "0"+dateComponents[0];
					}
					return DateTools.dateToString(DateTools.stringToDate(getYear(dateComponents[2])+getMonth(dateComponents[1])+getDay(dateComponents[0])),Resolution.DAY);
				}
			}
			catch (Exception e) {
				log.fatal( "Impossible to format the date [" + record.getSequence().getNormalizaed_date().trim() + "] for the accession ["+record.getAccession()+"], insert the default date 1-1-1");			
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
		    luceneDir.close();
		    log.info("Index closed successfully.");
		}
		catch (Exception e) {
			log.fatal( "Cannot close the index: "+e.getMessage());
		}
	}
	protected IndexWriter getWriter() {
		return writer;
	}
	/**
	 * Create the index based on the GenBank DB and Annotation DB available
	 */
	public void index() throws IndexerException {
		GenBankTree gbTree;
		GeoNameTree geoTree;
		try {
			gbTree = GenBankTree.getInstance();
			geoTree = GeoNameTree.getInstance();
		}
		catch (Exception e) {
			log.fatal( "An error occurred when creating the Taxonomy/GeoName trees : "+e.getMessage());
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
						log.fatal( "There was an error indexing " + record.getAccession() + ": " + e.getMessage());
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
			log.fatal( "An error occurred when fetching the Accessions : "+e.getMessage());
			throw new IndexerException("An error occurred when fetching the Accessions : "+e.getMessage());
		}
		finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (stm != null) {
					stm.close();
				}
				this.Close();
			}
			catch (SQLException e) {
				log.fatal( "ERROR could not close SQL resource(s): "+e.getMessage());
			}
			log.info("Total Missing Locations: "+missingLocs);
			log.info("Country Mapped Locations: "+countryMappedLocs);
			log.info("Unknown Locations: "+unknownLocs);
			log.info("Locations with too specific ID successfully mapped to country: "+tooSpecificCountryMappedLocs);
			log.info("Total Records missing Continents: "+missingContinentCount);
			log.info(missingContinents.size()+" unique Geonames missing Continents: "+missingContinents.toString());
		}
	}
}
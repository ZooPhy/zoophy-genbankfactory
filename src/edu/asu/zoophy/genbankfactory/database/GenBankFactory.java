package edu.asu.zoophy.genbankfactory.database;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.PropertiesProvider;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider.RP_PROVIDED_RESOURCES;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import edu.asu.zoophy.genbankfactory.utils.pmcid.PmidPmcidMapperCSV;
import edu.asu.zoophy.genbankfactory.utils.pmcid.PmidPmcidMapperInt;

/**
 * Create a GenBank from the Dump of the GenBank flat files of Genbank server
 * @author Mark/Davy/Demetrius
 */
public class GenBankFactory {
	private static Map<String, String> properties;
	private long recordsAvailable;
	private long recordsProcessed;
	private final static Logger log = Logger.getLogger("GenBankFactory");
	private ResourceProvider Provider = null;
	//variables that are configured from the properties file
	private static boolean use_local_files = false;
	private static int file_limit;
	private static boolean isDownload_only = false;
	//non static variables
	private String genbank_url; 
	private String pmcid_url;
	private String start_at_file;
	private InputStream gzipStream;
	private String gb_directory;
	private String pmcid_directory;
	private BufferedReader buffered;
	public static int batch_count;
	//Data Access Object for db operations//
	private static GenBankRecordDAOInt dao;
	private static GenBankFactory instance = null;
	protected PmidPmcidMapperInt mapper;
	private static String geoDirectory; 
	private static String geoInfoFile;
	private static String geoMappingFile;
	private static String geoCountryFile;
	private static String geoADMFile;
	private static String geoUrl;
	private DBManager dbManager;
	
	/**
	 * Decided to try out using a singleton. Not sure if that will fix my issues with the GWT project
	 * @return single instance of GenBankFactory
	 */
	public static GenBankFactory getInstance() {
		if (instance == null) {
			try {
				instance = new GenBankFactory();
			} 
			catch (Exception e) {
				log.fatal( "Couldn't get a Factory instance ):");
			}
		}
		return instance;
	}
	
	@SuppressWarnings("static-access")
	private GenBankFactory() throws Exception {
    	dao = new GenBankRecordSqlDAO();
		log.info("GenBank Factory Starting...");
	    try {
	    	Provider = new ResourceProvider();
	    	PropertiesProvider pp = new PropertiesProvider("./GenBankFactory.local.properties");
	    	Provider.addResource(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER, pp);
	    	dbManager = new DBManager();
	    	Provider.addResource("DBGenBank", dbManager);
	    	loadProperties();
	    }
	    catch(Exception e) {
	    	log.fatal( "Impossible to Initiate the Resources Provider:"+e.getMessage());
	    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
	    }
		log.info("GenBank Factory Instantiate.");
	}
	
	/**
	 * This method gets the properties from the properties file
	 * using PropertiesProvider.java
	 */
	private void loadProperties() {
		try {
			properties = new HashMap<String, String>();
			genbank_url = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("genbank.download_url");
			pmcid_url = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("pmcid.download_url");
			gb_directory = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("gzip.directory");
			pmcid_directory = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("pmcid.csv.dir");
			start_at_file = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("genbank.start_at_file");
			if(start_at_file.isEmpty())
				start_at_file = null;
			file_limit = Integer.parseInt((String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("genbank.file_limit"));
			if(((String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("genbank.download_only")).equals("yes"))
				isDownload_only = true;
			if(((String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("genbank.use_local_files")).equals("yes"))
				use_local_files = true;
			geoDirectory = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("geo.dir");
			geoInfoFile = "allCountries";
			geoMappingFile = "hierarchy";
			geoCountryFile = "countryInfo.txt";
			geoADMFile = "admin1CodesASCII.txt";
			geoUrl = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("geo.url");
			
			String bigDB = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("DB.Big.Name");
			String smallDB = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("DB.Small.Name");
			String bigIndex = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("index.big.dir");
			String smallIndex = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("index.ui.dir");
			String taxDumpFolder = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("genbank.taxonomy");
			String taxDumpURL = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("genbank.taxonomy.url");
			String pH1N1List = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("ph1n1.list");
			String predictorCSV = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("predictor.csv");
			String geoNamesIndexDirectory = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("geonames.index.location");
			String geoNamesMappingFile = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("geonames.mapping.file");
			
			
			properties.put("GenBankURL", genbank_url);
			properties.put("PmcidURL", pmcid_url);
			properties.put("GenBankDir", gb_directory);
			properties.put("PmcidDir", pmcid_directory);
			properties.put("StartAtFile", start_at_file);
			properties.put("FileLimit", String.valueOf(file_limit));
			properties.put("isDownloadOnly", String.valueOf(isDownload_only));
			properties.put("GeonameDir", geoDirectory);
			properties.put("GeonameInfoFile", geoInfoFile);
			properties.put("GeonameMappingFile", geoMappingFile);
			properties.put("GeonameCountryFile", geoCountryFile);
			properties.put("GeonameADMFile", geoADMFile);
			properties.put("GeonameURL", geoUrl);
			properties.put("TaxDumpFolder", taxDumpFolder);
			properties.put("TaxDumpURL", taxDumpURL);
			properties.put("BigDB", bigDB);
			properties.put("SmallDB", smallDB);
			properties.put("BigIndex", bigIndex);
			properties.put("SmallIndex", smallIndex);
			properties.put("PH1N1List", pH1N1List);
			properties.put("predictor.csv", predictorCSV);
			properties.put("GeoNamesIndexDir", geoNamesIndexDirectory);
			properties.put("GeoNamesMappingFile", geoNamesMappingFile);
		} 
		catch (Exception e) {
			log.fatal( "error getting properties file");
			e.printStackTrace();
		}
	}
	
	public String getProperty(String key) {
		return properties.get(key);
	}
	
	/**
	 * This method scrapes the genbank website, downloads each gzip file,
	 * and runs the readFile() method on them. Finally, it executes the batch
	 * after each gzip file
	 * Alternative can be commanded from the properties file by asking the process of local files
	 */
	public void getFiles(String filter) throws Exception {
		Document document;
		recordsAvailable = 0;
		recordsProcessed = 0;
		//process the dump downloaded in local
		if(use_local_files) {
			try {
				mapper = new PmidPmcidMapperCSV(pmcid_directory);
				for (final File fileEntry : new File(gb_directory).listFiles()) {
					if(file_limit == 0) {
						break;
					}
					String filename = fileEntry.getName();
			        if (!fileEntry.isDirectory() && filter(".seq.gz", filename) && !filter("gbcon", filename) && filter(filter, filename)) {		        	
			        	if(start_at_file == null || filename.equals(start_at_file + ".seq.gz")){
							start_at_file = null; //found file to start from
							log.info("********* Reading " + filename + " *********");
							ArrayList<GenBankRecord> parsedRecords = processFile(filename);
							dao.dumpRecords(parsedRecords);
							log.info("Finished with file " + filename);
							file_limit--;
						}
			        }
			    }
			}
			catch(Exception e) {
				log.fatal( "Impossible to find/process the dump from local file: "+e.getMessage());
				throw new Exception("Impossible to find/process the dump from local file: "+e.getMessage());
			}
		}
		else { //access the files from the server 
			try {
				downloadGeoFiles();
				downloadCSV();
				mapper = new PmidPmcidMapperCSV(pmcid_directory);
				document = Jsoup.connect(genbank_url).get();
				Elements links = document.select("a");
				for(int i = 0; i < links.size(); i++) {
					if(file_limit == 0) {
						break;
					}
					String filename = links.get(i).toString().split("[\\<\\>]")[2];
					if(filter(".seq.gz", filename) && !filter("gbcon", filename) && filter(filter, filename)) {
						if(start_at_file == null || filename.equals(start_at_file + ".seq.gz")) {
							start_at_file = null; //found file to start from
							download(filename);
							if(!isDownload_only) {
								log.info("Reading " + filename);
								ArrayList<GenBankRecord> parsedRecords = processFile(filename);
								dao.dumpRecords(parsedRecords);
								log.info("Finished batch on " + filename);
								remove(filename);
							}
							file_limit--;
						}
					}
				}
				deleteCSV();
			} 
			catch (IOException e) {
				log.fatal( "IOException in getFiles(): "+e.getMessage());
				throw new Exception("IOException in getFiles(): "+e.getMessage());
			} 
			catch (Exception e) {
				log.fatal( "Unexpected error occurred when processing the dump of GenBank from the server: "+e.getMessage());
				throw new Exception("Unexpected error occurred when processing the dump of GenBank from the server: "+e.getMessage());
			}
			finally {
				mapper.free();
			}
		}
		log.info("Proccessed " + recordsProcessed + " records out of " + recordsAvailable + " given records.");
		if (recordsProcessed != recordsAvailable) {
			log.fatal( "ERROR!!! MISSING " + (recordsAvailable-recordsProcessed) + " RECORDS IN TOTAL!!!");
		}
		else {
			log.info("SUCCESSFULLY PROCESSED EVERY RECORD FROM GIVEN DATA DUMP FILES");
		}
	}
	
	private void downloadGeoFiles() {
		log.info("Downloading GeoName files...");
		downloadGeoFile(geoInfoFile + ".zip");
		extractFile(geoInfoFile);
		downloadGeoFile(geoMappingFile + ".zip");
		extractFile(geoMappingFile);
		downloadGeoFile(geoCountryFile);
		downloadGeoFile(geoADMFile);
		log.info("Finished Downloading GeoName files");
	}
	
	private static void extractFile(String name) {
		log.info("Extracting " + name + "...");
		BufferedOutputStream out;
		ZipInputStream zin;
		String zipfile = geoDirectory + name + ".zip";
		ZipEntry entry;
	    String tempName;
		try {
			zin = new ZipInputStream(new FileInputStream(zipfile));
		    while ((entry = zin.getNextEntry()) != null) {
		    	tempName = entry.getName(); 
		    	if ((name + ".txt").equalsIgnoreCase(tempName)) {
		    		break;
		    	}
		    }
			byte[] buffer = new byte[4096];
		    out = new BufferedOutputStream(new FileOutputStream(new File(geoDirectory, name + ".txt")));
		    int count = -1;
		    while ((count = zin.read(buffer)) != -1) {
		    	out.write(buffer, 0, count);
		    }
		    out.close();
			zin.close();
			log.info("Finished Extracting " + name);
			log.info("Deleting " + name + ".zip" + "...");
			Path path = Paths.get(zipfile);
			Files.delete(path);
			log.info("Deleted " + name + ".zip");
		}
		catch (Exception e) {
			log.fatal( "Error extracting " + name + ": " + e.getMessage());
		}
	}
	
	private void downloadGeoFile(String filename) {
		String gDir = geoDirectory;
		String gUrl = geoUrl + filename;
		log.info("Downloading " + filename + "...");
		InputStream in = null;
	    FileOutputStream fout = null;
	    try {
	    	URL url = new URL(gUrl);
	        in = new BufferedInputStream(url.openStream());
	        fout = new FileOutputStream(gDir + filename);
	        final byte data[] = new byte[1024];
	        int count;
	        while ((count = in.read(data)) != -1) {
	            fout.write(data, 0, count);
	        }
	        fout.flush();
	        in.close();
	        fout.close();
	        log.info("Finished Downloading" + filename);
	    }
	    catch (IOException e) {
	    	log.fatal( "IOException when downloading " + filename + ": " + e.getMessage());
		}
	}

	private void deleteCSV() throws IOException {
		String dir = pmcid_directory;
		log.info("Deleting PMC-ids.csv...");
		String textPath = dir+"PMC-ids.csv";
		Path path = Paths.get(textPath);
		Files.delete(path);
		log.info("Deleted PMC-ids.csv");
	}

	private void downloadCSV() throws Exception { 
		String dir = pmcid_directory;
		String fileUrl = pmcid_url;
		InputStream in = null;
	    FileOutputStream fout = null;
	    try {
	    	log.info("Downloading PMC-ids.csv.gz...");
	    	URL url = new URL(fileUrl);
	        in = new BufferedInputStream(url.openStream());
	        fout = new FileOutputStream(dir+"PMC-ids.csv.gz");
	        final byte data[] = new byte[1024];
	        int count;
	        while ((count = in.read(data)) != -1) {
	            fout.write(data, 0, count);
	        }
	        fout.flush();
	        in.close();
	        fout.close();
	        log.info("Finished downloading PMC-ids.csv.gz. Exctracting csv...");
	        GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(dir+"PMC-ids.csv.gz"));
	        fout = new FileOutputStream(dir+"PMC-ids.csv");
	        int len;
	        byte[] buffer = new byte[1024];
	        while ((len = gzis.read(buffer)) > 0) {
	        	fout.write(buffer, 0, len);
	        }
	        gzis.close();
	        fout.close();
	        log.info("csv extracted");
	        log.info("Deleting PMC-ids.csv.gz");
	        String textDeletePath = dir+"PMC-ids.csv.gz";
	        Path deletePath = Paths.get(textDeletePath);
			Files.delete(deletePath);
	        log.info("Deleted PMC-ids.csv.gz");
	    } 
	    catch (IOException e) {
	    	log.fatal( "IOException when downloading PMC-ids.csv: " + e.getMessage());
		}
	}
	
	/**
	 * Similar to using string.contains, except null filters always return true.
	 * @param filter
	 * @param filename
	 * @return True if filename contains the filter.
	 */
	private boolean filter(String filter, String filename) {
		if (filter == null) {
			return true;
		}
		else {
			return filename.contains(filter);
		}
	}
	
	/**
	 * This method is called by getFiles(). It downloads the specified gzip file
	 * into the specified gb_directory
	 */
	public void download(String filename) {
		log.info("Downloading " + filename);
		String dir = gb_directory + filename;
		String fileUrl = genbank_url + "/" + filename;
		InputStream in = null;
	    FileOutputStream fout = null;
	    try {
	    	URL url = new URL(fileUrl);
	        in = new BufferedInputStream(url.openStream());
	        fout = new FileOutputStream(dir);
	        final byte data[] = new byte[1024];
	        int count;
	        while ((count = in.read(data)) != -1) {
	            fout.write(data, 0, count);
	        }
	        fout.flush();
	        in.close();
	        fout.close();
	    } 
	    catch (IOException e) {
	    	log.fatal( "IOException when downloading " + filename);
		}
	}
	
	/**
	 * Deletes the GenBank file after it is done being processed.
	 * Makes data dump cleanup a lot easier.
	 * @param filename
	 */
	private void remove(String filename) {
		log.info("Deleting " + filename);
		String textPath = gb_directory + filename;
		Path path = Paths.get(textPath);
		try {
			Files.delete(path);
			log.info("Successfully deleted " + filename);
		}
		catch (Exception e) {
			log.fatal( "Could not delete" + filename);
		}
	}
	
	/**
	 * Read the file and create a GenBankRecord object before inserting it
	 * @param filename
	 * @return records proccessed from the given file
	 */
	public ArrayList<GenBankRecord> processFile(String filename) {
		ArrayList<GenBankRecord> processedRecords = null;
		String encoding = "UTF-8";
		InputStream fileStream;
		try {
			int avail = 0;
			fileStream = new FileInputStream(gb_directory + filename);
			gzipStream = new GZIPInputStream(fileStream);
			Reader decoder = new InputStreamReader(gzipStream, encoding);
			buffered = new BufferedReader(decoder);
			//skip header
			for(int i = 0; i < 10; i++) {
				String s = buffered.readLine();
				if (s.contains("reported sequences")) {
					s = s.substring(0, s.indexOf("loci")-1).trim();
					avail = Integer.parseInt(s);
				}
			}
			processedRecords = processLoci(buffered);
			log.info("Read " + processedRecords.size() + " loci out of " + avail + " total from " + filename);
			recordsProcessed += processedRecords.size();
			recordsAvailable += avail;
			if (avail != processedRecords.size()) {
				log.fatal( "ERROR!!! MISSING " + (avail-processedRecords.size()) + " RECORDS!!!");
			}
		} 
		catch (IOException e) {
			log.fatal( "IOException reading GZIP header: " + e.getMessage());
		}
		catch(Exception e){
			log.fatal( "Unexpected error when reading the file: "+e.getMessage());
		}
		finally {
			if(buffered!=null) {
				try {
					buffered.close();
				} 
				catch (IOException e) {
					log.fatal( "Impossible to close the file ["+filename+"]: "+e.getMessage());
				}
			}
		}
		return processedRecords;
	}
	
	/**
	 * Process all Loci written in the file
	 * @param buffered a buffer opened on the file to process
	 * @return the count of records created and inserted
	 */
	public ArrayList<GenBankRecord> processLoci(BufferedReader buffered) throws Exception {
		ArrayList<GenBankRecord> parsedRecords = new ArrayList<GenBankRecord>();
		String line = "";
		List<String> locusLines = new ArrayList<String>(100);  
		while((line = buffered.readLine())!=null) { 
			if(line.equals("//")) {
				parsedRecords.add(processLocus(locusLines));
				locusLines = new ArrayList<String>(100);
			}
			else {
				locusLines.add(line);
			}
		}
		return parsedRecords;
	}
	
	/**
	 * Create the GenBankRecordDumped from the lines read
	 * @param locusLines
	 */ GenBankRecord processLocus(List<String> locusLines) {
		GenBankRecordParser parser = new GenBankRecordParser(locusLines, mapper);
		return parser.getGBRecord();
	}
	 
	/**
	 * This method checks to see if a string is all uppercase
	 * This is useful in parsing
	*/
	public static boolean isAllUpperCase(String s) {
	    for (char c : s.toCharArray()) {
	        if(! Character.isUpperCase(c))
	            return false;
	    }
	    return true;
	}
	
	/**
	 * This method is for retreiving records from the database
	 * @param accession Unique identifier for GenBankRecord
	 * @return Full GenBankRecord object from Database
	 */
	public GenBankRecord findFullRecord(String accession) {
		GenBankRecord record = dao.getRecord(accession);
		return record;
	}
	
	/**
	 * Adding light records to the UI only database
	 * @param recs
	 */
	public void insertUIonlyRecords(List<GenBankRecord> recs) {
		dao.dumpRecords(recs);
	}
	
	public void switchDB(String newDB) throws Exception {
		log.warn("Switching to new DB: "+newDB);
		dbManager.connectToNewDB(newDB);
	}
	
	public PossibleLocation getRecordLocation(String accession, Boolean normalized) {
		if (normalized) {
			return dao.findGeonameLocation(accession);
		}
		else {
			return dao.findGenBankLocation(accession);
		}
	}
	
	public void insertGenBankLocations(List<PossibleLocation> locs) {
		dao.insertGenBankLocations(locs);
	}
	
}
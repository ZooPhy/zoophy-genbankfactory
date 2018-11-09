package edu.asu.zoophy.genbankfactory.utils.pmcid;

import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class PmidPmcidMapperCSV implements PmidPmcidMapperInt {
	private static final Logger log = Logger.getLogger("PmidPmcidMapperCSV");
	private static String csv_file_path;
	private static final String [] FILE_HEADER_MAPPING = {"Journal Title","ISSN","eISSN","Year","Volume","Issue","Page","DOI","PMCID","PMID","Manuscript Id","Release Date"};
	protected Map<String,String> pmidPubmedRefMap = null;
	//protected Map<String,PublicationRef> pmcidPubmedRefMap = null;
	
	public PmidPmcidMapperCSV(String csvPath) throws Exception {
		csv_file_path = csvPath + "PMC-ids.csv";
		parseCSVFile();
	}
	
	protected void parseCSVFile() throws Exception {
		FileReader fileReader = null;
		CSVParser csvFileParser = null;
		long rest = 0;
		long nbrRef = 0;
		try {
			//log.info("Start to parse the CSV at ["+csv_file_path+"]...");
			fileReader = new FileReader(new File(csv_file_path));
			CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FILE_HEADER_MAPPING);
			csvFileParser = new CSVParser(fileReader, csvFileFormat);
			//pmcidPubmedRefMap = new HashMap<String, PublicationRef>(4000000);//kills the java heap
			pmidPubmedRefMap = new HashMap<String, String>(4000000);
			boolean isHeaderParsed = false;
			for (CSVRecord record: csvFileParser) {
				
				try {
					nbrRef++;
					//Create a new student object and fill his data
					if(!isHeaderParsed) {
						isHeaderParsed = true;
					}
					else {
						//TODO: re-add if we want to use all that info//
						//PublicationRef pubref = new PublicationRef(record.get("Journal Title"),record.get("ISSN"),record.get("eISSN"),record.get("Year"),record.get("Volume"),record.get("Issue"),record.get("Page"),record.get("DOI"),record.get("PMCID"),record.get("PMID"),record.get("Manuscript Id"),record.get("Release Date"));
						//TODO uncomment if we run that on a bigger machine: 
						//pmcidPubmedRefMap.put(pubref.getPMCID(), pubref);
						pmidPubmedRefMap.put(record.get("PMID"), record.get("PMCID"));
					}
					rest = nbrRef % 250000;
					if(rest==0) {
						StringBuilder out = new StringBuilder("References processed : ");
						out.append(nbrRef);
						log.info(out.toString());
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					log.warn("Unable to parse line no " + nbrRef + " Hence skipping it."  );
				}
			}
		}
		catch(Exception e) {
			log.fatal( "Impossible to parse the CSV file ["+csv_file_path+"], error occurred at line["+(nbrRef+1)+"]: "+e.getMessage());
			throw new Exception("Impossible to parse the CSV file ["+csv_file_path+"], error occurred at line["+(nbrRef+1)+"]: "+e.getMessage());
		}
		finally {
			fileReader.close();
			csvFileParser.close();
		}
	}
	@Override
	public String getPMCID(String PMID) {
		return pmidPubmedRefMap.get(PMID);
	}
	@Override
	public String getPMID(String PMCID) {
		log.fatal( "Not implement, I don't have enough memory on my small machine...");
		return null;
		//TODO uncomment if we run that on a bigger machine:
//		PublicationRef ref = pmcidPubmedRefMap.get(PMCID);
//		if(ref!=null) {
//			return ref.getPMID();
//		}
//		else { 
//			return null;
//		}
	}

	@Override
	public void free() {
		pmidPubmedRefMap.clear();
	}
}
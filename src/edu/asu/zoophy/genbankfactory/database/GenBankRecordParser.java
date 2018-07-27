package edu.asu.zoophy.genbankfactory.database;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.asu.zoophy.genbankfactory.utils.pmcid.PmidPmcidMapperInt;

/**
 * Given a record of GeneBank this parse it to a GenBankRecordDumped object ready to be inserted in the DB
 * @author Davy/Demetrius
 */
public class GenBankRecordParser {
	private final Logger log = Logger.getLogger("GenBankRecordParser");
	private Integer indexFeatures = null;
	private Integer indexOrigin = null;
	private List<String> locusLines = null;
	private Map<String, String> mapInformation = null;
	private List<Publication> extractedPub = null;
	private List<Feature> features = null;
	private List<Gene> genes = null;
	private PmidPmcidMapperInt mapper;
	// new design
	private String institutionName = null;
	private String submissionDate = null;
	private List<List<Author>> authorList = null;
	
	//indexLocus is always 0
	/**
	 * @param pLocusLines the record read from the flat file, one line
	 * @param mapper PMID mapper
	 */
	public GenBankRecordParser(List<String> pLocusLines, PmidPmcidMapperInt mapper) {
		this.mapper = mapper;
		locusLines = pLocusLines;
		mapInformation = new HashMap<String, String>();
		features = new ArrayList<Feature>();
		genes = new ArrayList<Gene>();
		authorList = new ArrayList<List<Author>>();
		extractedPub = new ArrayList<Publication>();
		int index = 0;
		while(index<locusLines.size()) {
			//log.info(index+" -> "+locusLines.get(index));
			if(locusLines.get(index).startsWith("FEATURES")) {
				indexFeatures = index;
			}
			else if(locusLines.get(index).startsWith("ORIGIN")) {
				indexOrigin = index;	
			}
		index++;
		}
		if(indexFeatures==null||indexOrigin==null) {
			log.warning("I found a GBRecord without Origin or Features section: "+this.toString());
		}
		processLocus(0, (indexFeatures - 1));
		if (indexOrigin != null) {
			processFeatures(indexFeatures, (indexOrigin - 1));
			processOrigin(indexOrigin);
		}
	}
	/**
	 * Process the part from LOCUS to FEATURES (excluded)
	 * @param start
	 * @param end
	 */
	protected void processLocus(Integer start, Integer end) {
		List<String> element = new ArrayList<String>();
		int index = start;
		while(index<=end) {
			element.add(locusLines.get(index));
			if(index+1<=end) {
				if(!locusLines.get(index+1).startsWith(" ")) {//it's another element starting we can process the one in memory 
					processLocusElement(element);
					element = new ArrayList<String>();
				}
			}
			else{//it's the last element need to be processed
				processLocusElement(element);
			}
		index++;
		}
	}
	/**
	 * We have the lines of the element
	 * @param elementLines
	 */
	protected void processLocusElement(List<String> elementLines) {
		String[] components = elementLines.get(0).split(" ");
		switch (components[0]) {
			case "ACCESSION":
				processAccession(elementLines);
				break;
			case "DEFINITION":
				processDefinition(elementLines);
				break;
			case "SOURCE"://contains ORGANISM
				processSource(elementLines);
				break;
			case "REFERENCE":
				processReference(elementLines);
				break;
			case "COMMENT":
				processComment(elementLines);
				break;
			case "LOCUS"://don't do anything for this entry, to change if they have to be processed
				break;
			case "VERSION"://don't do anything for this entry, to change if they have to be processed
				break;
			case "KEYWORDS"://don't do anything for this entry, to change if they have to be processed
				break;
			default:
				log.log(Level.FINE, "Find another Key in the Locus section, unprocessed: "+components[0]);
				break;
		}
	}
	
	protected void processComment(List<String> elementLines) {
		StringBuilder comment = new StringBuilder();
		elementLines.set(0, elementLines.get(0).substring(11));
		for (String str : elementLines) {
			comment.append(str.trim());
		}
		mapInformation.put("COMMENT", comment.toString());
	}
	
	/**
	 * Extract the REFERENCE as an Object since a genbank record may have many references
	 * @param components one reference at a time
	 */
	protected void processReference(List<String> components) {
		if(!(components.get(0).startsWith("REFERENCE   ") && components.size()>2 && mapInformation.get("ACCESSION")!=null)){
			log.log(Level.SEVERE, "Apparently I have an REFERENCE line with a different format than the one expected ["+components.get(0)+"]: "+this.toString());
		}
		else {
			Reference ref = new Reference(mapInformation.get("ACCESSION"), components);
			
			// Set authors if they haven't set yet
			/*if (authorList.size() == 0 ) {
				authorList = ref.getAuthorList();
			}*/
			// add authors
			if (ref.getAuthorList() != null & ref.getAuthorList().size() != 0 )
				authorList.add(ref.getAuthorList());
			
			// set institution and collection date 
			if (ref.getInstitution() != null ) {
				institutionName = ref.getInstitution();
				submissionDate = ref.getSubmissionDate();
			}
			else // reference of type having Title not equal to "Direct Submission" i.e all publication except "direct submission"
			
			{
				Publication publication = new Publication();
				if( ref.getPubmed() != 0) {
					publication.setPubmedId(ref.getPubmed());
					try {
						publication.setCentralId(mapper.getPMCID(String.valueOf( ref.getPubmed() )));
					}
					catch (Exception e) {
						log.log(Level.SEVERE, "Error pulling PMCID: " + e.getMessage() + " " + ref.getPubmed());
					}
					
				}
				publication.setJournal(ref.getJournal());
				publication.setTitle(ref.getTitle());
				
				extractedPub.add(publication);
				
				
			}
			// set publications
		/*	
			if (ref.getPubmed() != 0) {//at most 1 of the Locus references should have a pubmed id//
				extractedPub = new Publication();
				extractedPub.setPubmedId(ref.getPubmed());
				try {
					extractedPub.sif (authorList.size() == 0 ) {
				authorList = ref.getAuthorList();
			}etCentralId(mapper.getPMCID(String.valueOf(extractedPub.getPubmedId())));
				}
				catch (Exception e) {
					log.log(Level.SEVERE, "Error pulling PMCID: " + e.getMessage());
				}
				if (ref.getAuthors() != null) {
					extractedPub.setAuthors(ref.getAuthors());
				}
				else {
					extractedPub.setAuthors("Unknown");
					log.warning("Pubmed " + extractedPub.getPubmedId() + " does not have Authors listed.");
				}
				extractedPub.setJournal(ref.getJournal());
				extractedPub.setTitle(ref.getTitle());
				//log.info("Found Publication");//
			}*/
		}
	}
	/**
	 * Extract the ORGANISM from the SOURCE element
	 * @param components
	 */
	protected void processSource(List<String> components) {
		if(!(components.get(0).startsWith("SOURCE      ") && components.size()>2)) {
			log.log(Level.SEVERE, "Apparently I have an SOURCE line with a different format than the one expected ["+components.get(0)+"]: "+this.toString());
		}
		else {//first we loop for the source then for the organism 
			StringBuilder source = new StringBuilder();
			source.append(components.get(0).substring(12)); 
			int index = 1;
			while(index<components.size()) {
				if(components.get(index).startsWith("  ORGANISM  "))
					break;
				source.append(" ");
				source.append(components.get(index).trim());
			index++;
			}
			mapInformation.put("SOURCE", source.toString());
			//log.info("Added Source: " + source.toString());
			//we found the Source, we search for the ORGANISM under it
			StringBuilder organism = new StringBuilder();
			if(index>=components.size()) {
				log.log(Level.SEVERE, "Apparently I don't have an ORGANISM section in the SOURCE header: "+this.toString());
			}
			else{
				organism.append(components.get(index).substring(12));
				index++;
				while(index < components.size()) {
					organism.append(" ");
					organism.append(components.get(index).trim());
				index++;
				}
			}
			mapInformation.put("ORGANISM", organism.toString());
			//log.info("Added Organism: " + organism.toString());
		}
	}
	protected void processDefinition(List<String> components) {
		if(!components.get(0).startsWith("DEFINITION  ")) {
			log.log(Level.SEVERE, "Apparently I have an DEFINITION line with a different format than the one expected ["+components.get(0)+"]: "+this.toString());			
		}
		else {
			StringBuilder out = new StringBuilder();
			out.append(components.get(0).substring(11)); 
			int index = 1;
			while(index<components.size()){
				out.append(" ");
				out.append(components.get(index).trim());
			index++;
			}
			mapInformation.put("DEFINITION", out.toString());
			//log.info("Added Definition: " + out.toString());
		}
	}
//A finir verifier que les features soient tous et bien parser, gerer les records avec de multiples source sections quoi en faire, et enfin inserer les section dans la DB	
	protected void processAccession(List<String> accession) {
		if(!accession.get(0).startsWith("ACCESSION   ")) {//I keep them, if we don't want add the separation here.
			log.log(Level.SEVERE, "Apparently I have an accession line with a different format than the one expected ["+accession.size()+"]: "+this.toString());
		}
		else {
			String cleanAccession = accession.get(0).substring(11).trim();
			if (cleanAccession.contains(" ") && cleanAccession.length() > 6) {
				cleanAccession = cleanAccession.split(" ")[0].trim();
			}
			mapInformation.put("ACCESSION", cleanAccession);
		}
	}
	protected void processFeatures(Integer start, Integer end) {
		int index = start+1;
		while(index<=end) {
			List<String> element = new ArrayList<String>();
			element.add(locusLines.get(index));//it's the header
			if(index+1<locusLines.size() && locusLines.get(index+1).startsWith("            ")) {//there is a multiple lines for the subpart, continue until the next one or the exit
				index++;
				while(index<locusLines.size() && locusLines.get(index).startsWith("            ")) {
					element.add(locusLines.get(index));
				index++;
				}					
			}
			else {//only on line we continue to the next one
				index++;	
			}
			processFeature(element);
		}
	}
	/**
	 * Process each feature according to their nature
	 * Note: we can have multiple source for one genbank record...
	 * @param feature
	 */
	protected void processFeature(List<String> featureLines) {
		if(featureLines.get(0).startsWith("     gene            ")) {
			//log.info("Parse gene Feature for accession ["+mapInformation.get("ACCESSION")+"], feature starts: "+featureLines.get(0));
			Gene gene = new Gene();
			gene.parse(featureLines);
			genes.add(gene);
		}
		else {
			//log.info("Parse other Feature for accession ["+mapInformation.get("ACCESSION")+"], feature starts: "+featureLines.get(0));
			ArrayList<Feature> newFeatures = Feature.parse(featureLines);
			for (Feature feature : newFeatures) {
				features.add(feature);
			}
		}
	}
	/**
	 * Process the part ORIGIN which contains the sequence
	 * @param start
	 */
	protected void processOrigin(Integer start){
		StringBuilder sequence = new StringBuilder();
		//start always after: ORIGIN\n
		int index = start+1;
		while(index<locusLines.size()) {
			String subSequence = locusLines.get(index);
			if(!subSequence.matches(" *[0-9]+ ([a-z]| )+")) {
				log.log(Level.SEVERE, "=====> One line in Origin section is incorrectly formatted, continue without: \n"+this.toString());
				sequence = null;
				break;
			}
			else {
				sequence.append(subSequence.substring(10));
			}
		index++;
		}
		if(sequence!=null) {
			mapInformation.put("ORIGIN", sequence.toString().replaceAll(" ", ""));
		}//otherwise there was a problem...
	}
	/**
	 * 
	 * @return Entire GenBankRecord object with all of the valuable data extracted from the record 
	 */
	public GenBankRecord getGBRecord() {
		final String accession = mapInformation.get("ACCESSION"); //want to be sure that they all are linked with the same accession//
		//log.info("Constructing object for " + accession);
		GenBankRecord record = new GenBankRecord();
		record.setAccession(accession);
		try {
			Sequence seq = new Sequence();
			seq.setAccession(accession);
			seq.setSequence(mapInformation.get("ORIGIN"));
			seq.setDefinition(mapInformation.get("DEFINITION"));
			seq.setOrganism(mapInformation.get("ORGANISM"));
			seq.setComment(mapInformation.get("COMMENT"));
			seq.setPub(extractedPub);
			//add all genes//
			for (Gene gen : genes) {
				gen.setAccession(accession);
				record.addGene(gen);
			}
			//process all features//
			for (Feature feat : features) {
				feat.setAccession(accession);
				record.addFeature(feat);
			}
			Host host = new Host();
			host.setAccession(accession);
			//search through features to add additional data to seq and host//
			for (Feature feat : record.getFeatures()) {
				//check source for itv_from and itv_to//
				//only using the 1st source for now, if there are multiple sources in a record//
				if (feat.getHeader().contains("source") && seq.getItv_from() == 0) {
					if (feat.getPosition().contains("order")) {//multiple sources//
						seq.setItv_from(Integer.parseInt(feat.getPosition().substring(feat.getPosition().indexOf("(")+1, feat.getPosition().indexOf(".")).trim()));
						seq.setItv_to(Integer.parseInt(feat.getPosition().substring(feat.getPosition().indexOf("..")+2, feat.getPosition().indexOf(",")).trim()));
						log.warning("Multiple sources for " + seq.getAccession());
					}
					else {//only 1 source//
						seq.setItv_from(Integer.parseInt(feat.getPosition().substring(0, feat.getPosition().indexOf(".")).trim()));
						seq.setItv_to(Integer.parseInt(feat.getPosition().substring(feat.getPosition().indexOf("..")+2).trim()));
					}
				}
				switch (feat.getKey()) {
					case "db_xref" :
						if (feat.getValue().contains("taxon:")) {
							int tax_id = Integer.parseInt(feat.getValue().substring(feat.getValue().indexOf(":")+1));
							seq.setTax_id(tax_id);
						}
						break;
					case "isolate" :
						seq.setIsolate(feat.getValue()); 
						break;
					case "strain" :
						seq.setStrain(feat.getValue());
						break;
					case "collection_date" :
						seq.setCollection_date(feat.getValue());
						break;
					case "host" :
					//case "lab_host" : we don't want to use lab studies//
						host.setName(feat.getValue());
						break;
					case "country" :
						PossibleLocation loc = new PossibleLocation();
						loc.setAccession(accession);
						loc.setLocation(feat.getValue());
						record.setGenBankLocation(loc);
						break;
					default: break;
				}
			}
			if (record.getGenBankLocation() == null) {//added to consolidate sql queries when pulling records//
				PossibleLocation loc = new PossibleLocation();
				loc.setAccession(accession);
				loc.setLocation("Unknown");
				record.setGenBankLocation(loc);
			}
			record.setSequence(seq);
			record.setHost(host);
			record.setInstitute(institutionName);
			record.setAuthorList(authorList);
			record.setSubmissionDate(submissionDate);
			//return completed record//
			//checkRecord(record); //for testing how complete the records are//
		}
		catch (Exception e) {
			//don't want the whole parser to quit if it finds an error in 1 record//
			log.log(Level.SEVERE, "ERROR PARSING RECORD" + record.getAccession());
		}
		return record;
	}
	/**
	 * Takes a freshly parsed GenBankRecord and checks to see if any info is missing
	 * @param Parsed GenBankRecord
	 */
	@SuppressWarnings("unused")
	private void checkRecord(GenBankRecord record) {
		if (record.getAccession() == null) {
			log.log(Level.SEVERE, "NO ACCESSION FOR THIS RECORD??"); 
		}
		Sequence seq = record.getSequence();
		if (seq.getDefinition() == null) {
			log.warning("No DEFINITION for " + seq.getAccession());
		}
		if (seq.getCollection_date() == null) {
			log.warning("No COLLECTION_DATE for " + seq.getAccession());
		}
		if (seq.getIsolate() == null) {
			log.warning("No ISOLATE for " + seq.getAccession());
		}
		if (seq.getOrganism() == null) {
			log.warning("No ORGANISM for " + seq.getAccession());
		}
		if (seq.getSequence() == null) {
			log.warning("No SEQUENCE for " + seq.getAccession());
		}
		if (seq.getStrain() == null) {
			log.warning("No STRAIN for " + seq.getAccession());
		}
		if (seq.getTax_id() == 0) {
			log.warning("No TAXON_ID for " + seq.getAccession());
		}
		if (seq.getItv_from() == 0){
			log.warning("No ITV_FROM for" + seq.getAccession());
		}
		if (seq.getItv_to() == 0){
			log.warning("No ITV_TO for" + seq.getAccession());
		}
		if (seq.getPub() == null) {
			log.warning("No PUBMED for " + seq.getAccession());
		}
		if (record.getHost().getName() == null) {
			log.warning("No HOST_NAME for " + record.getAccession());
		}
		if (record.getHost().getTaxon() == 0) {
			log.warning("No HOST_TAXON for " + record.getAccession());
		}
		//add more checks as needed//
	}
}
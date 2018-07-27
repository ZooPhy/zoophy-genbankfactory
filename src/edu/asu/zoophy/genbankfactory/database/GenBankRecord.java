package edu.asu.zoophy.genbankfactory.database;

import java.util.ArrayList;
import java.util.List;
/**
 * Main class to encapsulate an entire GenBank Record so that it can be easily dumped into the DB
 * and sent to the client web application.
 * @author demetri
 */
public class GenBankRecord {
	
	private String accession;
	private Sequence sequence;
	private List<Gene> genes;
	private List<Feature> features;
	private Host host; 
	private PossibleLocation genbankLocation;
	private PossibleLocation geonameLocation;
	
	private String institute;
	private List<List<Author>> authorList;
	private String submissionDate;
	
	private List<PossibleLocation> possLocations;
	
	public GenBankRecord() {
		genes = new ArrayList<Gene>();
		features = new ArrayList<Feature>();
		possLocations = new ArrayList<PossibleLocation>();
		authorList = new ArrayList<List<Author>>();
	}

	public String getAccession() {
		return accession;
	}

	public void setAccession(String accession) {
		this.accession = accession;
	}

	public Sequence getSequence() {
		return sequence;
	}

	public void setSequence(Sequence sequence) {
		this.sequence = sequence;
	}

	public List<Gene> getGenes() {
		return genes;
	}

	public void setGenes(List<Gene> genes) {
		this.genes = genes;
	}

	public List<Feature> getFeatures() {
		return features;
	}

	public void setFeatures(List<Feature> features) {
		this.features = features;
	}

	public Host getHost() {
		return host;
	}

	public void setHost(Host host) {
		this.host = host;
	}
	
	public void addFeature(Feature feat) {
		features.add(feat);
	}
	
	public void addGene(Gene gen) {
		genes.add(gen);
	}

	public List<PossibleLocation> getPossLocations() {
		return possLocations;
	}

	public void setPossLocations(List<PossibleLocation> possLocations) {
		this.possLocations = possLocations;
	}

	public void addPossLocation(PossibleLocation loc) {
		this.possLocations.add(loc);
	}

	public PossibleLocation getGenBankLocation() {
		return genbankLocation;
	}
	
	public PossibleLocation getGeonameLocation() {
		return geonameLocation;
	}

	public void setGeonameLocation(PossibleLocation geonameLocation) {
		this.geonameLocation = geonameLocation;
	}

	public void setGenBankLocation(PossibleLocation genBankLocation) {
		this.genbankLocation = genBankLocation;
	}
	

	public List<List<Author>> getAuthorList() {
		return authorList;
	}

	public void setAuthorList(List<List<Author>> authorList) {
		this.authorList = authorList;
	}

	public String getSubmissionDate() {
		return submissionDate;
	}

	public void setSubmissionDate(String submissionDate) {
		this.submissionDate = submissionDate;
	}

	public String getInstitute() {
		return institute;
	}

	public void setInstitute(String institute) {
		this.institute = institute;
	}

	/**
	 * @return PossibleLocation with highest probability
	 */
	public PossibleLocation getMostLikelyLocation() {
		PossibleLocation bestLoc = null;
		if (!possLocations.isEmpty()) {
			bestLoc = possLocations.get(0);
			for (PossibleLocation poss : possLocations) {
				if (poss.getProbability() > bestLoc.getProbability()) {
					bestLoc = poss;
				}
			}
		}
		return bestLoc;
	}
	
	/**
	 * 
	 * @param threshold
	 * @return PossibleLocations with probability above the given threshold
	 */
	public List<PossibleLocation> getPossLocations(double threshold) {
		List<PossibleLocation> locs = new ArrayList<PossibleLocation>();
		for (PossibleLocation poss : possLocations) {
			if (poss.getProbability() >= threshold) {
				locs.add(poss);
			}
		}
		return locs;
	}

}
package edu.asu.zoophy.genbankfactory.database;

/**
 * @author demetri
 */
public class Host {
	
	private String accession; //universal link to GenBank record//
	//original content of the field (no normalization)
	private String name;
	//normalized taxon_id for the host. Name of the host can be found from this//
	private int taxon;
	
	public Host() {
		//default//
	}

	//getters and setters//
	public String getAccession() {
		return accession;
	}
	
	public void setAccession(String accession) {
		this.accession = accession;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public int getTaxon() {
		return taxon;
	}

	public void setTaxon(int taxon) {
		this.taxon = taxon;
	}
	/**
	 * @return formal name of the host, found using the normalized taxon id
	 */
	public String getFormalName() {
		String formalName = "";
		//TODO : This will eventually pull the host name from DB, using the normalized taxon_id
		return formalName;
	}
}
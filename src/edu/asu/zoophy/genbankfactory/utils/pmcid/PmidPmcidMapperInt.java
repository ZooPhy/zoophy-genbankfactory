package edu.asu.zoophy.genbankfactory.utils.pmcid;

public interface PmidPmcidMapperInt {
	/**
	 * @param PMID
	 * @return the corresponding PMCID, null if not found
	 */
	public String getPMCID(String PMID);
	/**
	 * @param PMCID
	 * @return the corresponding PMID
	 */
	public String getPMID(String PMCID);
	/**
	 * clears internal map
	 */
	public void free();
}
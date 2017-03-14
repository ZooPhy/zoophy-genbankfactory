package edu.asu.zoophy.genbankfactory.database;
import java.util.List; 
/**
 * @author demetri
 * Interface for all GenBankRecord CRUD operations
 */
public interface GenBankRecordDAOInt {
	/**
	 * Dump the parsed records into the DB
	 * @param parsedRecords List of parsed GenBankRecords
	 */
	public void dumpRecords(List<GenBankRecord> parsedRecords);
	/**
	 * 
	 * @return Full GenBankRecord from the DB
	 */
	public GenBankRecord getRecord(String accession);
	/**
	 * Clear out DB tables before new data dump
	 * @throws Exception 
	 */
	public void clearTables() throws Exception;
	/**
	 * Creates necessary tables that can be immediately used for a data dump
	 * @throws Exception 
	 */
	public void createTables() throws Exception;
	/**
	 * Meant for getting large quantities of records efficiently for Indexing
	 * @param limit Max # of GBR to return
	 * @param offset GBR row to start at (for looping through entire db in batches)
	 * @return List of GBR that contains only necessary info for Indexing
	 */
	public List<GenBankRecord> getIndexableRecords(long limit, long offset) throws Exception;
	
	public PossibleLocation findGeonameLocation(String accession);
	
	public PossibleLocation findGenBankLocation(String accession);
	
	public void insertGenBankLocations(List<PossibleLocation> locs);
	
}
package edu.asu.zoophy.genbankfactory.utils.normalizer.lineage;

import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.asu.zoophy.genbankfactory.database.GenBankRecord;

/**
 * Runs the Influenza B Lineage Normalization
 * @author developerdemetri
 */
public class InfluenzaBLineageNormalizer {

	private Logger log;
	private static final int BATCH_SIZE = 25000;
	private int batchCount;
	
	public InfluenzaBLineageNormalizer() {
		log = Logger.getLogger("InfluenzaBLineageNormalizer");
		batchCount = 0;
	}
	
	/**
	 * Runs the Influenza B Lineage Normalization
	 */
	public void run() {
		try {
			log.info("Starting Influenza B Lineage Normalization...");
			Queue<GenBankRecord> records = loadFluBRecords();
			GenBankRecord rec = records.poll();
			String lineage = null;
			while (rec != null) {
				lineage = InfluenzaBLineageDetector.assignLineage(rec);
				addLineageToBatch(rec.getAccession(), lineage);
				rec = records.poll();
			}
			executeLineageToBatch();
			log.info("Finished Influenza B Lineage Normalization.");
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Failed Influenza B Lineage Normalization: "+e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Loads Influenza B Records
	 * @return Queue of Influenza B GenBankRecords
	 */
	private Queue<GenBankRecord> loadFluBRecords() {
		Queue<GenBankRecord> records = new LinkedList<GenBankRecord>();
		// TOOD load records 
		return records;
	}
	
	/**
	 * Adds Accession,Lineage pair to SQL update batch
	 * @param accession Accession of record to update
	 * @param lineage Assigned Lineage of record
	 */
	private void addLineageToBatch(String accession, String lineage) {
		// TODO add Accession,Lineage pair to SQL update batch	
		batchCount++;
		if (batchCount >= BATCH_SIZE) {
			executeLineageToBatch();
		}
	}
	
	/**
	 * Executes Accession,Lineage SQL update batch
	 */
	private void executeLineageToBatch() {
		if (batchCount > 0) {
			// TODO execute SQL update batch
			batchCount = 0;
		}
	}
	
}

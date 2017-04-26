package edu.asu.zoophy.genbankfactory.utils.normalizer.lineage;

import java.util.List;

import edu.asu.zoophy.genbankfactory.database.Feature;
import edu.asu.zoophy.genbankfactory.database.GenBankRecord;

/**
 * Assigns either the Victoria or Yamagata lineage to Influenza B records
 * @author developerdemetri
 */
public class InfluenzaBLineageDetector {
	
	private static final String VICTORIA_LINEAGE = "Victoria";
	private static final String YAMAGATA_LINEAGE = "Yamagata";

	/**
	 * Assigns either the Victoria or Yamagata lineage to Influenza B records
	 * @param fullRecord Full GenBankRecord containing Features
	 * @return String denoting Influenza B Lineage that is either Victoria, Yamagata, or Unknown
	 */
	public static String assignLineage(GenBankRecord fullRecord) {
		// check Strain for obvious indication of Lineage
		String strain = fullRecord.getSequence().getStrain();
		if (strain.contains(VICTORIA_LINEAGE)) {
			return VICTORIA_LINEAGE;
		}
		else if (strain.contains(YAMAGATA_LINEAGE)) {
			return YAMAGATA_LINEAGE;
		}
		// check Comment for Lineage notes
		String comment = null;
		List<Feature> features = fullRecord.getFeatures();
		for (int i = 0; features != null && i < features.size(); i++) {
			if (features.get(i).getKey().equalsIgnoreCase("comment")) {
				comment = features.get(i).getValue();
				i = features.size();
			}
		}
		if (comment.contains("Lineage:"+VICTORIA_LINEAGE)) {
			return VICTORIA_LINEAGE;
		}
		else if (comment.contains("Lineage:"+YAMAGATA_LINEAGE)) {
			return YAMAGATA_LINEAGE;
		}
		// return Unknown if Lineage is not indicated
		return "Unknown";
	}
		
}

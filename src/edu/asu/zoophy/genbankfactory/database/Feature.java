package edu.asu.zoophy.genbankfactory.database;

import java.util.ArrayList; 
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author demetri
 */

public class Feature {
	
	private long id; //db prim key//
	private String accession; //universal link to GenBank record//
	private String header;
	private String position;
	private String key;
	private String value;
	
	public Feature() {
		//default//
	}

	//getters and setters//
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}
	
	public String getAccession() {
		return accession;
	}

	public void setAccession(String accession) {
		this.accession = accession;
	}
	
	public String getHeader() {
		return header;
	}

	public void setHeader(String header) {
		this.header = header;
	}

	public String getPosition() {
		return position;
	}

	public void setPosition(String position) {
		this.position = position;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public static ArrayList<Feature> parse(List<String> featureLines) {
		final Logger log = Logger.getLogger("Feature");
		try {
			ArrayList<Feature> features = new ArrayList<Feature>();
			featureLines.set(0, featureLines.get(0).trim());
			int endOfHead = featureLines.get(0).indexOf(" ");
			final String head = featureLines.get(0).substring(0, endOfHead);
			final String pos = featureLines.get(0).substring(endOfHead).trim();
			featureLines.remove(0);
			if (featureLines.isEmpty()) {
				//log.warn("Empty Feature");
			}
			else {
				for (int i = 0; i < featureLines.size(); i++) {
					String line = featureLines.get(i);
					Feature feat = new Feature();
					feat.setHeader(head);
					feat.setPosition(pos);
					if (line.contains("/")) { //some rna records have exon, intron line numbers that don't follow standard feature format in the features. not sure if we need to account for all of those (check AB009616 for example)
						if (line.contains("=")) {//found some features that don't have values with the key, such as AB011023 feature /ribosomal_slippage
							feat.setKey(line.substring(line.indexOf("/")+1, line.indexOf("=")));
							if (line.contains("\"")) {
								line = line.substring(line.indexOf("\"")+1);
								StringBuilder featVal = new StringBuilder();
								featVal.append(line);
								while (!line.contains("\"")) {
									i++;
									line = featureLines.get(i).trim();
									featVal.append(line);
								}
								feat.setValue(featVal.substring(0, featVal.toString().indexOf("\"")));
							}
							else {
								feat.setValue(line.substring(line.indexOf("=")+1).trim());
							}
							features.add(feat);
							//log.info("processed Feature " + feat.getKey());
						}
						else {
							//log.warn("feature " + line.trim() + " does not have a value");
						}
					}
					else {
						//log.warn("feature does not have key/value pair");
					}
				}
			}
			return features;
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "ERROR while parsing Features" + e.getMessage());
			return null;
		}
	}
}
package edu.asu.zoophy.gbmetadataupdater.metadataextractor;

import java.util.HashMap;


/**
 * A class representing a GenBank record (including relevant fields only)
 * @author tasnia
 *
 */
public class GBRecord {
	//an enum of all fields we are storing from GB records
	public enum GBRecordFields {
		Accession, PMID, Strain, Isolate, Organism, Country, Date, Host, Gene
	}
	
	GBMetadata gm; 
	
	//maps each record field to its corresponding value
	HashMap<GBRecordFields, String> recordMap = new HashMap<GBRecordFields, String>();
	
	//constructor 1: takes a recordMap as parameter
	public GBRecord (HashMap<GBRecordFields, String> recordMap) {
		this.recordMap = recordMap;
	}
	
	public String getAccession() {
		return recordMap.get(GBRecordFields.Accession);
	}
	
	//constructor 2: takes the individual fields and creates the map
	public GBRecord (String a, String p, String s, String i, String o, String c, String d, String h, String g) {
		recordMap.put(GBRecordFields.Accession, a);
		recordMap.put(GBRecordFields.PMID, p);
		recordMap.put(GBRecordFields.Strain, s);
		recordMap.put(GBRecordFields.Isolate, i);
		recordMap.put(GBRecordFields.Organism, o);
		recordMap.put(GBRecordFields.Country, c);
		recordMap.put(GBRecordFields.Date, d);
		recordMap.put(GBRecordFields.Host, h);
		recordMap.put(GBRecordFields.Gene, g);
	}
	
	//returns a string representation of the record
	public String toString() {
		String curString= recordMap.get(GBRecordFields.Accession)+"\t"+recordMap.get(GBRecordFields.PMID)+"\t"+recordMap.get(GBRecordFields.Strain)
		+"\t"+recordMap.get(GBRecordFields.Isolate)+"\t"+recordMap.get(GBRecordFields.Organism)+"\t"+recordMap.get(GBRecordFields.Country)
		+"\t"+recordMap.get(GBRecordFields.Host)+"\t"+recordMap.get(GBRecordFields.Gene)+"\t"+recordMap.get(GBRecordFields.Date)+"\n";
		return curString;
	}
}
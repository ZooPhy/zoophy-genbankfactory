package edu.asu.zoophy.genbankfactory.database;

import java.util.List;

import org.apache.log4j.Logger;

/**
 * @author demetri
 */
public class Gene {
	
	private long id; //db prim key//
	private String accession; //universal link to GenBank record//
	private String name;
	private String itv;
	
	public Gene() {
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

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getItv() {
		return itv;
	}

	public void setItv(String itv) {
		this.itv = itv;
	}

	public void parse(List<String> featureLines) {
		final Logger log = Logger.getLogger("Gene");
		try {
			itv = featureLines.get(0).substring(21);
			name = "";
			//some genes have multiline names//
			for (int i = 1; i < featureLines.size(); i++) {
				name += featureLines.get(i);
			}
			name = name.substring(name.indexOf("\"")+1, name.lastIndexOf("\""));
			//log.info("processed Gene: " + name);
		}
		catch (Exception e) {
			log.fatal( "ERROR parsing Gene" + e.getMessage());
		}
	}
	
	public String formatName(String name) {
		if (name.contains("/note")) {
			name = name.substring(0,name.indexOf("/note")).trim();
		}
		if (name.contains("/locus")) {
			name = name.substring(0,name.indexOf("/locus")).trim();
		}
		return name;
	}
}
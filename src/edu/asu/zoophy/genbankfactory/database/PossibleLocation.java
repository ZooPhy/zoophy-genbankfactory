package edu.asu.zoophy.genbankfactory.database;

/**
 * All possible locations for given location name found in GenBank record
 * @author demetri
 */
public class PossibleLocation {
	//db generated//
	private long id;
	//foreign key for the GenBank record//
	private String accession;
	private String location;
	private double latitude;
	private double longitude;
	//likelyhood of this being the correct location//
	private double probability;
	// GeoName location type ex. ADM1, ADM2, etc.
	private String type;
	private String country;
	
	public PossibleLocation() {
		//generic//
	}

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

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public double getProbability() {
		return probability;
	}

	public void setProbability(double probability) {
		this.probability = probability;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}
}
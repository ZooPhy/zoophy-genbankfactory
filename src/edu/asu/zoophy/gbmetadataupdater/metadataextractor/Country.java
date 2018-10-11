package edu.asu.zoophy.gbmetadataupdater.metadataextractor;


/**
 * A class representing a country
 * @author tasnia
 *
 */
public class Country extends LocationPart {
	boolean isCountry=true;
	
	public Country(String country, String ccode, Double lat, Double lng) {
		super("", country, ccode, "", "", "", lat, lng, -1, "");
	}
	
	public Country(String id, String country, String ccode, String fcode, Double lat, Double lng, long population) {
		super(id, country, ccode, fcode, "", "", lat, lng, population, "A");
	}
		
	@Override
	public String toString() {
		return ccode.toString();
	}

	@Override
	public boolean equals(Object loc) {
		if(!(loc instanceof LocationPart)) {
			return false;
		}
		LocationPart l = (LocationPart) loc;
		return l.isCountry==true && l.ccode.equals(this.ccode);
	}

	@Override
	public int hashCode() {
		return ( (isCountry) ? 31 : 19+ 3*ccode.hashCode());
	}
	
	@Override
	public boolean isConsistentWith(LocationPart l) {
		return l.ccode.equals(this.ccode);
	}
	
}

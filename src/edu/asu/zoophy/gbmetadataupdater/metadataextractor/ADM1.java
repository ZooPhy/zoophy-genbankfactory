package edu.asu.zoophy.gbmetadataupdater.metadataextractor;


/**
 * A class representing an ADM1-level locatioon
 * @author tasnia
 *
 */
public class ADM1 extends LocationPart {
	boolean isADM1=true;

	public ADM1(String id, String name, String ccode, String adm1code, Double lat, Double lng, int population) {
		super(id, name, ccode, "ADM1", adm1code, "", lat, lng, population, "");
	}
	
	
	@Override
	public boolean equals(Object loc) {
		if(!(loc instanceof LocationPart)) {
			return false;
		}
		LocationPart l = (LocationPart) loc;
		return l.isADM1==true && l.ccode.equals(this.ccode) && l.adm1code.equals(this.adm1code);
	}
	
	@Override
	public int hashCode() {
		return ( (isADM1) ? 31 : 17+ 3*ccode.hashCode()+19*adm1code.hashCode());
	}
	
	@Override
	public boolean isConsistentWith(LocationPart l) {
		if(l.isCountry==true) {
			return this.ccode.equals(l.ccode);
		}
		return l.ccode.equals(this.ccode) && l.adm1code.equals(this.adm1code);
	}
}

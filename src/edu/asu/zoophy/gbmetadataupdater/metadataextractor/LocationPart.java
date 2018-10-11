package edu.asu.zoophy.gbmetadataupdater.metadataextractor;


/**
 * A class for representing the different location parts of GenBank metadata
 * @author tasnia
 *
 */
public class LocationPart {
	String id="";
	String name="";
	Double lat=-1.00;
	Double lng=-1.00;
	String ccode="";
	String adm1code="";
	String adm2code="";
	String fcode="";
	String fclass="";
	long population=-1; 
	
	boolean isCountry = false;
	boolean isADM1 = false;
	
	public enum Fcode {
		OTHERF, PPL, PPLA4, PPLA3, PPLA2, PPLA, PPLC, 
		ADMD, ADM5, ADM4, ADM3, ADM2, ADM1, 
		PCLS,  PCLIX, PCLI, PCLF,
		PCLD, PCL, CONT;
	}
	public LocationPart() {
		
	}
	
	public LocationPart(String id, String name, String ccode, String fcode, String adm1code, String adm2code, Double lat, Double lng, long population, String fclass) {
		this.id=id;
		this.name=name;
		this.ccode=ccode;
		this.fcode=fcode;
		this.adm1code=adm1code;
		this.adm2code=adm2code;
		this.lat=lat;
		this.lng=lng;
		this.population=population;
		this.fclass=fclass;
	}
	 
	public String toString() {
		return name.toString();
	}
	
	public String getName() {
		return this.name;
	}
	public String getCcode() {
		return ccode;
	}
	
	public Fcode getCodedFcode() {
		for(Fcode f: Fcode.values()) {
			if(f.toString().toUpperCase().equals(fcode.toUpperCase())) {
				return f;
			}
		}
		return Fcode.OTHERF;
	}
	public String getFcode() {
		return fcode;
	}
	
	public String getID() {
		return id;
	}
	
	public Double getLatitude() {
		return lat;
	}
	
	public String getADM1code() {
		return adm1code;
	}
	
	public Double getLongitude() {
		return lng;
	}
	
	public Long getPopulation() {
		return (Long)  population;
	}
	@Override
	public boolean equals(Object loc) {
		if(!(loc instanceof LocationPart)) {
			return false;
		}
		LocationPart l = (LocationPart) loc;
		return this.lat==l.lat && this.lng==l.lng && this.population==l.population && this.ccode.equals(l.ccode) && this.fcode.equals(l.fcode)
				&&this.adm1code.equals(l.adm1code);
	}

	@Override
	public int hashCode() {
		int popHash =0;
		if(population < Integer.MAX_VALUE) {
			popHash = (int) ((int) 0.13*population);
		} else {
			popHash = (int) ((int) 0.13*population/Integer.MAX_VALUE);
		}
		
		return (3*lat.intValue()+19*lng.intValue()+popHash+fcode.hashCode()+ccode.hashCode()+adm1code.hashCode());
	}
	
	public boolean isConsistentWith(LocationPart l) {
		if(l.isCountry==true) {
			return this.ccode.equals(l.ccode);
		} else if(l.isADM1==true) {
			return this.ccode.equals(l.ccode) && this.adm1code.equals(l.adm1code);
		} else if(l.fcode.equals("ADM2") || this.fcode.equals("ADM2")) {
			return this.ccode.equals(l.ccode) && this.adm1code.equals(l.adm1code) && this.adm2code.equals(l.adm2code);
		} else {
			return false;
		}
	}
}

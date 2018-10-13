package edu.asu.zoophy.gbmetadataupdater.metadataextractor;


/**
 * A class representing geospatial metadata extracted from GenBank
 * @author tasnia
 *
 */
public class GBMetadata {
	LocationPart country;
	LocationPart adm1;
	LocationPart specific1;
	LocationPart specific2;
	String countryName="";
	boolean isSufficientInCountry=false;
	String recSource="";
	String allLocations="";
	String allSources="";
	Double lat=0.0;
	Double lng=0.0;
	String geonameID="-1";
	String fcodeMostSpecific = "";
	public GBMetadata() {
		isSufficientInCountry=false;
	}
	
	
	public enum MetadataType {
		TWO_OTHER_AND_ADM1_AND_COUNTRY, OTHER_AND_ADM1_AND_COUNTRY, ADM1_AND_COUNTRY, OTHER_AND_COUNTRY, COUNTRY, OTHER_AND_ADM1, ADM1, OTHER, BLANK
	}

	public Double getLat() {
		return this.lat;
	}
	public Double getLng() {
		return this.lng;
	}
	
	public String getCcode() {
		if(country!=null) {
			return country.ccode;
		}
		return null;
	}
	public String[] getAllSources() {
		return allSources.split(";");
	}
	public String getMostSpecificFcode() {
		return this.fcodeMostSpecific;
	}
	
	public String getCountryName() {
		return this.countryName;
	}
	
	public String getADM1Code() {
		if(adm1!=null) {
			return adm1.adm1code;
		}
		return null;
	}
	
	public String getID() {
		return geonameID;
	}
	
	public LocationPart getSpecific() {
		return specific1;
	}
	
	public LocationPart getADM1() {
		return adm1;
	}
	
	public LocationPart getCountry() {
		return country;
	}
	
	public void setID(String id) {
		this.geonameID=id;
	}
	public void setADM1(LocationPart adm1) {
		this.adm1 = adm1;
	}
	
	public void setCountry(LocationPart country) {
		this.country = country;
	}
	
	public void setCountryName(String country) {
		this.countryName = country;
	}
	
	public void setspecific(LocationPart specific) {
		this.specific1 = specific;
	}
	
	public void setFcodeSpecific(String fcode) {
		this.fcodeMostSpecific = fcode;
	}
	public void setLat(Double lat) {
		this.lat = lat;
	}
	
	public void setLng(Double lng) {
		this.lng = lng;
	}
	
	public String toString() {
		if(country!=null && adm1!=null && specific1!=null && specific2!=null) {
			return specific2.toString()+","+specific1.toString() + ","+adm1.toString()+","+country.toString();
		} else if(country!=null && adm1!=null && specific1!=null) {
			return specific1.toString() + ","+adm1.toString()+","+country.toString();
		} else if(country!=null && adm1!=null) {
			return adm1.toString()+","+country.toString();
		} else if(adm1!=null && specific1!=null) {
			return specific1.toString() + ","+adm1.toString();
		} else if(country!=null && specific1!=null) {
			return specific1.toString()+","+country.toString();
		}else if(adm1!=null) {
			return adm1.toString();
		} else if(country!=null) {
			return country.toString();
		} else if(specific1!=null) {
			return specific1.toString();
		} 
		return "Unknown";
	}
	
	public  String printAll() {
		String cur="";
		if(isSufficient()) {
			cur = "Yes\t";
		} else {
			cur = "No\t";
		}
		if(isSufficientInCountry) {
			cur+="Yes\t";
		} else {
			cur+="No\t";
		}
		cur = cur+allLocations+"\t"+allSources+"\t"+toString();
		return cur;
		
	}
	public MetadataType getType() {
		if(country!=null && adm1!=null && specific1!=null && specific2!=null) {
			return MetadataType.TWO_OTHER_AND_ADM1_AND_COUNTRY;
		} else if(country!=null && adm1!=null && specific1!=null) {
			return MetadataType.OTHER_AND_ADM1_AND_COUNTRY;
		} else if(country!=null && adm1!=null) {
			return MetadataType.ADM1_AND_COUNTRY;
		} else if(adm1!=null && specific1!=null) {
			return MetadataType.OTHER_AND_ADM1;
		} else if(country!=null && specific1!=null) {
			return MetadataType.OTHER_AND_COUNTRY;
		}else if(adm1!=null) {
			return MetadataType.ADM1;
		} else if(country!=null) {
			return MetadataType.COUNTRY;
		} else if(specific1!=null) {
			return MetadataType.OTHER;
		} 
		return MetadataType.BLANK;
	}

	public boolean isSufficient() {
		if (getType()==MetadataType.TWO_OTHER_AND_ADM1_AND_COUNTRY || getType()==MetadataType.OTHER_AND_ADM1_AND_COUNTRY) {
			return true;
		}
		return false; 
	}
	
}



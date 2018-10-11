package edu.asu.zoophy.gbmetadataupdater.evaluator;

import edu.asu.zoophy.gbmetadataupdater.geonamelucene.LuceneSearcher;
import edu.asu.zoophy.gbmetadataupdater.metadataextractor.LocationPart;

public class Evaluator {
	
	public class Annotation implements Comparable<Annotation>{
		String geonameID;
		String location;
		Double latitude;
		Double longitude;
		String ccode="";
		
		public Annotation(String geonameID, String location, Double latitude, Double longitude, LuceneSearcher searcher) {
			this.geonameID=geonameID;
			this.location=location.trim().toLowerCase().replaceAll(" ","");
			this.latitude=latitude;
			this.longitude=longitude;
			String[] splits = location.split(",");
			String possibleCountry = splits[splits.length-1];
			LocationPart country = searcher.searchCcode(possibleCountry);
			if(country==null) {
				country = searcher.searchCountry(possibleCountry);
			}
			if(country==null) {
				country = searcher.searchCcodeGeoname(possibleCountry);
			}
			if(country==null) {
				country = searcher.searchCountryGeoname(possibleCountry);
			}
			if(country!=null) {
				ccode=country.getCcode();
			}
		}

		@Override
		public int compareTo(Annotation other) {
			if(this.geonameID.equals(other.geonameID)) {
				return 0;
			} else if((this.geonameID.trim().equals("-1")||other.geonameID.equals("-1"))&&this.ccode.equals(other.ccode)) {
				if(this.location.contains(",")){
					return 1;
				} else if(other.location.contains(",")){
					return -1;
				} else {
					return 0;
				}
			} else if(this.geonameID.trim().equals("0")||other.geonameID.equals("0")||this.location.equals("unknown")||other.location.equals("unknown")) {
				if((this.location.length()==0||this.location.equals("unknown"))&& (other.location.length()==0||other.location.equals("unknown"))){
					return 0;
				} else if(this.location.length()==0||this.location.equals("unknown")){
					return -1;
				} else if(other.location.length()==0||other.location.equals("unknown")) {
					return -1;
				}
			}
			return -1;
		}
	}
}

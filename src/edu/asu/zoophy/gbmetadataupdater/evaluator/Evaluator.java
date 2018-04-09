package edu.asu.zoophy.gbmetadataupdater.evaluator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

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

	public void evaluate(String annot1, String annot2) throws FileNotFoundException {
		LuceneSearcher searcher = new LuceneSearcher();
		
	}
	
	private HashMap<String, Annotation> createAnnotMap (String annotfile, LuceneSearcher searcher) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(annotfile));
		String line = reader.readLine();
		HashMap<String, Annotation> annotMap =null;
		while(line!=null) {
			String[] fields = line.split("\t");
			if(!fields[0].trim().toLowerCase().equals("accession")&&fields.length==5) {
				if(annotMap == null){
					annotMap = new HashMap<String, Annotation>();
				}
				Double latitude = 0.0;
				if(fields[3].trim().length()>0) {
					latitude = Double.valueOf(fields[3].trim());
				}
				Double longitude =0.0;
				if(fields[4].trim().length()>0) {
					longitude = Double.valueOf(fields[4].trim());
				}
				Annotation a = new Annotation(fields[1], fields[2], latitude, longitude, searcher);
				annotMap.put(fields[0], a);
			}
			line=reader.readLine();
		}
		return annotMap;
	}
}

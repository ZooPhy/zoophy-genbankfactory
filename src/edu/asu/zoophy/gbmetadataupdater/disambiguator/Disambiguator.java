package edu.asu.zoophy.gbmetadataupdater.disambiguator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.asu.zoophy.gbmetadataupdater.geonamelucene.LuceneSearcher;
import edu.asu.zoophy.gbmetadataupdater.metadataextractor.GBMetadata;
import edu.asu.zoophy.gbmetadataupdater.metadataextractor.LocationPart;
import edu.asu.zoophy.gbmetadataupdater.metadataextractor.LocationPartComparator;
public class Disambiguator {
	
	public void disambiguate(GBMetadata gbm, LuceneSearcher searcher) {
		List<LocationPart> candidates =null;
		LocationPart selected=null;
		String ccode = "";
		LocationPart c = null;
		switch(gbm.getType()){
		case COUNTRY:
			gbm.setLat(gbm.getCountry().getLatitude());
			gbm.setLng(gbm.getCountry().getLongitude());
			ccode = gbm.getCcode();
			c = searcher.searchCcodeGeoname(ccode);
			if(c!=null) {
				gbm.setCountry(c);
				gbm.setFcodeSpecific(c.getFcode());
				gbm.setCountryName(c.getName());
				gbm.setID(c.getID());
			} 
			break;
		case ADM1:
			candidates = new ArrayList<LocationPart>(searcher.searchAdm1(gbm.getADM1().toString()));
			Collections.sort(candidates, new LocationPartComparator());
			selected=candidates.get(0);
			gbm.setADM1(selected);
			gbm.setLat(selected.getLatitude());
			gbm.setLng(selected.getLongitude());
			gbm.setID(selected.getID());
			gbm.setFcodeSpecific(selected.getFcode());
			break;
		case OTHER:
			candidates = new ArrayList<LocationPart>(searcher.searchLocation(gbm.getSpecific().toString()));
			Collections.sort(candidates, new LocationPartComparator());
			selected=candidates.get(0);
			gbm.setspecific(selected);
			gbm.setLat(selected.getLatitude());
			gbm.setLng(selected.getLongitude());
			gbm.setID(selected.getID());
			gbm.setFcodeSpecific(selected.getFcode());
			break;
		case ADM1_AND_COUNTRY:
			selected = searcher.searchAdm1GivenCcode(gbm.getADM1().toString(), gbm.getCcode());
			gbm.setADM1(selected);
			gbm.setLat(selected.getLatitude());
			gbm.setLng(selected.getLongitude());
			gbm.setID(selected.getID());
			gbm.setFcodeSpecific(selected.getFcode());
			ccode = gbm.getCcode();
			c = searcher.searchCcodeGeoname(ccode);
			if(c!=null) {
				gbm.setCountry(c);
				gbm.setCountryName(c.getName());
			} 
			break;
		case OTHER_AND_ADM1_AND_COUNTRY:
			candidates =  new ArrayList<LocationPart>(searcher.searchLocationGivenCcodeAndAdm1code(gbm.getSpecific().toString(), gbm.getCcode(), gbm.getADM1Code()));
			Collections.sort(candidates, new LocationPartComparator());
			selected = candidates.get(0);
			gbm.setspecific(selected);
			gbm.setLat(selected.getLatitude());
			gbm.setLng(selected.getLongitude());
			gbm.setID(selected.getID());
			gbm.setFcodeSpecific(selected.getFcode());
			ccode = gbm.getCcode();
			c = searcher.searchCcodeGeoname(ccode);
			if(c!=null) {
				gbm.setCountry(c);
				gbm.setCountryName(c.getName());
			} 
			break;
		case OTHER_AND_ADM1:
			candidates =  new ArrayList<LocationPart>(searcher.searchLocationGivenADM1code(gbm.getSpecific().toString(), gbm.getADM1Code()));
			Collections.sort(candidates, new LocationPartComparator());
			selected = candidates.get(0);
			gbm.setspecific(selected);
			gbm.setLat(selected.getLatitude());
			gbm.setLng(selected.getLongitude());
			gbm.setID(selected.getID());
			break;
		case OTHER_AND_COUNTRY:
			candidates =  new ArrayList<LocationPart>(searcher.searchLocationGivenCcode(gbm.getSpecific().toString(), gbm.getCcode()));
			Collections.sort(candidates, new LocationPartComparator());
			selected = candidates.get(0);
			gbm.setspecific(selected);
			gbm.setLat(selected.getLatitude());
			gbm.setLng(selected.getLongitude());
			gbm.setID(selected.getID());
			gbm.setFcodeSpecific(selected.getFcode());
			ccode = gbm.getCcode();
			c = searcher.searchCcodeGeoname(ccode);
			if(c!=null) {
				gbm.setCountry(c);
				gbm.setCountryName(c.getName());
			} 
			break;
		default:
			
			
		}
			
	}
	
}
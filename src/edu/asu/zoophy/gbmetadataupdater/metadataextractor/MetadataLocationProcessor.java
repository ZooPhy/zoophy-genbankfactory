package edu.asu.zoophy.gbmetadataupdater.metadataextractor;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import edu.asu.zoophy.gblocationupdater.spellchecker.SpellChecker;
import edu.asu.zoophy.gbmetadataupdater.geonamelucene.LuceneSearcher;
import edu.asu.zoophy.gbmetadataupdater.metadataextractor.GBMetadata.MetadataType;

import java.util.Set;
import java.util.Comparator;
import java.util.Collections;


/**
 * A class for processing GenBank geospatial metadata
 * @author tasnia
 *
 */
public class MetadataLocationProcessor {

	//geospatial metadata currently extracted from the record
	GBMetadata curMetadata = new GBMetadata();

	//List of codes we consider insufficient
	String[] insuffFcodes = new String[]{"ADM1", "ADM1H", "PCL", "PCLD", "PCLF", "PCLH", "PCLI", "PCLS"};

	//Extra geospatial information that will be removed from the place mentioned in the record before re-checking in  geonames
	String[] checks = new String[] { "south", "north", "east", "west", "eastern", "western", "southern", "northern", "south-east", "north-east", "south-west", "north-west",
			" state", " province", " county", " village", " region", " governorate", " district" };

	//maps us state abbreviations to their full forms
	HashMap<String, String> stateMap = new HashMap<String, String>();

	//different possible geoname entries detected in the record fields analyzed
	Set<LocationPart> locations = new HashSet<LocationPart>();

	//different possible adm1-level geoname entries detected in the record fields analyzed
	Set<LocationPart> adm1s = new HashSet<LocationPart>();

	//list of countries detected in metadata
	HashSet<LocationPart> countries = new HashSet<LocationPart>();

	//HashMap with the name of the place as the key and a hashSet containing names of all fields where the place was found
	HashMap<String, HashSet<String>>  sourceMap = new HashMap<String, HashSet<String>>();

	//for searching the lucene index of locations
	LuceneSearcher searcher;
	SpellChecker sp;

	/**
	 * Constructor creates new instance of the searcher and populates us state map from file
	 * @throws Exception
	 */
	public MetadataLocationProcessor() throws Exception {
		populateStateMap(System.getProperty("user.dir")+"/Resources/Us state.txt");
		searcher = new LuceneSearcher();
		sp = new SpellChecker(searcher);
	}
	
	/**
	 * @param searcher The searcher for the Lucene index of geospatial locations
	 * @throws Exception
	 */
	public MetadataLocationProcessor(LuceneSearcher searcher) throws Exception {
		populateStateMap(System.getProperty("user.dir")+"/Resources/Us state.txt");
		this.searcher = searcher;
		sp = new SpellChecker(searcher);
	}
	
	/**
	 * Method to close the searcher if needed
	 */
	public void closeSearcher() {
		searcher.close();
	}

	
	/**
	 * Clears all data
	 */
	public void clear() {
		sourceMap.clear();
		countries.clear();
		adm1s.clear();
		locations.clear();
		curMetadata = new GBMetadata();
		
	}
	
	/**
	 * 
	 * Method to store a mention detected in GenBank metadata if it is found to be a country name
	 * @param mention
	 * @throws Exception
	 */
	public int parseAsCountry(String mention) throws Exception {
		if(mention.toLowerCase().equals("unknown")) {
			return 0;
		}
		LocationPart country = searcher.searchCountry(mention);
		if(country==null) {
			country = searcher.searchCountryGeoname(mention);
		}
		if(country!=null) {
			countries.add(country);
			return 1;
		}
		return 0;
	}	

	public String spellCheck(String mention) {
		return sp.checkSpell(mention);
	}

	/**
	 * Method to store a mention detected in GenBank metadata if it is found to be a location name
	 * @param mention String to be checked
	 * @return The string to be added to source map (original string mention may be edited to find a hit in geonames, need to store the modified string for which we found a hit for future reference)
	 */
	public String checkMention(String mention) {
		if(mention.toLowerCase().equals("unknown")) {
			return "";
		}
		//first see if the mention can be the abbreviation of an US state
		if(stateMap.get(mention.toUpperCase())!=null) {
			mention = stateMap.get(mention.toUpperCase());
			Set<LocationPart> curadm1s = searcher.searchAdm1(mention);
			if(curadm1s.size()>0){
				adm1s.addAll(curadm1s);
				return mention.toLowerCase();
			}
		}
		//aside from us state no other location should be of length 2 - this is a heuristic used to reduce FPs
		if(mention.length()<=2) {
			return "";
		}
		//the string for which we found a hit in GeoNames
		String added = "";
		Set<LocationPart> curLocations = searcher.searchLocation(mention);
		if(curLocations!=null) {
			locations.addAll(curLocations);
			added=mention.toLowerCase();
		} else {
			ArrayList<String> replacers = hasCheck(mention);
			//edit the string by removing  add ons such as "county" and check if Geonames contains the edited string
			for(String r: replacers) {
				String replaced = mention.replaceAll(r, "");
				replaced = replaced.replaceAll("\\s+$", "");
				replaced = replaced.replaceAll("^\\s+", "");
				if(replaced.length()>0) {
					curLocations = searcher.searchLocation(replaced);
					if(curLocations!=null) {
						locations.addAll(curLocations);
						added=replaced.toLowerCase();
					}
					Set<LocationPart> curadm1s = searcher.searchAdm1(replaced);
					if(curadm1s!=null){
						adm1s.addAll(curadm1s);
						added = replaced.toLowerCase();
					}
				}
			}
		}
		//search for adm1s separately to facilitate future processing
		Set<LocationPart> curadm1s = searcher.searchAdm1(mention);
		if(curadm1s!=null){
			adm1s.addAll(curadm1s);
			added = mention.toLowerCase();
		}
		return added;
	}
	
	
	/**
	 * Integrates all locations found so far in the GB records to produce the most specific and most comprehensive location possible
	 * @return The most specific integrated GenBank geospatial metadata currently available for the record based on all fields analyzed so far
	 */
	public GBMetadata integrateLocations() {
		Comparator<GBMetadata> comp = new MetadataComparator();
		List<GBMetadata> curGMList = new ArrayList<GBMetadata>();
		boolean foundConsistentADM1 = false;
		//for every country
		for(LocationPart c: countries) {
			//for every adm1
			for(LocationPart a: adm1s) {
				//if adm1 is consistent with country
				if(c.isConsistentWith(a)&&!c.name.equals(a.name)) {
					//create new gbmetadata and look for more specific, consistent location -- if found just return it - search complete
					//otherwise add the new gbmetadata to the list and keep searching
					GBMetadata gm = new GBMetadata();
					gm.adm1=a;
					gm.country=c;
					for(LocationPart l: locations) {
						if(!isInsufficientFcode(l.fcode) && c.isConsistentWith(l)&& a.isConsistentWith(l)&&!l.name.equals(a.name)&!(l.name.equals(c.name))) {
							gm.specific1=l;
							return gm;
						}
					}
					foundConsistentADM1=true;
					curGMList.add(gm);
				} 
				//only look for (specific, adm1) pair if no (adm1, country) pair has yet been found since only the latter will be returned anyways
				if(!foundConsistentADM1) {
					for(LocationPart l: locations) {
						if(!isInsufficientFcode(l.fcode) && a.isConsistentWith(l) && !l.name.equals(a.name)) {
							GBMetadata gm = new GBMetadata();
							gm.adm1=a;
							gm.specific1=l;
							curGMList.add(gm);
						}
					}
				}
			}
			//look for (specific, country) pair
			for(LocationPart l: locations) {
				if(!isInsufficientFcode(l.fcode) && c.isConsistentWith(l)&&!l.name.equals(c.name)) {
					GBMetadata gm = new GBMetadata();
					gm.country=c;
					gm.specific1=l;
					curGMList.add(gm);
				}
			}
		}
		//check for (specific, adm1) pair only if no other metadata pairs have been added since it will be discarded anyways otherwise
		if(curGMList.size()==0) {

			for(LocationPart a: adm1s) {
				for(LocationPart l: locations) {
					if(!isInsufficientFcode(l.fcode)&&a.isConsistentWith(l)&&!a.name.equals(a.name)) {
						GBMetadata gm = new GBMetadata();
						gm.adm1=a;
						gm.specific1=l;
						curGMList.add(gm);
					}
				}
			}	
		}
		if(curGMList.size()>0) {
			GBMetadata gm = new GBMetadata();
			//list is sorted based on the specificity of metadata. see MetadataComparator class
			if(countries.size()>0) {
				gm.country=countries.iterator().next();
				for(LocationPart c: countries) {
					if(sourceMap.get(c.name).contains("country")) {
						gm.country=c;
					}
				}
			}
			if(!gm.country.ccode.equals("US")) {
				curGMList.add(gm);
			}
			Collections.sort(curGMList, comp);
			return curGMList.get(0);
		} else { 
			//if no consistent pairs of location is found, first check for available countries, then for available adm1s and then for non-country, non-adm1 locations
			//if multiple countries/adm1s/other locations found, select the one extracted from country field if possible, otherwise select the first entry
			//locations which represent administrative divisions are also prioritized over others 
			GBMetadata gm = new GBMetadata();
			if(countries.size()>0) {
				gm.country=countries.iterator().next();
				for(LocationPart c: countries) {
					if(sourceMap.get(c.name).contains("country")) {
						gm.country=c;
					}
				}
			} else if (adm1s.size()>0) {
				gm.adm1=adm1s.iterator().next();
				for(LocationPart a: adm1s) {
					if(sourceMap.get(a.name).contains("country")) {
						gm.adm1=a;
					}
				}
			} else if(locations.size()>0) {
				boolean addedBest=false;
				boolean added=false;
				for(LocationPart l: locations) {
					if(!l.name.equals("unknown")&&l.fclass.toUpperCase().equals("A")&&sourceMap.get(l.name).contains("country")) {
						gm.specific1=l;
						addedBest=true;
						added=true;
					} else if(!addedBest && !l.name.equals("unknown")&&(sourceMap.get(l.name).contains("country")||l.fclass.toUpperCase().equals("A"))) {
						gm.specific1=l;
						added=true;
					} 
				}
				if(!added) {
					for(LocationPart l: locations) {
						if(!l.name.equals("unknown")) {
							gm.specific1=l;
							added=true;
						} 
					}
				}
				if(!added) {
					gm.specific1=locations.iterator().next();
				}
			} 
			return gm;
		}
	}

	
	
	/**
	 * Completes metadata if possible, eg if current metadata contains ADM1, check if the ADM1 locations can only exist in a single country, if so we know the country name as well
	 * @param metadata the metadata to be completed
	 */
	public void completeMetadataIfPossible(GBMetadata metadata) {
		if(metadata.getType()==MetadataType.ADM1) {
			Set<LocationPart> adm1Set = searcher.searchAdm1(metadata.adm1.name);
			if(adm1Set!=null&&adm1Set.size()==1) {
			//	String countryName = searcher.mapCcode(adm1Set.iterator().next().ccode, true);
				LocationPart country = searcher.searchCcode(adm1Set.iterator().next().ccode);
				if(country==null) {
					country = searcher.searchCcodeGeoname(adm1Set.iterator().next().ccode);
				}
			//	LocationPart country= searcher.searchCountry(countryName);
				metadata.country=country;
			}
		} else if(metadata.getType()==MetadataType.OTHER) {
			Set<LocationPart> locSet = searcher.searchLocation(metadata.specific1.name);
			if(locSet!=null && locSet.size()==1) {
				LocationPart country= searcher.searchCcode(locSet.iterator().next().ccode);
				if(country==null) {
					country = searcher.searchCcodeGeoname(locSet.iterator().next().ccode);
				}
				metadata.country=country;
			}
		} else if(metadata.getType()==MetadataType.OTHER_AND_COUNTRY) {
			Set<LocationPart> locSet = searcher.searchLocationGivenCcode(metadata.specific1.name, metadata.country.ccode);
			if(locSet!=null&&locSet.size()==1) {
				LocationPart adm1 = searcher.searchADM1CodeGivenCcode(locSet.iterator().next().adm1code, metadata.country.ccode);
				metadata.adm1=adm1;
			}
		} else if(metadata.getType()==MetadataType.OTHER_AND_ADM1)  {
			Set<LocationPart> locSet = searcher.searchLocationGivenADM1code(metadata.specific1.name, metadata.adm1.adm1code);
			if(locSet!=null&&locSet.size()==1) {
				LocationPart country = searcher.searchCcode(locSet.iterator().next().ccode);
				if(country==null) {
					country = searcher.searchCcodeGeoname(locSet.iterator().next().ccode);
				}
				metadata.country=country;
			}
		}
	}
	
	/**
	 * Updates sourceMap 
	 * @param place The name of the place 
	 * @param source The name of the source where place was found
	 */
	public void updateSourceMap(String place, String source) {
		HashSet<String> sources = sourceMap.get(place);
		if(sources!=null) {
			sources.add(source);
			sourceMap.put(place, sources);
		} else {
			sources = new HashSet<String>();
			sources.add(source);
			sourceMap.put(place, sources);
		}
	}
	
	/**
	 * Method to retrieve the list of locations and their corresponding source for the given record
	 * @return an array of string with first entry containing the list of locations and second entry containing its corresponding source
	 */
	public String[] getAllLocationAndSources() {
		String places="";
		String sources="";
		if(sourceMap.size()==0) {
			String[] placeSources = {"null", "null"};
			return placeSources;
		}
		for (Entry<String, HashSet<String>> entry : sourceMap.entrySet()) {
			String key = entry.getKey();
			HashSet<String> value = entry.getValue();
			places = places + key+";";
			String curSource ="";
			for(String source: value) {
				curSource = curSource + source + "/";
			}
			curSource = curSource.substring(0, curSource.length()-1);
			sources = sources + curSource + ";";
		}
		places = places.substring(0, places.length()-1);
		sources = sources.substring(0, sources.length()-1);
		String[] placeSources = {places, sources};
		return placeSources;
	}

	/**
	 * Method for checking whether a place mention has over-specific geospatial information that may be removed 
	 * to get a match in geonames
	 * @param place The string that will be checked
	 * @return A list of strings that may be removed from the string for recheck
	 */
	private ArrayList<String> hasCheck(String place) {
		ArrayList<String> checksFound = new ArrayList<String>();
		for(String c: checks) {
			if(place.toLowerCase().contains(c)) {
				checksFound.add(c);
			}
		}
		return checksFound;
	}

	/**
	 * @param fileName The name of the file mapping US states to their corresponding two-letter abbreviations
	 */
	private void populateStateMap(String fileName) {
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(fileName));
			String line = reader.readLine();
			while(line!=null){
				String [] parts = line.split("\t");
				stateMap.put(parts[1].trim(), parts[0].trim());
				line = reader.readLine();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * Method for checking if a feature code is insufficient
	 * @param fcode the feature code
	 * @return true if feature code is insufficient
	 */
	public boolean isInsufficientFcode(String fcode) {
		for(String r: insuffFcodes) {	
			if(fcode.equals(r)) {
				return true;
			}
		}
		return false;
	}

}




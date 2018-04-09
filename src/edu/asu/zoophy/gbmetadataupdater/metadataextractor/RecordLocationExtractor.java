package edu.asu.zoophy.gbmetadataupdater.metadataextractor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Set;

import edu.asu.zoophy.gbmetadataupdater.geonamelucene.LuceneSearcher;
import edu.asu.zoophy.gbmetadataupdater.metadataextractor.GBRecord.GBRecordFields;


/**
 * Class for determining whether a GenBank record is sufficient based on its metadata and extracting the 
 * most specific location information from the record
 * @author ttahsin
 * 
 */
/**
 * @author tasnia
 *
 */
public class RecordLocationExtractor {

	public static void main(String[] args) throws Exception {
		BufferedReader b = new BufferedReader(new FileReader(System.getProperty("user.dir")+"/Resources/TestFiles/influenza3-17.txt"));
		//BufferedReader b = new BufferedReader(new FileReader(System.getProperty("user.dir")+"/Resources/TestFiles/newtest.txt"));
		BufferedWriter w = new BufferedWriter(new FileWriter(System.getProperty("user.dir")+"/Resources/TestFiles/curOutput3.txt"));
		String line = b.readLine();
		RecordLocationExtractor rle = new RecordLocationExtractor();
		while(line!=null) {
			String[] lineParts = line.split("\t");
			GBMetadata gm = rle.extractGBLocation(lineParts[6], lineParts[2], lineParts[7], lineParts[10]);
			System.out.print(lineParts[0]+"\t"+gm.printAll()+"\n");
			w.write(lineParts[0]+"\t"+gm.printAll()+"\t"+lineParts[6]+"\t"+lineParts[2]+"\t"+lineParts[7]+"\t"+lineParts[10]+"\n");
			line=b.readLine();
			rle.clear();
		}
		rle.closeSearcher();
		w.close();
	}
	
	//a filter for species names since they may be mistaken as locations
	String[] animalNames = new String[] { "duck", "turkey", "chicken", "canine", "raccoon", "eagle owl", "swine", "pika", "equine", "feline", "goose", "gull", "wild duck", "pintail", "mallard", "quail", "partridge", "peacock", "pheasant", "tern"};
	
	MetadataLocationProcessor mlp;
	
	//most specific GenBank geospatial metadata extracted
	GBMetadata finalRecLoc;


	/**
	 * Constructor creates new instance of MetadataLocationProcessor
	 * @throws Exception
	 */
	public RecordLocationExtractor() throws Exception {
		mlp = new MetadataLocationProcessor();
	
	}

	/**
	 * Constructor creates new instance of MetadataLocationProcessor using the given searcher
	 * @param searcher The searcher for the Lucene index created
	 * @throws Exception
	 */
	public RecordLocationExtractor(LuceneSearcher searcher) throws Exception {
		mlp = new MetadataLocationProcessor(searcher);
	}

	/**
	 * Method to close the searcher if needed
	 */
	public void closeSearcher()  {
		mlp.closeSearcher();
	}
	
	
	/**
	 * Method to change finalRecLoc to null
	 */
	public void clear() {
		finalRecLoc=null;
		mlp.clear();
	}

	/**
	 * Method to check if a country field has sufficient geospatial data
	 * @param country The information in country field of the record
	 * @return true if country field has sufficient geospatial data
	 * @throws Exception
	 */
	protected boolean checkCountry(String country) throws Exception {
		//split the field on ,:- since they separate different location mentions
		String[] places = country.split("[,:\\-_\\d]+");
		Set<String> missedPlaces = new HashSet<String>(); 
		for(String place: places) {
			place = place.toLowerCase();
			place = place.replaceAll("\"", "");
			place = place.replaceAll("( co\\.)", " county");
			place = place.replaceAll(" couunty", " county");
			place = place.replaceAll("\\s+$", "");
			place = place.replaceAll("^\\s+", "");
			place = place.replaceAll("\\d+", " ").trim();
			if(!place.equals("null")&&!place.equals("")) {
				int hasCountry = mlp.parseAsCountry(place);
				String placeAdded = mlp.checkMention(place);
				if(hasCountry==1) {
					mlp.updateSourceMap(place, "country");
				}
				if(placeAdded.length()>0) {
					mlp.updateSourceMap(placeAdded, "country");
				}
				if(placeAdded.length()==0 && hasCountry==0) {
					missedPlaces.add(place);
				}
			}
		}
		for(String place: missedPlaces) {
			String correction = mlp.spellCheck(place);
			if(correction.length()>5) {
				String placeAdded = mlp.checkMention(place);
				if(placeAdded.length()>0) {
					mlp.updateSourceMap(placeAdded, "country");
				}
			}
		}
		GBMetadata gm = mlp.integrateLocations();
		mlp.completeMetadataIfPossible(gm);
		if(gm.isSufficient()){
			finalRecLoc = gm;
			return true;
		} else {
			return false;
		}
	}


	/**
	 * Method to check if a strain, organism, host or isolate field has sufficient geospatial data
	 * @param field The information contained in the field
	 * @param type The type of the field (strain, organism, host or isolate)
	 * @return true if the  field has sufficient geospatial data
	 * @throws Exception
	 */
	protected boolean checkOtherField(String field, String type) throws Exception {
		//split field on "/" since this separates different parts of strain field in influenza (other fields contain strains)
		String[] strainFields = field.split("[/\\-_\\d]+");
		Set<String> missedPlaces = new HashSet<String>();
		for(int i=0; i<strainFields.length; i++) {	
			String strainField = strainFields[i];
			strainField = strainField.toLowerCase();
			strainField = strainField.replaceAll("\\s+$", "");
			strainField = strainField.replaceAll("^\\s+", "");
			strainField = strainField.replaceAll("\\d+", " ").trim();
			if(!strainField.equals("null") && !isNumeric(strainField)&&!strainField.equals("")&&!isAnimal(strainField,i)) {
				int hasCountry = mlp.parseAsCountry(strainField);
				String placeAdded="";
				if(strainField.length()>3) {
					placeAdded = mlp.checkMention(strainField);
				}
				if(hasCountry==1) {
					mlp.updateSourceMap(strainField, type);
				}
				if(placeAdded.length()>0) {
					mlp.updateSourceMap(placeAdded, type);
				}
				if(placeAdded.length()==0 && hasCountry==0) {
					missedPlaces.add(strainField);
				}
			}
		}
		for(String place: missedPlaces) {
			String correction = mlp.spellCheck(place);
			if(correction.length()>5) {
				String placeAdded = mlp.checkMention(place);
				if(placeAdded.length()>0) {
					mlp.updateSourceMap(placeAdded, field);
				}
			}
		}
		GBMetadata gm = mlp.integrateLocations();
		mlp.completeMetadataIfPossible(gm);
		if(gm.isSufficient()){
			finalRecLoc = gm;
			return true;
		} else {
			return false;
		}
	}


	/**
	 * Method to check if country, isolate, strain or organism field is sufficient and extract
	 * most specific location mention from these fields
	 * @param country
	 * @param isolate
	 * @param strain
	 * @param organism
	 * @return the most specific and most comprehensive geospatial metadata present in the record fields checked
	 * @throws Exception
	 */
	public GBMetadata extractGBLocation(String country, String strain, String isolate, String organism) throws Exception {
		//String host = gb.getFields().get("Host");
		if(checkCountry(country)==true) {
			finalRecLoc.isSufficientInCountry=true;
			finalRecLoc.recSource="country";
		} else if(checkOtherField(strain, "strain")==true) {
			finalRecLoc.recSource="strain";
		} else if (checkOtherField(isolate, "isolate")==true) {
			finalRecLoc.recSource="isolate";
		} else if (checkOtherField(organism, "organism")==true) {
			finalRecLoc.recSource="organism";
		} else {
			finalRecLoc = mlp.integrateLocations();
			mlp.completeMetadataIfPossible(finalRecLoc);
		}
		String[] placeSources = mlp.getAllLocationAndSources();
		finalRecLoc.allSources=placeSources[1];
		finalRecLoc.allLocations=placeSources[0];
		mlp.clear();
		return finalRecLoc;
	}
	
	/**
	 * Method to check if country, isolate, strain or organism field is sufficient and extract
	 * most specific location mention from these fields
	 * @param country
	 * @param isolate
	 * @param strain
	 * @param organism
	 * @return the most specific and most comprehensive geospatial metadata present in the record fields checked
	 * @throws Exception
	 */
	public GBMetadata extractGBLocation(String country, String strain, String isolate, String organism, String comment, String note) throws Exception {
		//String host = gb.getFields().get("Host");
		if(checkCountry(country)==true) {
			finalRecLoc.isSufficientInCountry=true;
			finalRecLoc.recSource="country";
		} else if(checkOtherField(strain, "strain")==true) {
			finalRecLoc.recSource="strain";
		} else if (checkOtherField(isolate, "isolate")==true) {
			finalRecLoc.recSource="isolate";
		} else if (checkOtherField(organism, "organism")==true) {
			finalRecLoc.recSource="organism";
		} else if (checkOtherField(note, "note")==true) {
			finalRecLoc.recSource="organism";
		} else if (checkOtherField(comment, "comment")==true) {
			finalRecLoc.recSource="comment";
		} else {
			finalRecLoc = mlp.integrateLocations();
			mlp.completeMetadataIfPossible(finalRecLoc);
		}
		String[] placeSources = mlp.getAllLocationAndSources();
		finalRecLoc.allSources=placeSources[1];
		finalRecLoc.allLocations=placeSources[0];
		mlp.clear();
		return finalRecLoc;
	}


	/**
	 * Method to check if country, isolate, strain or organism field is sufficient and extract
	 * most specific location mention from these fields
	 * @param gb The GenBank record being processed
	 * @return the most specific and most comprehensive geospatial metadata present in gb
	 * @throws Exception
	 */
	public GBMetadata extractGBLocation(GBRecord gb) throws Exception {
		String country = gb.recordMap.get(GBRecordFields.Country);
		String strain = gb.recordMap.get(GBRecordFields.Strain);
		String isolate = gb.recordMap.get(GBRecordFields.Isolate);
		String organism = gb.recordMap.get(GBRecordFields.Organism);
		//String host = gb.getFields().get("Host");
		if(checkCountry(country)==true) {
			finalRecLoc.isSufficientInCountry=true;
			finalRecLoc.recSource="country";
		} else if(checkOtherField(strain, "strain")==true) {
			finalRecLoc.recSource="strain";
		} else if (checkOtherField(isolate, "isolate")==true) {
			finalRecLoc.recSource="isolate";
		} else if (checkOtherField(organism, "organism")==true) {
			finalRecLoc.recSource="organism";
		} else {
			finalRecLoc = mlp.integrateLocations();
			mlp.completeMetadataIfPossible(finalRecLoc);
		}
		String[] placeSources = mlp.getAllLocationAndSources();
		finalRecLoc.allSources=placeSources[1];
		finalRecLoc.allLocations=placeSources[0];
		mlp.clear();
		//gb.gm=finalRecLoc;
		return finalRecLoc;
	}

	
	/**
	 * Method for checking whether a string is numeric
	 * @param str The string that will be checked 
	 * @return true if string is numeric, false otherwise
	 */
	protected static boolean isNumeric(String str)  {  
		try  {  
			double d = Double.parseDouble(str);  
		}  
		catch(NumberFormatException nfe)  {  
			return false;  
		}  
		return true;  
	}
	
	/**
	 * @param str 
	 * @param i
	 * @return
	 */
	protected boolean isAnimal(String str, int i) {
		if(i<2) {
			for(String animal: animalNames) {
				if(animal.equals(str)) {
					return true;
				}
			}
		}
		return false;
	}
}




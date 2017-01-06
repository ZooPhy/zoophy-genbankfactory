package edu.asu.zoophy.genbankfactory.utils.normalizer.date;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TempDate {
	private String accession;
	private String date;
	private String strain;
	private String isolate;
	private String organism;
	private String definition;

	public TempDate() {
		
	}

	public String getStrain() {
		return strain;
	}

	public void setStrain(String strain) {
		this.strain = strain;
	}

	public String getAccession() {
		return accession;
	}

	public void setAccession(String accession) {
		this.accession = accession;
	}

	public String getIsolate() {
		return isolate;
	}

	public void setIsolate(String isolate) {
		this.isolate = isolate;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public boolean checkForDate() {
		if (date == null) {
			if (strain != null) {
				if (strain.contains("/")) {
					String possYr = strain.substring(strain.lastIndexOf("/")+1);
					try {
						int yr = Integer.parseInt(possYr);
						if (yr < 100) {
							if (yr < 17) {
								yr += 2000;
							}
							else {
								yr += 1900;
							}
						}
						if (yr > 1900 && yr < 2020) {
							date = String.valueOf(yr);
						}	
					}
					catch (Exception e) {
						//not found
					}
				}
			}
			if (date == null && isolate != null) {
				if (isolate.contains("/")) {
					String possYr = isolate.substring(isolate.lastIndexOf("/")+1);
					try {
						int yr = Integer.parseInt(possYr);
						if (yr < 100) {
							if (yr < 17) {
								yr += 2000;
							}
							else {
								yr += 1900;
							}
						}
						if (yr > 1900 && yr < 2020) {
							date = String.valueOf(yr);
						}	
					}
					catch (Exception e) {
						//not found
					}
				}
			}
			if (date == null && organism != null) {
				if (organism.contains("/")) {
					String longPattern = "(/\\d{2,4}\\(|/\\d{2,4}\\)|/\\d{2,4}-)";
					Pattern longDatePattern = Pattern.compile(longPattern);
					Matcher dateMatcher = longDatePattern.matcher(organism);
					while (dateMatcher.find()) {
						for (int i = 0; i <dateMatcher.groupCount() && date == null; i++) {
							try {
								String match = dateMatcher.group(0).substring(1, dateMatcher.group(0).length()-1);
								int yr = Integer.parseInt(match);
								if (yr < 100) {
									if (yr < 17) {
										yr += 2000;
									}
									else {
										yr += 1900;
									}
								}
								if (yr > 1900 && yr < 2020) {
									date = String.valueOf(yr);
								}	
							}
							catch (Exception e) {
								//not found
							}
						}
					}
				}
			}
			if (date == null && definition != null) {
				if (definition.contains("/")) {
					String longPattern = "(/\\d{2,4}\\(|/\\d{2,4}\\)|/\\d{2,4}-)";
					Pattern longDatePattern = Pattern.compile(longPattern);
					Matcher dateMatcher = longDatePattern.matcher(definition);
					while (dateMatcher.find()) {
						for (int i = 0; i <dateMatcher.groupCount() && date == null; i++) {
							try {
								String match = dateMatcher.group(0).substring(1, dateMatcher.group(0).length()-1);
								int yr = Integer.parseInt(match);
								if (yr < 100) {
									if (yr < 17) {
										yr += 2000;
									}
									else {
										yr += 1900;
									}
								}
								if (yr > 1900 && yr < 2020) {
									date = String.valueOf(yr);
								}	
							}
							catch (Exception e) {
								//not found
							}
						}
					}
				}
			}
			if (date == null) {
				return false;
			}
		}
		return true;
	}

	public String getOrganism() {
		return organism;
	}

	public void setOrganism(String organism) {
		this.organism = organism;
	}

	public String getDefinition() {
		return definition;
	}

	public void setDefinition(String definition) {
		this.definition = definition;
	}
	
}

package edu.asu.zoophy.gblocationupdater.time;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.AnnotationPipeline;
import edu.stanford.nlp.pipeline.POSTaggerAnnotator;
import edu.stanford.nlp.pipeline.TokenizerAnnotator;
import edu.stanford.nlp.pipeline.WordsToSentencesAnnotator;
import edu.stanford.nlp.time.TimeAnnotations;
import edu.stanford.nlp.time.TimeAnnotator;
import edu.stanford.nlp.time.TimeExpression;
import edu.stanford.nlp.util.CoreMap;

public class TimeNormalizer {
	AnnotationPipeline pipeline;
	static final String curYear = String.valueOf(Calendar.getInstance().get(Calendar.YEAR));

	public TimeNormalizer() {
		String home = "C:/Users/ttasn/workspace/Lib/stanford-corenlp-full-2015-12-09";
		String defs_sutime = home+"/sutime/defs.sutime.txt";
		String holiday_sutime = home+"/sutime/english.holidays.sutime.txt";
		String sutime = home+"/sutime/english.sutime.txt";
		Properties props = new Properties();
		String sutimeRules = defs_sutime + "," + holiday_sutime
				+ "," + sutime;
		props.setProperty("sutime.rules", sutimeRules);
		props.setProperty("sutime.binders", "0");
		pipeline = new AnnotationPipeline();
		pipeline.addAnnotator(new TokenizerAnnotator(false));
		pipeline.addAnnotator(new WordsToSentencesAnnotator(false));
		pipeline.addAnnotator(new POSTaggerAnnotator(false));
		pipeline.addAnnotator(new TimeAnnotator("sutime", props));
	}
	
	public TimeNormalizer(String home) {
		//String home = "C:/Users/ttasn/workspace/Lib/stanford-corenlp-full-2015-12-09";
		String defs_sutime = home+"/sutime/defs.sutime.txt";
		String holiday_sutime = home+"/sutime/english.holidays.sutime.txt";
		String sutime = home+"/sutime/english.sutime.txt";
		Properties props = new Properties();
		String sutimeRules = defs_sutime + "," + holiday_sutime
				+ "," + sutime;
		props.setProperty("sutime.rules", sutimeRules);
		props.setProperty("sutime.binders", "0");
		pipeline = new AnnotationPipeline();
		pipeline.addAnnotator(new TokenizerAnnotator(false));
		pipeline.addAnnotator(new WordsToSentencesAnnotator(false));
		pipeline.addAnnotator(new POSTaggerAnnotator(false));
		pipeline.addAnnotator(new TimeAnnotator("sutime", props));
	}
	public enum Months {
		Jan ("01", "January"), Feb ("02", "February"), Mar ("03", "March"), Apr ("04", "April"), May ("05", "May"), Jun("06", "June"), Jul("07", "July"), Aug("08", "August"), Sep("09", "September"), Oct("10", "October"), Nov("11", "November"), Dec("12", "December");
		private final String value;
		private final String fullName;
		private Months(String value, String fullName) {
			this.value = value;
			this.fullName=fullName;
		}
		public String getValue() {
			return value;
		}
	}
	
	public static void main(String[] args) throws IOException {
//		TimeNormalizer a = new TimeNormalizer();
//		Scanner reader = new Scanner(System.in);  // Reading from System.in
//		System.out.println("Enter date: ");
//		String n = reader.nextLine();
//		while(!n.equals("exit")) {
//			String date = a.getNormDateNew(n);
//			System.out.println("Result ["+date+"]");
//			n=reader.nextLine();
//		}
		
		String metafile = args[0];
		int defIndex = Integer.valueOf(args[1]);
		int dateIndex = Integer.valueOf(args[2]);
		int strainIndex = Integer.valueOf(args[3]);
		int organismIndex = Integer.valueOf(args[4]);
		String outfile = args[5];
		String home = args[6];
		BufferedReader reader = new BufferedReader(new FileReader(metafile));
		BufferedWriter writer = new BufferedWriter(new FileWriter(outfile));
		String line = reader.readLine();
		writer.write(line+"\tyear\tmonth\tday\n");
		line = reader.readLine();
		TimeNormalizer tn = new TimeNormalizer(home);
		while(line!=null) {
			String[] fields = line.split("\t");
			String date = "";
			if(fields.length>dateIndex) {
				date = fields[dateIndex];
			}
			
			String strain = fields[strainIndex];
			String organism = fields[organismIndex];
			String definition = fields[defIndex];
			DateParts normalizedDate = null;
			if(date.length()>0) {
				normalizedDate = tn.getNormDateAll(date);
			} else {
				normalizedDate = new DateParts("", "", "", "1000-01-01", 7);
			}
			if(normalizedDate.accuracy==7 && definition.toLowerCase().contains("influenza")) {
				String year = tn.extractYear(organism, strain);
				if(year.length()==4) {
					normalizedDate.year=Integer.parseInt(year);
					normalizedDate.accuracy=4;
					normalizedDate.date = year+normalizedDate.date.substring(4, normalizedDate.date.length());
				}
			}
			writer.write(line+"\t"+normalizedDate.year+"\t"+normalizedDate.month+"\t"+normalizedDate.day+"\n");
			line = reader.readLine();
		}
		writer.close();
	}
	public String getNormDate(String date) {
		Annotation annotation = new Annotation(date);
		annotation.set(CoreAnnotations.DocDateAnnotation.class, "2017-07-21");
		pipeline.annotate(annotation);
		List<CoreMap> timexAnnsAll = annotation.get(TimeAnnotations.TimexAnnotations.class);
		String normDate ="";
		if(timexAnnsAll.size()>0) {
			normDate = timexAnnsAll.get(0).get(TimeExpression.Annotation.class).getTemporal().toString();
		} else {
			String[] normDateParts = new String[3];
			String[] curDateParts = date.split("-");
			if(curDateParts.length==0) {
				return "";
			} else if(curDateParts.length==1) {
				return curDateParts[0];
			} else if(curDateParts.length==2) {
				normDateParts[2]="##";
				if(!isNumeric(curDateParts[1])) {
					return "";
				}
				if(Integer.valueOf(curDateParts[1])<=17) {
					normDateParts[0]= "20" +curDateParts[1];
				} else if(curDateParts[1].length()==4) {
					normDateParts[0] = curDateParts[1];
				} else {
					return "";
				}
				Months[] months = Months.values();
				for(Months month: months) {
					if(curDateParts[0].equals(month.name())) {
						normDateParts[1]=month.getValue();
					}
				}
				if(normDateParts[1].length()==0) {
					if(normDateParts[0]!=null && normDateParts[0].length()==4) {
						return normDateParts[0];
					}
					return "";
				}
			} else if(curDateParts.length==3) {
				if(!isNumeric(curDateParts[2])||!isNumeric(curDateParts[0])) {
					if(isNumeric(curDateParts[2])) {
						if(Integer.valueOf(curDateParts[2])<=17) {
							normDateParts[0]= "20" +curDateParts[2];
						} else if(curDateParts[2].length()==4) {
							normDateParts[0] = curDateParts[2];
						} else {
							return "";
						}
						return normDateParts[0];
					}
					return "";
				}
				if(Integer.valueOf(curDateParts[2])<=17) {
					normDateParts[0]= "20" +curDateParts[2];
				} else if(curDateParts[2].length()==4) {
					normDateParts[0] = curDateParts[2];
				} else {
					return "";
				}
				
				Months[] months = Months.values();
				for(Months month: months) {
					if(curDateParts[1].equals(month.name())) {
						normDateParts[1]=month.getValue();
					}
				}
				if(normDateParts[1].length()==0) {
					if(normDateParts[0]!=null && normDateParts[0].length()==4) {
						return normDateParts[0];
					}
					return "";
				}
				if(Integer.valueOf(curDateParts[0])<=9 && !curDateParts[0].startsWith("0")) {
					normDateParts[2]="0"+curDateParts[0];
				} else {
					normDateParts[2]=curDateParts[0];
				}
			}
			for(String normDatePart: normDateParts) {
				normDate = normDate+normDatePart+"-";
			}
			normDate = normDate.replaceAll("-$", "");
			normDate = normDate.replaceAll("#", "");
			normDate = normDate.replaceAll("-$", "");
		}
		return normDate;
	}
	
	public String getNormDateNew(String date) {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd");
		LocalDate localDate = LocalDate.now();
		String curDate = dtf.format(localDate);
		Months[] months = Months.values();
		int curYear = Integer.valueOf(curDate.split("/")[0].substring(2,4));
		Annotation annotation = new Annotation(date);
		//annotation.set(CoreAnnotations.DocDateAnnotation.class, curDate);
		pipeline.annotate(annotation);
		List<CoreMap> timexAnnsAll = annotation.get(TimeAnnotations.TimexAnnotations.class);
		String normDate = "XXXX-XX-XX";
		if(timexAnnsAll.size()>0) {
			normDate = timexAnnsAll.get(0).get(TimeExpression.Annotation.class).getTemporal().toISOString().toString();
			if(normDate.length()==4) {
				normDate = normDate+"-XX-XX";
			} else if (normDate.length()==7) {
				normDate = normDate+"-XX";
			}
		} else {
			String newDate = date.toLowerCase();
			int numMon = 0;
			for(Months month: months) {
				if(newDate.contains(month.name().toLowerCase())) {
					newDate = newDate.replaceAll(month.name().toLowerCase(), month.fullName.toLowerCase());
					numMon++;
				}
			}
			annotation = new Annotation(newDate);
			annotation.set(CoreAnnotations.DocDateAnnotation.class, curDate);
			
			pipeline.annotate(annotation);
			timexAnnsAll = annotation.get(TimeAnnotations.TimexAnnotations.class);
			if(timexAnnsAll.size()==0 && numMon>0 && newDate.split("[-/ ]").length>3) {
				String nNewDate = date.toLowerCase();
				for(Months month: months) {
					if(newDate.contains(month.name().toLowerCase())) {
						nNewDate = nNewDate.replaceAll(month.name().toLowerCase(), month.getValue());
						numMon++;
					} else if(newDate.contains(month.fullName.toLowerCase())) {
						nNewDate = nNewDate.replaceAll(month.fullName.toLowerCase(), month.getValue());
						numMon++;
					}
				}
				annotation = new Annotation(nNewDate);
				annotation.set(CoreAnnotations.DocDateAnnotation.class, curDate);
				pipeline.annotate(annotation);
				timexAnnsAll = annotation.get(TimeAnnotations.TimexAnnotations.class);
			}
			if(timexAnnsAll.size()>0) {
				normDate = timexAnnsAll.get(0).get(TimeExpression.Annotation.class).getTemporal().toISOString().toString();
				if(normDate.length()==4) {
					normDate = normDate+"-XX-XX";
				} else if (normDate.length()==7) {
					normDate = normDate+"-XX";
				}
			} else {
				String[] curDateParts = date.split("[-/]");
				if(curDateParts.length==1) {
					if(curDateParts[0].length()==4 && isNumeric(curDateParts[0])) {
						normDate=curDateParts[0]+"-XX-XX";
					}
				} else if(curDateParts.length==2) {
					int monIndex = -1;
					String monVal = "";
					int rem = -1;
					for(Months month: months) {
						if(curDateParts[0].toLowerCase().equals(month.name().toLowerCase())) {
							monIndex = 0;
							rem = 1;
							monVal = month.getValue();
							break;
						} else if(curDateParts[1].toLowerCase().equals(month.name().toLowerCase())) {
							monIndex = 1;
							rem=0;
							monVal = month.getValue();
							break;
						}
					}
					if(rem>-1) {
						String val = curDateParts[rem];
						if(val.length()==4 && isNumeric(val)) {
							normDate = val+"-"+monVal+"-XX";
						} else if(val.length()<=2 && isNumeric(val)&& Integer.valueOf(val)<=31) {
							if(val.length()==1) {
								normDate = "XXXX-"+monVal+"-0"+val;
							} else {
								normDate = "XXXX-"+monVal+"-"+val;
							}
						} else if(val.length()==2 && isNumeric(val) && Integer.valueOf(val)>31) {
							normDate = "19"+val+"-"+monVal+"-XX";
						}
					}
				} else if(curDateParts.length==3) {
					String yrVal = "XXXX";
					String dayVal = "XX";
					String monVal = "XX";
					for(Months month: months) {
						if(curDateParts[1].toLowerCase().equals(month.name().toLowerCase())) {
							monVal = month.getValue();
							break;
						} 
					}
					if(isNumeric(curDateParts[2])) {
						if(Integer.valueOf(curDateParts[2])<=curYear) {
							yrVal= "20" +curDateParts[2];
						} else if(curDateParts[2].length()==4) {
							yrVal = curDateParts[2];
						} else if(curDateParts[2].length()==2) {
							yrVal = "19"+curDateParts[2];
						}
					}
					if(isNumeric(curDateParts[0])&& curDateParts[0].length()<=2 && Integer.valueOf(curDateParts[0])<=31) {
						if(curDateParts[0].length()==1) {
							dayVal = "0"+curDateParts[0];
						} else {
							dayVal = curDateParts[0];
						}
					}
					normDate = yrVal+"-"+monVal+"-"+dayVal;
				}
			}
		}

		if(normDate.substring(0, 2).equals("XX")&&isNumeric(normDate.substring(2,4))) {
			int val = Integer.parseInt(normDate.substring(2,4));
			String year = "";
			if(val<=9) {
				year = "200"+val;
			}
			else if(val<=curYear) {
				year = "20"+val;
			} else {
				year = "19"+val;
			}
			normDate = year+normDate.substring(4,normDate.length());
		}
		if(normDate.substring(0,4).equals("XXXX")&&!normDate.substring(8,10).equals("XX")) {
			String[] dateParts = date.split(" ");
			if(dateParts.length==3) {
				int monIndex = -1;
				for(Months month: months) {
					if(dateParts[1].toLowerCase().equals(month.name().toLowerCase())) {
						monIndex=1;
						break;
					}
				}
				if(monIndex==1 && isNumeric(dateParts[2]) && dateParts[2].length()==2) {
					int yrVal = Integer.parseInt(dateParts[2]);
					String year = "";
					if(yrVal<=9) {
						year = "200"+yrVal;
					}
					else if(yrVal<=curYear) {
						year = "20"+yrVal;
					} else {
						year = "19"+yrVal;
					}
					normDate = year+normDate.substring(4,normDate.length());
				}
			}
		}
		return normDate;
	}
	public DateParts getNormDateAll(String date) {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd");
		LocalDate localDate = LocalDate.now();
		String curDate = dtf.format(localDate);
		Months[] months = Months.values();
		int curYear = Integer.valueOf(curDate.split("/")[0].substring(2,4));
		Annotation annotation = new Annotation(date);
		//annotation.set(CoreAnnotations.DocDateAnnotation.class, curDate);
		pipeline.annotate(annotation);
		List<CoreMap> timexAnnsAll = annotation.get(TimeAnnotations.TimexAnnotations.class);
		String normDate = "XXXX-XX-XX";
		if(timexAnnsAll.size()>0) {
			normDate = timexAnnsAll.get(0).get(TimeExpression.Annotation.class).getTemporal().toISOString().toString();
			if(normDate.length()==4) {
				normDate = normDate+"-XX-XX";
			} else if (normDate.length()==7) {
				normDate = normDate+"-XX";
			}
		} else {
			String newDate = date.toLowerCase();
			int numMon = 0;
			for(Months month: months) {
				if(newDate.contains(month.name().toLowerCase())) {
					newDate = newDate.replaceAll(month.name().toLowerCase(), month.fullName.toLowerCase());
					numMon++;
				}
			}
			annotation = new Annotation(newDate);
			annotation.set(CoreAnnotations.DocDateAnnotation.class, curDate);
			
			pipeline.annotate(annotation);
			timexAnnsAll = annotation.get(TimeAnnotations.TimexAnnotations.class);
			if(timexAnnsAll.size()==0 && numMon>0 && newDate.split("[-/ ]").length>3) {
				String nNewDate = date.toLowerCase();
				for(Months month: months) {
					if(newDate.contains(month.name().toLowerCase())) {
						nNewDate = nNewDate.replaceAll(month.name().toLowerCase(), month.getValue());
						numMon++;
					} else if(newDate.contains(month.fullName.toLowerCase())) {
						nNewDate = nNewDate.replaceAll(month.fullName.toLowerCase(), month.getValue());
						numMon++;
					}
				}
				annotation = new Annotation(nNewDate);
				annotation.set(CoreAnnotations.DocDateAnnotation.class, curDate);
				pipeline.annotate(annotation);
				timexAnnsAll = annotation.get(TimeAnnotations.TimexAnnotations.class);
			}
			if(timexAnnsAll.size()>0) {
				normDate = timexAnnsAll.get(0).get(TimeExpression.Annotation.class).getTemporal().toISOString().toString();
				if(normDate.length()==4) {
					normDate = normDate+"-XX-XX";
				} else if (normDate.length()==7) {
					normDate = normDate+"-XX";
				}
			} else {
				String[] curDateParts = date.split("[-/]");
				if(curDateParts.length==1) {
					if(curDateParts[0].length()==4 && isNumeric(curDateParts[0])) {
						normDate=curDateParts[0]+"-XX-XX";
					}
				} else if(curDateParts.length==2) {
					int monIndex = -1;
					String monVal = "";
					int rem = -1;
					for(Months month: months) {
						if(curDateParts[0].toLowerCase().equals(month.name().toLowerCase())) {
							monIndex = 0;
							rem = 1;
							monVal = month.getValue();
							break;
						} else if(curDateParts[1].toLowerCase().equals(month.name().toLowerCase())) {
							monIndex = 1;
							rem=0;
							monVal = month.getValue();
							break;
						}
					}
					if(rem>-1) {
						String val = curDateParts[rem];
						if(val.length()==4 && isNumeric(val)) {
							normDate = val+"-"+monVal+"-XX";
						} else if(val.length()<=2 && isNumeric(val)&& Integer.valueOf(val)<=31) {
							if(val.length()==1) {
								normDate = "XXXX-"+monVal+"-0"+val;
							} else {
								normDate = "XXXX-"+monVal+"-"+val;
							}
						} else if(val.length()==2 && isNumeric(val) && Integer.valueOf(val)>31) {
							normDate = "19"+val+"-"+monVal+"-XX";
						}
					}
				} else if(curDateParts.length==3) {
					String yrVal = "XXXX";
					String dayVal = "XX";
					String monVal = "XX";
					for(Months month: months) {
						if(curDateParts[1].toLowerCase().equals(month.name().toLowerCase())) {
							monVal = month.getValue();
							break;
						} 
					}
					if(isNumeric(curDateParts[2])) {
						if(Integer.valueOf(curDateParts[2])<=curYear) {
							yrVal= "20" +curDateParts[2];
						} else if(curDateParts[2].length()==4) {
							yrVal = curDateParts[2];
						} else if(curDateParts[2].length()==2) {
							yrVal = "19"+curDateParts[2];
						}
					}
					if(isNumeric(curDateParts[0])&& curDateParts[0].length()<=2 && Integer.valueOf(curDateParts[0])<=31) {
						if(curDateParts[0].length()==1) {
							dayVal = "0"+curDateParts[0];
						} else {
							dayVal = curDateParts[0];
						}
					}
					normDate = yrVal+"-"+monVal+"-"+dayVal;
				}
			}
		}

		if(normDate.substring(0, 2).equals("XX")&&isNumeric(normDate.substring(2,4))) {
			int val = Integer.parseInt(normDate.substring(2,4));
			String year = "";
			if(val<=9) {
				year = "200"+val;
			}
			else if(val<=curYear) {
				year = "20"+val;
			} else {
				year = "19"+val;
			}
			normDate = year+normDate.substring(4,normDate.length());
		}
		if(normDate.substring(0,4).equals("XXXX")&&!normDate.substring(8,10).equals("XX")) {
			String[] dateParts = date.split(" ");
			if(dateParts.length==3) {
				int monIndex = -1;
				for(Months month: months) {
					if(dateParts[1].toLowerCase().equals(month.name().toLowerCase())) {
						monIndex=1;
						break;
					}
				}
				if(monIndex==1 && isNumeric(dateParts[2]) && dateParts[2].length()==2) {
					int yrVal = Integer.parseInt(dateParts[2]);
					String year = "";
					if(yrVal<=9) {
						year = "200"+yrVal;
					}
					else if(yrVal<=curYear) {
						year = "20"+yrVal;
					} else {
						year = "19"+yrVal;
					}
					normDate = year+normDate.substring(4,normDate.length());
				}
			}
		}
		DateParts d = parseNormDate(normDate);
		return d;
	}
	
	public DateParts parseNormDate(String normDate) {
		String[] parts = normDate.split("-");
		String year = "";
		String month = "";
		String day="";
		if(isNumeric(parts[0])) {
			year = parts[0];
		}
		if(isNumeric(parts[1])) {
			month = parts[1];
		}
		if(isNumeric(parts[2])) {
			day = parts[2];
		}
		String date = normDate.replaceAll("XXXX", "1000");
		date = date.replaceAll("XX", "01");
		int accuracy;
		if(year.length()>0 && month.length()>0 && day.length()>0) {
			accuracy =0;
		}
		else if(year.length()>0 && month.length()>0) {
			accuracy = 1;
		} else if(month.length()>0 && day.length()>0) {
			accuracy = 2;
		} else if(year.length()>0 && day.length()>0) {
			accuracy = 3;
		} else if(year.length()>0) {
			accuracy = 4;
		} else if(month.length()>0) {
			accuracy = 5;
		} else if(day.length()>0) {
			accuracy = 6;
		} else {
			accuracy =7;
		}
		DateParts dp = new DateParts(day, month, year, date, accuracy);
		return dp;
	}
	
	public String extractYear(String organism, String strain) {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd");
		LocalDate localDate = LocalDate.now();
		String curDate = dtf.format(localDate);
		Months[] months = Months.values();
		int curYear = Integer.valueOf(curDate.split("/")[0].substring(2,4));
		String year ="";
		String[] strainParts = strain.split("/");
		int len=strainParts.length;
		if(len>3 && isNumeric(strainParts[len-1])) {
			String curStrVal = strainParts[len-1];
			year = extractFromStrain(curYear, len, curStrVal);
		}
		if(year.length()==0) {
			String[] organismParts = organism.split("\\(");
			if(organismParts.length==3) {
				String[] organismParts2 = organism.split("/");
				String curStrVal = organismParts2[organismParts2.length-1].split("\\(")[0];
				if(organismParts2.length>3 && isNumeric(curStrVal)) {
					year = extractFromStrain(curYear, len, curStrVal);
				}
			}
		}
		return year;
	}
	private String extractFromStrain(int curYear, int len, String curStrVal) {
		String year = "";
		if(curStrVal.length()==4) {
			year = curStrVal;
		} else if (curStrVal.length()==2){
			int val = Integer.valueOf(curStrVal);
			if(val<=9) {
				year = "200"+val;
			} else if(val<=curYear) {
				year = "20"+val;
			} else {
				year = "19"+val;
			}
		}
		return year;
	}
	public String retrieveDate(String curValue, String organism) {
		String value;
		String[] dateParts = curValue.split("-");
		String[] organismParts = organism.split("\\(");
		if(dateParts.length>1) {
			String partYear = dateParts[dateParts.length-1].trim();
			if(partYear.length()==4) {
				value = partYear;
			} else if(partYear.length()==2) {			
				//	System.out.println("Curyear:" + Integer.valueOf(curYear.substring(2,4)));
				if(Integer.valueOf(partYear)<=Integer.valueOf(curYear.substring(2,4))) {
					value = curYear.substring(0,2)+partYear;
					//		System.out.println("inside check year="+year);
				} else {
					value = "19"+partYear;
				}
			} else {
				//	System.out.println("NOT YEAR!!!");
				if(organismParts.length==3) {
					String[] organismParts2 = organism.split("/");
					value = organismParts2[organismParts2.length-1].split("\\(")[0];
				//	System.out.println("date:"+year);
				} else {
					value = curValue;
				}
			}
		} else {
			if(organismParts.length==3) {
				String[] organismParts2 = organism.split("/");
				value = organismParts2[organismParts2.length-1].split("\\(")[0];
				//System.out.println("date:"+year);
			} else {
				value = curValue;
			}
		}
		//	System.out.println(year);
		if(value.length()!=4) {
			//	System.out.println(year+" " + date+ " NOT DATE!");
			value = curValue;
		}
		return value;
	}
	
	public static boolean isNumeric(String str)
	{
		return str.matches("-?\\d+(.\\d+)?");
	}
}

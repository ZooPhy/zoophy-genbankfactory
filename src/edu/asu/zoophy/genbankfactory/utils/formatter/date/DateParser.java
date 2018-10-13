package edu.asu.zoophy.genbankfactory.utils.formatter.date;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;

public class DateParser {

	public static String normalizeDate(String dateString) {
		
		String normalizedDate = "";
		DateTimeFormatter defaultFormatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy");
		DateTimeFormatter monthYearFormatter = new DateTimeFormatterBuilder()
			    .appendPattern("MMM-uuuu")
			    .parseDefaulting(ChronoField.DAY_OF_MONTH, 1) 
			    .toFormatter(Locale.ENGLISH);
		
		DateTimeFormatter yearFormatter = new DateTimeFormatterBuilder()
				.appendPattern("uuuu")
				.parseDefaulting(ChronoField.MONTH_OF_YEAR, 7)
				.parseDefaulting(ChronoField.DAY_OF_MONTH,1)
				.toFormatter(Locale.ENGLISH);
		
		DateTimeFormatter reverseDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		LocalDate localDate ;
		if (parse(dateString,defaultFormatter)) {
			localDate = LocalDate.parse(dateString, defaultFormatter);
			normalizedDate = localDate.format(defaultFormatter);
			
		} else if (parse(dateString,monthYearFormatter)) {
			localDate = LocalDate.parse(dateString, monthYearFormatter);
			normalizedDate = localDate.format(defaultFormatter);
		}
		else if (parse(dateString, yearFormatter)){
			localDate = LocalDate.parse(dateString, yearFormatter);
			normalizedDate = localDate.format(defaultFormatter);
			
		}if (parse(dateString, reverseDateFormatter)) {
			localDate = LocalDate.parse(dateString, reverseDateFormatter);
			normalizedDate = localDate.format(defaultFormatter);
		}
		
		return normalizedDate;
	}
	
	public static boolean parse(String date, DateTimeFormatter dateFormat) {
		
		try {
			LocalDate.parse(date, dateFormat);
			return true;
			
		}catch (Exception e) {
			return false;
			
		}
	}

}



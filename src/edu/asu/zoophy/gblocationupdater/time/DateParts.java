package edu.asu.zoophy.gblocationupdater.time;

public class DateParts {
	public int day=0;
	public int month=0;
	public int year=0;
	public String date;
	public int accuracy;
	public DateParts(String day, String month, String year, String date, int accuracy) {
		if(day.length()>0) {
			this.day=Integer.parseInt(day);
		}
		if(month.length()>0) {
			this.month=Integer.parseInt(month);
		}
		if(year.length()>0) {
			this.year=Integer.parseInt(year);
		}
		this.date=date;
		this.accuracy=accuracy;
	}
}
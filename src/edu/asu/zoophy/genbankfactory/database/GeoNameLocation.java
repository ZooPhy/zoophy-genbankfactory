package edu.asu.zoophy.genbankfactory.database;

public class GeoNameLocation {
	private int id;
	private String name;
	private double latitude;
	private double longitude;
	private String continent;
	private String country;
	private String adm1;
	private String adm2;
	private String adm3;
	private String adm4;
	private String type;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	public String getContinent() {
		return continent;
	}
	public void setContinent(String continent) {
		this.continent = continent;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getAdm1() {
		return adm1;
	}
	public void setAdm1(String adm1) {
		this.adm1 = adm1;
	}
	public String getAdm2() {
		return adm2;
	}
	public void setAdm2(String adm2) {
		this.adm2 = adm2;
	}
	public String getAdm3() {
		return adm3;
	}
	public void setAdm3(String adm3) {
		this.adm3 = adm3;
	}
	public String getAdm4() {
		return adm4;
	}
	public void setAdm4(String adm4) {
		this.adm4 = adm4;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
}
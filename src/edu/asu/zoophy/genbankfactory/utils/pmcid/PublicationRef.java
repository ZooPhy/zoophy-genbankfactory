package edu.asu.zoophy.genbankfactory.utils.pmcid;

/**
 * A POJO to read the CSV
 * @author Davy
 */
public class PublicationRef {
	private String JournalTitle;
	private String ISSN;
	private String eISSN;
	private String Year;
	private String Volume;
	private String Issue;
	private String Page;
	private String DOI;
	private String PMCID;
	private String PMID;
	private String ManuscriptId;
	private String ReleaseDate;
	
	public PublicationRef(String JournalTitle, String ISSN, String eISSN, String Year, String Volume, String Issue, String Page, String DOI, String PMCID, String PMID, String ManuscriptId, String ReleaseDate){
		this.JournalTitle = JournalTitle;
		this.ISSN = ISSN;
		this.eISSN = eISSN;
		this.Year = Year;
		this.Volume = Volume;
		this.Issue = Issue;
		this.Page = Page;
		this.DOI = DOI;
		this.PMCID = PMCID;
		this.PMID = PMID;
		this.ManuscriptId = ManuscriptId;
		this.ReleaseDate = ReleaseDate;
	}
	
	public String getJournalTitle() {
		return JournalTitle;
	}
	public void setJournalTitle(String journalTitle) {
		JournalTitle = journalTitle;
	}
	public String getISSN() {
		return ISSN;
	}
	public void setISSN(String iSSN) {
		ISSN = iSSN;
	}
	public String geteISSN() {
		return eISSN;
	}
	public void seteISSN(String eISSN) {
		this.eISSN = eISSN;
	}
	public String getYear() {
		return Year;
	}
	public void setYear(String year) {
		Year = year;
	}
	public String getVolume() {
		return Volume;
	}
	public void setVolume(String volume) {
		Volume = volume;
	}
	public String getIssue() {
		return Issue;
	}
	public void setIssue(String issue) {
		Issue = issue;
	}
	public String getPage() {
		return Page;
	}
	public void setPage(String page) {
		Page = page;
	}
	public String getDOI() {
		return DOI;
	}
	public void setDOI(String dOI) {
		DOI = dOI;
	}
	public String getPMCID() {
		return PMCID;
	}
	public void setPMCID(String pMCID) {
		PMCID = pMCID;
	}
	public String getPMID() {
		return PMID;
	}
	public void setPMID(String pMID) {
		PMID = pMID;
	}
	public String getManuscriptId() {
		return ManuscriptId;
	}
	public void setManuscriptId(String manuscriptId) {
		ManuscriptId = manuscriptId;
	}
	public String getReleaseDate() {
		return ReleaseDate;
	}
	public void setReleaseDate(String releaseDate) {
		ReleaseDate = releaseDate;
	}
	
	public String toString() {
		StringBuilder out = new StringBuilder();
		out.append(JournalTitle);out.append(" ");
		out.append(ISSN);out.append(" ");
		out.append(eISSN);out.append(" ");
		out.append(Year);out.append(" ");
		out.append(Volume);out.append(" ");
		out.append(Issue);out.append(" ");
		out.append(Page);out.append(" ");
		out.append(DOI);out.append(" ");
		out.append(PMCID);out.append(" ");
		out.append(PMID);out.append(" ");
		out.append(ManuscriptId);out.append(" ");
		out.append(ReleaseDate);
		return out.toString();
	}
}
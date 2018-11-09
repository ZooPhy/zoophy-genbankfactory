package edu.asu.zoophy.genbankfactory.database;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;


/**
 * 
 * An object for keeping track of the reference
 * @author Mark/Davy
 */
public class Reference {
	
	private final Logger log = Logger.getLogger("Reference");
	private String title = null;
	private String journal = null;
	private String remark = null;
	private int pubmed = 0;
	private String accession = null;
	private String authors = null;
	private String consrtm = null;
	/**
	 * Davy: a constructor for the reference
	 * @param pAccession
	 * @param components
	 */
	public Reference(String pAccession, List<String> components)
	{
		accession = pAccession;
		if(!components.get(0).matches("REFERENCE   [0-9]+.*")) {
			log.fatal( "Apparently the REFERENCE is not in the format expected, first line incorrect: "+components.get(0));
		}
		else {
			int index = 1;
			while(index<components.size()) {
				List<String> element = new ArrayList<String>();
				element.add(components.get(index));//it's the header
				if(index+1<components.size() && components.get(index+1).startsWith("            ")){//there is a multiple lines for the subpart, continue until the next one or the exit
					index++;
					while(index<components.size() && components.get(index).startsWith("            ")){
						element.add(components.get(index));
					index++;
					}					
				}
				else {//only on line we continue to the next one
					index++;	
				}
				processSubsection(element);
			}
		}
	}
	/**
	 * We have one subsection of the Reference, we process it
	 * @param element one element in multiple line
	 */
	protected void processSubsection(List<String> element){
		if(element.get(0).startsWith("  AUTHORS"))
			processAuthors(element);
		else if(element.get(0).startsWith("  TITLE"))
			processTitle(element);
		else if(element.get(0).startsWith("  JOURNAL"))
			processJournal(element);
		else if(element.get(0).startsWith("   PUBMED"))
			processPubmed(element);
		else if(element.get(0).startsWith("  REMARK")){//don't do anything for that
		}
		else if(element.get(0).startsWith("  CONSRTM")){//don't do anything for that
		}
		else if(element.get(0).startsWith("  MEDLINE")){//don't do anything for that
		}
		else{
			log.fatal( "Unknown subection in the Reference, add the code to handle it if needed: "+element.get(0).substring(0, 12));
		}
	}
	protected void processAuthors(List<String> element){
		StringBuilder out = new StringBuilder();
		out.append(element.get(0).substring(12));
		int index = 1;
		while(index<element.size()){
			out.append(element.get(index));
		index++;
		}
		authors = out.toString().replaceAll(" +", " ");
	}
	protected void processTitle(List<String> element){
		StringBuilder out = new StringBuilder();
		out.append(element.get(0).substring(12));
		int index = 1;
		while(index<element.size()){
			out.append(element.get(index));
		index++;
		}
		title = out.toString().replaceAll(" +", " ");
	}
	protected void processJournal(List<String> element){
		StringBuilder out = new StringBuilder();
		out.append(element.get(0).substring(12));
		int index = 1;
		while(index<element.size()){
			out.append(element.get(index));
		index++;
		}
		journal = out.toString().replaceAll(" +", " ");
	}
	protected void processPubmed(List<String> element){
		StringBuilder out = new StringBuilder();
		out.append(element.get(0).substring(12));
		try {
			pubmed = Integer.valueOf(out.toString());
		}catch (NumberFormatException e){
			log.fatal( "Impossible to convert the Pubmed ID in the line ["+element.get(0)+"] for accession ["+accession+"]");
		}
	}
	public Reference(String accession){
		this.accession = accession;
	}
	
	public void addTitle(String info){
		//info = InfoOld.removePunctuation(info);
		if(title == null)
			title = info;
		else
			title += " " + info;
	}
	public void addJournal(String info){
		//info = InfoOld.removePunctuation(info);
		if(journal == null)
			journal = info;
		else
			journal += " " + info;
	}
	public void addRemark(String info){
		//info = InfoOld.removePunctuation(info);
		if(remark == null)
			remark = info;
		else
			remark += " " + info;
	}
	public void addPubmed(String info){
		if(pubmed == 0)
			pubmed = Integer.parseInt(info);
		else
			log.info("error, multiple pubmedIDs. accession:"+accession);
	}
	public void addAuthors(String info){
		//info = InfoOld.removePunctuation(info);
		if(authors == null)
			authors = info;
		else
			authors += " " + info;
	}
	public void addConstrm(String info){
		//info = InfoOld.removePunctuation(info);
		if(consrtm == null)
			consrtm = info;
		else
			consrtm += " " + info;
	}
	public int getPubmed() {
		return pubmed;
	}
	public String getJournal() {
		return journal;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getAuthors() {
		return authors;
	}
	public void setAuthors(String authors) {
		this.authors = authors;
	}
}
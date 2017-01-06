package edu.asu.zoophy.genbankfactory.database;

/**
 * @author demetri
 */
public class Sequence {
	
	private String accession; //universal link to GenBank record//
	//for Sequence_Details table//
	private String definition;
	private int tax_id;
	private String organism;
	private String isolate;
	private String strain;
	private String collection_date; //leaving as a string for now to avoid conversion issues//
	private int itv_from;
	private int itv_to;
	private String comment;
	//for Sequence table//
	private String sequence;
	private int segment_length;
	//for Sequence_Publication table//
	private Publication pub;
	
	public Sequence() {
		//default//
	}

	//getters and setters//
	public String getAccession() {
		return accession;
	}

	public void setAccession(String accession) {
		this.accession = accession;
	}

	public String getDefinition() {
		return definition;
	}

	public void setDefinition(String definition) {
		this.definition = definition;
	}

	public int getTax_id() {
		return tax_id;
	}

	public void setTax_id(int tax_id) {
		this.tax_id = tax_id;
	}

	public String getOrganism() {
		return organism;
	}

	public void setOrganism(String organism) {
		this.organism = organism;
	}

	public String getIsolate() {
		return isolate;
	}

	public void setIsolate(String isolate) {
		this.isolate = isolate;
	}

	public String getStrain() {
		return strain;
	}

	public void setStrain(String strain) {
		this.strain = strain;
	}

	public String getCollection_date() {
		return collection_date;
	}

	public void setCollection_date(String collection_date) {
		this.collection_date = collection_date;
	}

	public int getItv_from() {
		return itv_from;
	}

	public void setItv_from(int itv_from) {
		this.itv_from = itv_from;
	}

	public int getItv_to() {
		return itv_to;
	}

	public void setItv_to(int itv_to) {
		this.itv_to = itv_to;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getSequence() {
		return sequence;
	}

	public void setSequence(String sequence) {
		this.sequence = sequence;
	}

	public Publication getPub() {
		return pub;
	}

	public void setPub(Publication pub) {
		this.pub = pub;
	}

	public int getSegment_length() {
		return segment_length;
	}

	public void setSegment_length(int segment_length) {
		this.segment_length = segment_length;
	}	
}
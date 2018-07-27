package edu.asu.zoophy.genbankfactory.database;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.asu.zoophy.genbankfactory.utils.locextractor.CountryNameExtracter;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBQuery;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;

/**
 * SQL implementation of the GenBankRecordDAO.
 * All SQL methods from the GenBankFactory have been migrated into this class
 * @author demetri
 */
public class GenBankRecordSqlDAO implements GenBankRecordDAOInt {
	
	private final Logger log = Logger.getLogger("GenBankRecordSqlDAO");
	
	private final String FEATURE_INSERT = "INSERT INTO \"Features\"(\"Feature_ID\",\"Accession\",\"Header\",\"Position\",\"Key\",\"Value\") VALUES(default,?,?,?,?,?);"; 
	private final String GENE_INSERT = "INSERT INTO \"Gene\"(\"Gene_ID\",\"Accession\",\"Gene_Name\",\"Itv\") VALUES(default,?,?,?);";
	private final String HOST_INSERT = "INSERT INTO \"Host\"(\"Accession\",\"Host_Name\",\"Host_taxon\") VALUES(?,?,?);";
	private final String GENBANK_LOCATION_INSERT = "INSERT INTO \"Location_GenBank\"(\"Accession\",\"Location\",\"Latitude\",\"Longitude\") VALUES(?,?,?,?);";
	private final String EMPTY_GEONAME_LOCATION_INSERT = "INSERT INTO \"Location_Geoname\"(\"Accession\") VALUES(?);";
	private final String FULL_GEONAME_LOCATION_INSERT = "INSERT INTO \"Location_Geoname\"(\"Accession\", \"Geoname_ID\", \"Location\", \"Latitude\", \"Longitude\", \"Type\", \"Country\") VALUES (?, ?, ?, ?, ?, ?, ?);";
	private final String PUBLICATION_INSERT = "INSERT INTO \"Publication\"(\"Pub_ID\",\"Pubmed_ID\",\"Pubmed_Central_ID\",\"Authors\",\"Title\",\"Journal\") VALUES(default,?,?,?,?,?);";
	private final String SEQUENCE_INSERT = "INSERT INTO \"Sequence\"(\"Accession\",\"Sequence\",\"Segment_Length\") VALUES(?,?,?);";
	private final String DETAILS_INSERT = "INSERT INTO \"Sequence_Details\"(\"Accession\",\"Definition\",\"Tax_ID\",\"Organism\",\"Isolate\",\"Strain\",\"Collection_Date\",\"Itv_From\",\"Itv_To\",\"Comment\",\"pH1N1\") VALUES(?,?,?,?,?,?,?,?,?,?,?);";
	private final String SEQUENCE_PUBLICATION_INSERT = "INSERT INTO \"Sequence_Publication\"(\"Accession\",\"Pub_ID\") VALUES(?,?);";
	private final String CHECK_PUBLICATION = "SELECT * FROM \"Publication\" WHERE \"Pubmed_ID\"=?";
	private final String RETRIEVE_DETAILS = "SELECT * FROM \"Sequence_Details\" WHERE \"Accession\"=?";
	private final String RETRIEVE_SEQUENCE = "SELECT \"Sequence\" FROM \"Sequence\" WHERE \"Accession\"=?";
	private final String RETRIEVE_HOST = "SELECT * FROM \"Host\" WHERE \"Accession\"=?";
	private final String RETRIEVE_GENES = "SELECT * FROM \"Gene\" WHERE \"Accession\"=?";
	private final String RETRIEVE_FEATURES = "SELECT * FROM \"Features\" WHERE \"Accession\"=?";
	private final String RETRIEVE_LOCATIONS = "SELECT * FROM \"Possible_Location\" WHERE \"Accession\"=?";
	private final String RETRIEVE_GENBANK_LOCATION = "SELECT * FROM \"Location_GenBank\" WHERE \"Accession\"=?";
	private final String RETRIEVE_GEONAME_LOCATION = "SELECT * FROM \"Location_Geoname\" WHERE \"Accession\"=?";
	private final String RETRIEVE_PUBLICATION = "SELECT * FROM \"Publication\" JOIN \"Sequence_Publication\" ON (\"Publication\".\"Pubmed_ID\" = \"Sequence_Publication\".\"Pub_ID\") WHERE \"Accession\"=?";
	private final String RETRIEVE_INDEX_RECORDS_BATCH = "SELECT \"Sequence_Details\".\"Accession\", \"Collection_Date\", \"Definition\", \"Tax_ID\", \"Organism\", \"Strain\", \"pH1N1\", \"Host_Name\", \"Host_taxon\", \"Geoname_ID\", \"Location\",\"Type\",\"Country\",\"Pub_ID\",\"Segment_Length\" FROM \"Sequence_Details\" JOIN \"Host\" ON \"Sequence_Details\".\"Accession\"=\"Host\".\"Accession\" JOIN \"Location_Geoname\" ON \"Sequence_Details\".\"Accession\"=\"Location_Geoname\".\"Accession\" LEFT JOIN \"Sequence_Publication\" ON \"Sequence_Details\".\"Accession\"=\"Sequence_Publication\".\"Accession\" JOIN \"Sequence\" ON \"Sequence_Details\".\"Accession\"=\"Sequence\".\"Accession\" ORDER BY \"Accession\" ASC LIMIT ? OFFSET ?";
	private final String RETRIEVE_INDEX_GENES = "SELECT \"Normalized_Gene_Name\" FROM \"Gene\" WHERE \"Accession\"=?";
	
	private final String INSTITUTION_INSERT = "INSERT INTO \"Institution\" (\"Institution\") SELECT ? WHERE NOT EXISTS (SELECT 1 FROM \"Institution\" WHERE \"Institution\"=?)";
	//Query objects defined in Davy's WipeFinder project//
	private DBQuery detailsQuery;
	private DBQuery publicationQuery;
	private DBQuery sequenceQuery;
	private DBQuery sequencePubQuery;
	private DBQuery genLocQuery;
	private DBQuery emptyGeoLocQuery;
	private DBQuery fullGeoLocQuery;
	private DBQuery hostQuery;
	private DBQuery geneQuery;
	private DBQuery featureQuery;
	private DBQuery pubCheckQuery;
	private DBQuery getDetialsQuery;
	private DBQuery getSequenceQuery;
	private DBQuery getHostQuery;
	private DBQuery getGenesQuery;
	private DBQuery getFeaturesQuery;
	private DBQuery getLocationsQuery;
	private DBQuery getGenBankLocationQuery;
	private DBQuery getGeonameLocationQuery;
	private DBQuery getPublicationQuery;
	private DBQuery getIndexRecordsQuery;
	private DBQuery getIndexGenesQuery;
	
	private DBQuery institutionQuery;
	
	// Hashmaps
	private HashMap<Publication,Integer> publicationMap = new HashMap<Publication,Integer>();
	//private HashMap<String,List<Integer>> accessionPubIdMap = new HashMap<String,List<Integer>>();
	private HashMap<Author,Integer> authorMap = new HashMap<Author,Integer>();
	private HashSet<Pair> authorIdPubId = new HashSet<Pair>();
	private HashMap<String,Integer> institutionMap = new HashMap<String,Integer>();
	private HashSet<Pair> authorInstitute = new HashSet<Pair>();
	private HashSet<Pair> submission = new HashSet<Pair>();
	private HashSet<Pair> sequencePublication = new HashSet<Pair>();
	
	
	//IDs
	private Integer pubId = 0;
	private Integer authorId = 0;
	private Integer instituteId = 0;
	
	
	/**
	 * Refresh insertion queries from the DB connection
	 * @param conn DB Connection
	 */
	private void setupInsertQueries(Connection conn) {
		try {
			sequenceQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, SEQUENCE_INSERT);
			sequencePubQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, SEQUENCE_PUBLICATION_INSERT);
			genLocQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, GENBANK_LOCATION_INSERT);
			emptyGeoLocQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, EMPTY_GEONAME_LOCATION_INSERT);
			fullGeoLocQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, FULL_GEONAME_LOCATION_INSERT);
			hostQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, HOST_INSERT);
			geneQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, GENE_INSERT);
			featureQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, FEATURE_INSERT);
			institutionQuery = new DBQuery(conn,DBQuery.QT_INSERT_BATCH,INSTITUTION_INSERT);
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error setting up queries");
		}
	}
	/**
	 * Refresh retreival queries from the DB connection
	 * @param conn DB Connection 
	 */
	private void setupRetreiveQueries(Connection conn) {
		try {
			getDetialsQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, RETRIEVE_DETAILS);
			getSequenceQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, RETRIEVE_SEQUENCE);
			getHostQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, RETRIEVE_HOST);
			getGenesQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, RETRIEVE_GENES);
			getFeaturesQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, RETRIEVE_FEATURES);
			getLocationsQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, RETRIEVE_LOCATIONS);
			getPublicationQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, RETRIEVE_PUBLICATION);
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error setting up queries");
		}
	}
	@Override
	public void dumpRecords(List<GenBankRecord> parsedRecords) {
		Connection conn = null;
		try {
			int batchSize = 0;
			//need to take care of all of these first, since Accession is a FKey for everything//
			log.info("Inserting " + parsedRecords.size() + " records");
			insertSequenceDetails(parsedRecords);
			//taking care of Publications since they are also used as FKeys//
			//insertPublications(parsedRecords);
			// insert new tables Author etc
			insertNewTables(parsedRecords);
			//setup DB connection//
			conn = getDriver();
			//setup prepared statements//
			setupInsertQueries(conn);
			//go through list of records and insert via batches of 1000 records// 
			/*for (int i = 0; i < parsedRecords.size(); i++) {
				try {
					addSequence(parsedRecords.get(i).getSequence());
					addGenes(parsedRecords.get(i).getGenes());
					addFeatures(parsedRecords.get(i).getFeatures());
					addHost(parsedRecords.get(i).getHost());
					addGenBankLocation(parsedRecords.get(i).getGenBankLocation());
					addGeonameLocation(parsedRecords.get(i).getGeonameLocation(), parsedRecords.get(i).getAccession());
					//addInstitute(parsedRecords.get(i).getInstitute());
					batchSize++;
					if (batchSize == 50000 || (i == parsedRecords.size()-1 && batchSize > 0)) {
						executeMainBatch();
						batchSize = 0;
						//log.info("Main Batch inserted");
						setupInsertQueries(conn);
					}
				}
				catch (Exception e) {
					log.log(Level.SEVERE, "INSERTION ERROR: " + e.getMessage()); 
				}
			}*/
			log.info("Main Insertion successfully completed");
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error Inserting Records: " + e.getMessage());
		}
		finally {
			//close DB connection//
			closeConn(conn);
		}
	}

	/**
	 * Inserts all of the Sequence_Details, which is critical for Foreign Key constraints
	 * @param parsedRecords
	 * @throws Exception
	 */
	private void insertSequenceDetails(List<GenBankRecord> parsedRecords) throws Exception {
		Connection conn = null;
		int batchSize = 0;
		try {
			conn = getDriver();
			detailsQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, DETAILS_INSERT);
			Sequence seq;
			List<Object> queryParams;
			for (int i = 0; i < parsedRecords.size(); i++) {
					seq = parsedRecords.get(i).getSequence();
					queryParams = new LinkedList<Object>();
					//add data to sequence_details batch//
					queryParams.add(seq.getAccession().trim());
					queryParams.add(seq.getDefinition().trim());
					queryParams.add(seq.getTax_id());
					queryParams.add(seq.getOrganism());
					queryParams.add(seq.getIsolate());
					queryParams.add(seq.getStrain());
					queryParams.add(seq.getCollection_date());
					queryParams.add(seq.getItv_from());
					queryParams.add(seq.getItv_to());
					queryParams.add(seq.getComment());
					queryParams.add(seq.isPH1N1());
					detailsQuery.addBatch(queryParams);
					seq = null;
					queryParams.clear();
					batchSize++;
					//log.info("Sequence " + seq.getAccession() + "successfully added to the batch");
					if (batchSize == 1000 || (i == parsedRecords.size()-1 && batchSize > 0)) {
						detailsQuery.executeBatch();
						detailsQuery.close();
						//log.info("Details Batch inserted");
						batchSize = 0;
						detailsQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, DETAILS_INSERT);
					}
			}
			log.info("Sequence Details successfully inserted");
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "ERROR INSERTING SEQUENCE_DETAILS: " + e.getMessage());
			throw new Exception("ERROR INSERTING SEQUENCE_DETAILS");
		} 
		finally {
			//close connection//
			closeConn(conn);
		}
	}
	
	private void insertNewTables(List<GenBankRecord> parsedRecords) {
		Connection conn = null;
		/*String SELECTION_PUBLICATION = "SELECT \"Pub_ID\" from \"Publication\" where \"Pubmed_ID\" = ? and Lower(\"Title\") = ? and Lower(\"Journal\") = ? ";
		String INSERT_PUBLICATION = "Insert into \"Publication\"(\"Pubmed_ID\",\"Pubmed_Central_ID\", \"Title\", \"Journal\")  VALUES(?,?,?,?) Returning \"Pub_ID\" ";
		String SELECTION_AUTHOR = "SELECT \"Author_ID\" from \"Author\" where Lower(\"Last_Name\") = ? and Lower(\"Initials\") = ? ";
		String INSERT_AUTHOR = "INSERT INTO \"Author\"(\"First_Name\",\"Initials\",\"Last_Name\") VALUES(?,?,?) Returning \"Author_ID\" ";
		String SELECT_AUTHOR_PUBLICATION = "SELECT \"Author_ID\" from \"Author_Publication\" where \"Author_ID\" = ? and \"Pub_ID\" = ? ";
		String INSERT_AUTHOR_PUBLICATION = "INSERT INTO \"Author_Publication\" VALUES(?,?)";
		String INSERT_INSTITUTION = "INSERT INTO \"Institution\" (\"Institution\",\"Country\") VALUES(?,?) Returning \"Institution_ID\" ";
		String SELECTION_INSTITUTION = "Select \"Institution_ID\" from \"Institution\" where Lower(\"Institution\") = ? ";
		String SELECT_AUTHOR_INSTITUTION = "SELECT \"Author_ID\" from \"Author_Institution\" where \"Author_ID\" = ? and \"Institution_ID\" = ?";
		String INSERT_AUTHOR_INSTITUTION = "INSERT INTO \"Author_Institution\" VALUES (?,?,?) " ;
		String INSERT_SUBMISSION = "INSERT INTO \"Submission\" VALUES(?,?);";
		String INSERT_SEQUENCE_PUBLICATION = "INSERT INTO \"Sequence_Publication\" VALUES(?,?);";
		CountryNameExtracter countryNameExtracter = new CountryNameExtracter();*/
		
		try {
			conn = getDriver();
			
			
			for ( int i = 0; i < parsedRecords.size(); i++ ) {
				
				// insert Publications
				
				List<Publication> pubs =  parsedRecords.get(i).getSequence().getPub();
				List<Integer> perAccessionPubIds = new ArrayList<Integer>();
				for (int j = 0; j < pubs.size() ; j++) {
					
					if ( publicationMap.containsKey(pubs.get(j))) {
						perAccessionPubIds.add( publicationMap.get(pubs.get(j)));
						
							
					}
					else {
						publicationMap.put(pubs.get(j), ++pubId);
						perAccessionPubIds.add(pubId);
					}
					
				}
				
				// insert Authors and  Author_Publication
				List<List<Author>> allAuthorsofAccession = parsedRecords.get(i).getAuthorList();
				// if authors are empty skip inserting Author, Author_Publication, Institution, Author_Institution, Submission
				if ( allAuthorsofAccession != null & allAuthorsofAccession.size() != 0 ) {
				
						for (int m = 0; m < perAccessionPubIds.size(); m++) {
							// iterating over the authors of mth publication
							for(int k = 0; k < allAuthorsofAccession.get(m).size(); k++) {
								
								
								if (authorMap.containsKey( allAuthorsofAccession.get(m).get(k) )) {
									Pair pair = new Pair(authorMap.get(allAuthorsofAccession.get(m).get(k)), perAccessionPubIds.get(m) );
									authorIdPubId.add(pair);
								}
								else {
									authorMap.put(allAuthorsofAccession.get(m).get(k), ++authorId);
									Pair pair = new Pair(authorId, perAccessionPubIds.get(m) );
									authorIdPubId.add(pair);
									
								}
							}
			
						}
						// first author of direct submission titled reference
						Integer firstAuhorSubmission;
						try {
							if (authorMap.containsKey( allAuthorsofAccession.get(allAuthorsofAccession.size()-1).get(0)) ) {
								firstAuhorSubmission = authorMap.get( allAuthorsofAccession.get(allAuthorsofAccession.size()-1).get(0) ) ;
							}
							else {
								authorMap.put(allAuthorsofAccession.get(allAuthorsofAccession.size()-1).get(0) , ++authorId);
								firstAuhorSubmission = authorId;
							}
						} catch (Exception e) {
							// TODO Auto-generated catch block
							System.out.println("Accession: "+ parsedRecords.get(i).getAccession() );
							for (List<Author> alist :allAuthorsofAccession)
								System.out.println(alist);
							
							firstAuhorSubmission = 10;
							e.printStackTrace();
						}
							
						
						// insert Institution
						Integer perAccessionInstituteId;
						if ( institutionMap.containsKey(parsedRecords.get(i).getInstitute()) ) {
							perAccessionInstituteId = institutionMap.get(parsedRecords.get(i).getInstitute());
						} 
						else {
							institutionMap.put(parsedRecords.get(i).getInstitute(), ++instituteId);
							perAccessionInstituteId = instituteId;
						}
						
						// insert Author_Institution  TODO: add collection date insertAuthorInstStmt.setString(3, parsedRecords.get(i).getSubmissionDate());
						Pair pairAuthInst = new Pair(firstAuhorSubmission,perAccessionInstituteId );
						
						if (!authorInstitute.contains(pairAuthInst )) {
							authorInstitute.add(pairAuthInst);
						}
						
						// insert Submission
						Pair pairSubmission = new Pair(firstAuhorSubmission, parsedRecords.get(i).getAccession());
						if (!submission.contains(pairSubmission)) {
							submission.add(pairSubmission);
						}
				}		
								
				// insert in Sequence_Publication
				for (Integer temp: perAccessionPubIds) {
					Pair localPair = new Pair(parsedRecords.get(i).getAccession(), temp);
					
					if (!sequencePublication.contains(localPair)) {
						sequencePublication.add(localPair);
					}
				}
				
			}
			
			/* debug
			System.out.println(" *Publication* "+ Arrays.asList( publicationMap));
			System.out.println(" *Author* "+Arrays.asList( authorMap));
			System.out.println(" *Institution* " + Arrays.asList( institutionMap));*/
			
			/*System.out.println(Arrays.asList( authorIdPubId));
			System.out.println(Arrays.asList( authorIdPubId));
			System.out.println(Arrays.asList( submission));
			System.out.println(Arrays.asList( sequencePublication));
			*/
			// Inserting records in database
			PreparedStatement preparedStatement;
			//
			String INSERT_PUBLICATION_BATCH = "Insert into \"Publication\"(\"Pub_ID\", \"Pubmed_ID\",\"Pubmed_Central_ID\", \"Title\", \"Journal\")  VALUES(?,?,?,?,?) ";
			preparedStatement = conn.prepareStatement(INSERT_PUBLICATION_BATCH);
			
			for (Publication pubkey: publicationMap.keySet()) {
				preparedStatement.setInt(1,publicationMap.get(pubkey) );
	            preparedStatement.setInt(2, pubkey.getPubmedId());
	            preparedStatement.setString(3, pubkey.getCentralId());
	            preparedStatement.setString(4, pubkey.getTitle());
	            preparedStatement.setString(5, pubkey.getJournal());
	            preparedStatement.addBatch();
			}
			int[] inserted = preparedStatement.executeBatch();
			//
			String INSERT_AUTHOR_BATCH = "INSERT INTO \"Author\"(\"Author_ID\", \"First_Name\",\"Initials\",\"Last_Name\") VALUES(?,?,?,?) ";
			
			preparedStatement = conn.prepareStatement(INSERT_AUTHOR_BATCH);
			
			for(Author author: authorMap.keySet()) {
				preparedStatement.setInt(1, authorMap.get(author));
				preparedStatement.setString(2, author.getFirstName());
				preparedStatement.setString(3, author.getInitial());
				preparedStatement.setString(4, author.getLastName());
				preparedStatement.addBatch();
				
			}
			int [] authorInserted = preparedStatement.executeBatch();
			//
			String INSERT_AUTHOR_PUBLICATION_BATCH = "INSERT INTO \"Author_Publication\" VALUES(?,?)";
			
			preparedStatement = conn.prepareStatement(INSERT_AUTHOR_PUBLICATION_BATCH);
			
			for(Pair authPubPair: authorIdPubId) {
				preparedStatement.setInt(1, (Integer)authPubPair.getLeft());
				preparedStatement.setInt(2, (Integer)authPubPair.getRight());
				preparedStatement.addBatch();
				
			}
			int [] authorPublicationInserted = preparedStatement.executeBatch();
			
			
			
			String INSERT_INSTITUTION_BATCH = "INSERT INTO \"Institution\" (\"Institution_ID\", \"Institution\") VALUES(?,?)  ";
			
			preparedStatement = conn.prepareStatement(INSERT_INSTITUTION_BATCH);
			
			for(String name: institutionMap.keySet()) {
				preparedStatement.setInt(1, institutionMap.get(name));
				preparedStatement.setString(2, name);
				preparedStatement.addBatch();
				
			}
			int [] institutionInserted = preparedStatement.executeBatch();
			
			
			String INSERT_AUTHOR_INSTITUTION_BATCH = "INSERT INTO \"Author_Institution\" (\"Author_ID\", \"Institution_ID\" )  VALUES (?,?) " ;
			
			preparedStatement = conn.prepareStatement(INSERT_AUTHOR_INSTITUTION_BATCH);
			
			for(Pair authorInstitute: authorInstitute) {
				preparedStatement.setInt(1, (Integer)authorInstitute.getLeft());
				preparedStatement.setInt(2, (Integer)authorInstitute.getRight());
				preparedStatement.addBatch();
				
			}
			int [] authorInstituteInserted = preparedStatement.executeBatch();
			
			//
			String INSERT_SUBMISSION_BATCH = "INSERT INTO \"Submission\" VALUES(?,?);";
			
			preparedStatement = conn.prepareStatement(INSERT_SUBMISSION_BATCH);
			
			for(Pair sub: submission) {
				preparedStatement.setInt(1, (Integer)sub.getLeft());
				preparedStatement.setString(2, (String)sub.getRight());
				preparedStatement.addBatch();
				
			}
			int [] submissionInserted = preparedStatement.executeBatch();
			
			//
			String INSERT_SEQUENCE_PUBLICATION_BATCH = "INSERT INTO \"Sequence_Publication\" VALUES(?,?);";
			
			preparedStatement = conn.prepareStatement(INSERT_SEQUENCE_PUBLICATION_BATCH);
			
			for(Pair seqPub: sequencePublication) {
				preparedStatement.setString(1, (String)seqPub.getLeft());
				preparedStatement.setInt(2, (Integer)seqPub.getRight());
				preparedStatement.addBatch();
				
			}
			int [] seqPubInserted = preparedStatement.executeBatch();
			
			System.out.println(" ^^HashMap Sizes ^^   \n"
					+ "Publication Map: " + publicationMap.size() + "\n"
					+ "Author Map: " + authorMap.size() + "\n"
					+ "AuthorPub Map: " + authorIdPubId.size() + "\n"
					+ "Inst Map: " + institutionMap.size() + "\n"
					+ "AuthorInst Map: " + authorInstitute.size() + "\n"
					+ "Sub Map: " + submission.size() + "\n"
					+ "SeqPub Map: " + sequencePublication.size() + "\n");
			
			// clear all the hashmap and hashsets
			publicationMap.clear();
			authorMap.clear();
			authorIdPubId.clear();
			institutionMap.clear();
			authorInstitute.clear();
			submission.clear();
			sequencePublication.clear();
			
		}catch (Exception e) {
			e.printStackTrace();
			log.log(Level.SEVERE, "Error inserting New tables: " + e.getMessage());
		}finally {
			//close connection//
			closeConn(conn);
		}
	}
	
	/**
	 * Inserts all of the Publications to satisfy Foreign Key constraints
	 * @param parsedRecords
	 */
	private void insertPublications(List<GenBankRecord> parsedRecords) {
		Connection conn = null;
		int batchSize = 0;
		try {
			conn = getDriver();
			//need to track pubmedIDs from the same batch before insertion//
			ArrayList<Integer> usedIds = new ArrayList<Integer>();
			List<Object> queryParams;
			publicationQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, PUBLICATION_INSERT);
			for (int i = 0; i < parsedRecords.size(); i++) {
				try {
					Publication pub = parsedRecords.get(i).getSequence().getPub().get(0);
					if (pub != null) {
						//avoiding duplicat records//
						pubCheckQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, CHECK_PUBLICATION);
						if (!PubExists(pub.getPubmedId()) && !usedIds.contains(pub.getPubmedId())) {
							queryParams = new LinkedList<Object>();
							queryParams.add(pub.getPubmedId());
							usedIds.add(pub.getPubmedId());
							queryParams.add(pub.getCentralId());
							queryParams.add(pub.getAuthors());
							queryParams.add(pub.getTitle());
							queryParams.add(pub.getJournal());
							publicationQuery.addBatch(queryParams);
							queryParams.clear();
							batchSize++;
						}
					}
					if (batchSize == 1000 || (i == parsedRecords.size()-1 && batchSize > 0)) {
						publicationQuery.executeBatch();
						publicationQuery.close();
						//log.info("Publication Batch inserted");
						batchSize = 0;
						publicationQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, PUBLICATION_INSERT);
					}
				}
				catch (Exception e) {
					log.log(Level.SEVERE, "Error adding Publicaton for " + parsedRecords.get(i).getAccession() + ": " + e.getMessage());
				}
			}
			log.info("Publications successfully inserted");
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error inserting Publications: " + e.getMessage());
		}
		finally {
			//close connection//
			closeConn(conn);
		}
	}
	/**
	 * Adds data for Sequence, and Sequence_Publication to batch
	 * @param sequence
	 */
	private void addSequence(Sequence seq) {
		try {
			List<Object> queryParams = new LinkedList<Object>();
			//add data to sequence batch//
			queryParams.add(seq.getAccession().trim());
			queryParams.add(seq.getSequence().trim());
			queryParams.add(seq.getSequence().trim().length());//segment length//
			sequenceQuery.addBatch(queryParams);
			queryParams.clear();
			//add data to sequence_publication batch//
			/*if (seq.getPub() != null) {
				queryParams.add(seq.getAccession().trim());
				queryParams.add(seq.getPub().getPubmedId());
				sequencePubQuery.addBatch(queryParams);
				queryParams.clear();
			}*/
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error adding Sequence " + seq.getAccession() + ": " + e.getMessage());
		}
	}
	/**
	 * Checks to see if the Publication is already in the DB, to prevent duplicating inserts
	 * @param pubmedId
	 * @return true if the publication is already in the DB
	 */
	private boolean PubExists(int pubmedId) {
		try {
			List<Object> queryParams = new LinkedList<Object>();
			queryParams.add(pubmedId);
			pubCheckQuery.addBatch(queryParams);
			ResultSet rs = pubCheckQuery.executeSelect_MultiRows();
			if (!rs.isBeforeFirst()){
				//ResultSet is empty
				return false;
			}
			queryParams.clear();
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error Checking Publication: " + e.getMessage());
		}
		return true;
	}
	/**
	 * This method executes the batch, dumping info into the database, then calling setupQueries()
	 */
	public void executeMainBatch() {
		//log.info("executing main batch"); 
		try {
			geneQuery.executeBatch();
			geneQuery.close();
		}
		catch(SQLException se) {
			printSQLError(se);
		}
		try {
			hostQuery.executeBatch();
			hostQuery.close();
		}
		catch(SQLException se) {
			printSQLError(se);
		}
		try {
			genLocQuery.executeBatch();
			genLocQuery.close();
		}
		catch(SQLException se) {
			printSQLError(se);
		}
		try {
			emptyGeoLocQuery.executeBatch();
			emptyGeoLocQuery.close();
		}
		catch(SQLException se) {
			printSQLError(se);
		}
		try {
			fullGeoLocQuery.executeBatch();
			fullGeoLocQuery.close();
		}
		catch(SQLException se) {
			printSQLError(se);
		}
		try {
			sequenceQuery.executeBatch();
			sequenceQuery.close();
		}
		catch(SQLException se) {
			printSQLError(se);
		}
		/*try {
			sequencePubQuery.executeBatch();
			sequencePubQuery.close();
		}
		catch(SQLException se) {
			printSQLError(se);
		}*/
		try {
			featureQuery.executeBatch();
			featureQuery.close();
		}
		catch(SQLException se) {
			printSQLError(se);
		}
		
		try {
			institutionQuery.executeBatch();
			institutionQuery.close();
		}
		catch(SQLException se) {
			printSQLError(se);
		}
		
		
		//log.info("Main Batch Executed");
	}
	
	private void addInstitute(String institute) {
		
		try {
			List<Object> queryParams = new LinkedList<Object>();
			queryParams.add(institute);
			queryParams.add(institute);
			institutionQuery.addBatch(queryParams);
		} catch (SQLException e) {
			log.log(Level.SEVERE, "Error adding Institute: " + e.getMessage());
			e.printStackTrace();
		}
		
		
		
	}
	
	
	/**
	 * Adds data for Genes to the Gene batch
	 * @param genes
	 */
	private void addGenes(List<Gene> genes) {
		try {
			List<Object> queryParams = new LinkedList<Object>();
			for (Gene gen : genes) {
				gen.setName(gen.formatName(gen.getName()));
				queryParams.add(gen.getAccession().trim());
				queryParams.add(gen.getName().trim());
				queryParams.add(gen.getItv().trim());
				geneQuery.addBatch(queryParams);
				queryParams.clear();
			}
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error adding Genes: " + e.getMessage());
		}
	}
	/**
	 * Adds data for Host to the Host batch
	 * @param host
	 */
	private void addHost(Host host) {
		try {
			List<Object> queryParams = new LinkedList<Object>();
			queryParams.add(host.getAccession().trim());
			queryParams.add(host.getName());
			queryParams.add(host.getTaxon());
			hostQuery.addBatch(queryParams);
			queryParams.clear();
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error adding Host: " + e.getMessage());
		}
	}
	/**
	 * Adds data for features to the Feature batch
	 * @param features
	 */
	private void addFeatures(List<Feature> features) {
		try {
			List<Object> queryParams = new LinkedList<Object>();
			for (Feature feat : features) {
				queryParams.add(feat.getAccession().trim());
				queryParams.add(feat.getHeader().trim());
				queryParams.add(feat.getPosition().trim());
				queryParams.add(feat.getKey().trim());
				queryParams.add(feat.getValue().trim());
				featureQuery.addBatch(queryParams);
				queryParams.clear();
			}
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error adding Features: " + e.getMessage());
		}
	}
	/**
	 * Adds data for Possible Locations
	 * @param loc GenBankLocation 
	 */
	private void addGenBankLocation(PossibleLocation loc) {
		try {
			if (loc != null) {
				List<Object> queryParams = new LinkedList<Object>();
				queryParams.add(loc.getAccession().trim());
				queryParams.add(loc.getLocation());
				queryParams.add(loc.getLatitude());
				queryParams.add(loc.getLongitude());
				genLocQuery.addBatch(queryParams);
				queryParams.clear();
			}
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error adding Genbank Location: " + e.getMessage());
		}
	}
	
	private void addGeonameLocation(PossibleLocation geonameLocation, String accession) {
		try {
			List<Object> queryParams = new LinkedList<Object>();
			if (geonameLocation != null) {
				queryParams.add(geonameLocation.getAccession().trim());
				queryParams.add(geonameLocation.getId());
				queryParams.add(geonameLocation.getLocation());
				queryParams.add(geonameLocation.getLatitude());
				queryParams.add(geonameLocation.getLongitude());
				queryParams.add(geonameLocation.getType());
				queryParams.add(geonameLocation.getCountry());
				fullGeoLocQuery.addBatch(queryParams);
				queryParams.clear();
			}
			else {
				queryParams.add(accession.trim());
				emptyGeoLocQuery.addBatch(queryParams);
				queryParams.clear();
			}
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error adding Genbank Location: " + e.getMessage());
		}
	}
	
	@Override
	public GenBankRecord getRecord(String accession) {
		GenBankRecord record = new GenBankRecord();
		record.setAccession(accession);
		Connection conn = null;
		try {
			//setup DB connection//
			conn = getDriver();
			setupRetreiveQueries(conn);
			Sequence seq = findSequenceDetails(accession);
			seq.setSequence(findSequence(accession));
			seq.setSegment_length(seq.getSequence().length());
			//seq.setPub(findPublication(accession));
			record.setSequence(seq); 
			record.setHost(findHost(accession));
			PossibleLocation genBankLoc = findGenBankLocation(accession);
			PossibleLocation geonameLoc = findGeonameLocation(accession);
			record.setGenBankLocation(genBankLoc);
			record.setGeonameLocation(geonameLoc);
			record.setGenes(findGenes(accession));
			record.setFeatures(findFeatures(accession));
			record.setPossLocations(findPossLocations(accession));
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error pulling record " + accession + ": " + e.getMessage());
			if (record.getSequence() == null) {
				record = null;//don't want to return a hollow record//
			}
		}
		finally {
			//close DB connection//
			closeConn(conn);
		}
		return record;
	}
	/**
	 * @param accession Unique Record Accession
	 * @return Corresponding details for that record
	 * @throws Exception 
	 */
	private Sequence findSequenceDetails(String accession) throws Exception {
		Sequence seq = new Sequence();
		List<Object> queryParams = new LinkedList<Object>();
		try {
			queryParams.add(accession.trim());
			getDetialsQuery.addBatch(queryParams);
			ResultSet rs = null;
			try {
				rs = getDetialsQuery.executeSelectedRow();
			}
			catch (Exception e) {
				throw new Exception ("Cannot find record: " + accession);
			}
			queryParams.clear();
			seq.setAccession(rs.getString("Accession"));
			seq.setCollection_date(rs.getString("Collection_Date"));
			seq.setComment(rs.getString("Comment"));
			seq.setDefinition(rs.getString("Definition"));
			seq.setIsolate(rs.getString("Isolate"));
			seq.setOrganism(rs.getString("Organism"));
			seq.setStrain((rs.getString("Strain")));
			seq.setTax_id(rs.getInt("Tax_ID"));
			seq.setItv_from(rs.getInt("Itv_From"));
			seq.setItv_to(rs.getInt("Itv_To"));
			seq.setPH1N1(rs.getBoolean("pH1N1"));
			rs.close();
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "There was an error mapping the SequenceDetails");
			throw e;//stop the retreival process//
		}
		return seq;
	}
	/**
	 * @param accession Unique Record Accession
	 * @return Corresponding DNA/RNA Sequence for that record
	 */
	private String findSequence(String accession) {
		String sequence = null;
		List<Object> queryParams = new LinkedList<Object>();
		try {
			queryParams.add(accession.trim());
			getSequenceQuery.addBatch(queryParams);
			ResultSet rs = getSequenceQuery.executeSelectedRow();
			queryParams.clear();
			sequence = rs.getString("Sequence");
			rs.close();
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "There was an error mapping the Sequence: " + e.getMessage());
		}
		return sequence;
	}
	/**
	 * @param accession Unique Record Accession
	 * @return Corresponding publication for that record
	 */
	private Publication findPublication(String accession) {
		Publication pub = null;
		List<Object> queryParams = new LinkedList<Object>();
		try {
			queryParams.add(accession.trim());
			getPublicationQuery.addBatch(queryParams);
			ResultSet rs = getPublicationQuery.executeSelect_MultiRows();
			queryParams.clear();
			if (rs.next()) { //some records don't have pubmed associations in GenBank//
				pub = new Publication();
				pub.setPubId(rs.getInt(("Pub_ID")));
				pub.setPubmedId(rs.getInt("Pubmed_ID"));
				pub.setCentralId(rs.getString("Pubmed_Central_ID"));
				pub.setAuthors(rs.getString("Authors"));
				pub.setTitle(rs.getString("Title"));
				pub.setJournal(rs.getString("Journal"));
			}
			rs.close();
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "There was an error mapping the Publication: " + e.getMessage());
		}
		return pub;
	}
	/**
	 ** @param accession Unique Record Accession
	 * @return Corresponding Host for that record
	 */
	private Host findHost(String accession) {
		Host host = new Host();
		host.setAccession(accession);
		List<Object> queryParams = new LinkedList<Object>();
		try {
			queryParams.add(accession.trim());
			getHostQuery.addBatch(queryParams);
			ResultSet rs = getHostQuery.executeSelectedRow();
			queryParams.clear();
			host.setName(rs.getString("Host_Name"));
			host.setTaxon(rs.getInt("Host_Taxon"));
			rs.close();
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "There was an error mapping the Host: " + e.getMessage());
		}
		return host;
	}
	/**
	 * @param accession Unique Record Accession
	 * @return GenBankLocation for the record
	 */
	public PossibleLocation findGenBankLocation(String accession) {
		PossibleLocation genBankLoc = null;
		List<Object> queryParams = new LinkedList<Object>();
		try {
			getGenBankLocationQuery = new DBQuery(getDriver(), DBQuery.QT_INSERT_BATCH, RETRIEVE_GENBANK_LOCATION);
			queryParams.add(accession.trim());
			getGenBankLocationQuery.addBatch(queryParams);
			ResultSet rs = getGenBankLocationQuery.executeSelect_MultiRows();
			queryParams.clear();
			if (rs.next()) { //some records don't have locations in GenBank//
				genBankLoc = new PossibleLocation();
				genBankLoc.setAccession(accession);
				genBankLoc.setId(0);
				genBankLoc.setLocation(rs.getString("Location"));
				genBankLoc.setLatitude(rs.getDouble("Latitude"));
				genBankLoc.setLongitude(rs.getDouble("Longitude"));
			}
			rs.close();
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "There was an error mapping the GenBankLocation: " + e.getMessage());
		}
		return genBankLoc;
	}
	
	/**
	 * @param accession Unique Record Accession
	 * @return GenBankLocation for the record
	 */
	public PossibleLocation findGeonameLocation(String accession) {
		PossibleLocation genBankLoc = null;
		List<Object> queryParams = new LinkedList<Object>();
		try {
			getGeonameLocationQuery = new DBQuery(getDriver(), DBQuery.QT_INSERT_BATCH, RETRIEVE_GEONAME_LOCATION);
			queryParams.add(accession.trim());
			getGeonameLocationQuery.addBatch(queryParams);
			ResultSet rs = getGeonameLocationQuery.executeSelect_MultiRows();
			queryParams.clear();
			if (rs.next()) { //some records don't have locations//
				genBankLoc = new PossibleLocation();
				genBankLoc.setAccession(accession);
				genBankLoc.setId(rs.getLong("Geoname_ID"));
				genBankLoc.setLocation(rs.getString("Location"));
				genBankLoc.setLatitude(rs.getDouble("Latitude"));
				genBankLoc.setLongitude(rs.getDouble("Longitude"));
				genBankLoc.setType(rs.getString("Type"));
				genBankLoc.setCountry(rs.getString("Country"));
			}
			rs.close();
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "There was an error mapping the GenBankLocation: " + e.getMessage());
		}
		return genBankLoc;
	}
	
	/**
	 * @param accession Unique Record Accession
	 * @return Corresponding Genes for that record
	 */
	private List<Gene> findGenes(String accession) {
		List<Gene> genes = new LinkedList<Gene>();
		List<Object> queryParams = new LinkedList<Object>();
		try {
			queryParams.add(accession.trim());
			getGenesQuery.addBatch(queryParams);
			ResultSet rs = getGenesQuery.executeSelect_MultiRows();
			queryParams.clear();
			while (rs.next()) {
				Gene gen = new Gene();
				gen.setAccession(accession);
				gen.setId(rs.getLong("Gene_ID"));
				gen.setItv(rs.getString("Itv"));
				gen.setName(rs.getString("Gene_Name"));
				genes.add(gen);
			}
			rs.close();
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "There was an error mapping the Genes: " + e.getMessage());
		}
		return genes;
	}
	/**
	 * @param accession Unique Record Accession
	 * @return Corresponding Features for that record
	 */
	private List<Feature> findFeatures(String accession) {
		List<Feature> features = new LinkedList<Feature>();
		List<Object> queryParams = new LinkedList<Object>();
		try {
			queryParams.add(accession.trim());
			getFeaturesQuery.addBatch(queryParams);
			ResultSet rs = getFeaturesQuery.executeSelect_MultiRows();
			queryParams.clear();
			while (rs.next()) {
				Feature feat = new Feature();
				feat.setAccession(accession);
				feat.setId(rs.getLong("Feature_ID"));
				feat.setHeader(rs.getString("Header"));
				feat.setPosition(rs.getString("Position"));
				feat.setKey(rs.getString("Key"));
				feat.setValue(rs.getString("Value"));
				features.add(feat);
			}
			rs.close();
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "There was an error mapping the Features: " + e.getMessage());
		}
		return features;
	}
	/**
	 * @param accession Unique Record Accession
	 * @return Corresponding Possible Locations for that record
	 */
	private List<PossibleLocation> findPossLocations(String accession) {
		List<PossibleLocation> possLocs = new LinkedList<PossibleLocation>();
		List<Object> queryParams = new LinkedList<Object>();
		try {
			queryParams.add(accession.trim());
			getLocationsQuery.addBatch(queryParams);
			ResultSet rs = getLocationsQuery.executeSelect_MultiRows();
			queryParams.clear();
			while (rs.next()) {
				PossibleLocation possLoc = new PossibleLocation();
				possLoc.setId(rs.getLong("Id"));
				possLoc.setAccession(accession);
				possLoc.setLocation(rs.getString("Location"));
				possLoc.setLatitude(rs.getDouble("Latitude"));
				possLoc.setLongitude(rs.getDouble("Longitude"));
				possLoc.setProbability(rs.getDouble("probability"));
				possLocs.add(possLoc);
			}
			rs.close();
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "There was an error mapping the Possible Locations: " + e.getMessage());
		}
		return possLocs;
	}
	/**
	 * This method gets connection on the postgres table
	 */
	public Connection getDriver() throws Exception {	
        try {
        	//get the central connection used by the application//
        	Connection conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
            return conn;
        } 
        catch (Exception e) {
            log.log(Level.SEVERE, "error getting connection: "+e.getMessage());
            throw new Exception("error getting connection: "+e.getMessage());
        }
    }
	/**
	 * This method closes the connection to the database
	 */
	public void closeConn(Connection conn) {
		conn = null; //The connection is shared by everything in the project, so conn.Close() breaks stuff//
	}
	/**
	 * This method deletes all info from every table in the database
	 */
	@Override
	public void clearTables() throws Exception {
		Connection conn = null;
		try {
			//setup DB connection//
			conn = getDriver();
			Statement st = conn.createStatement();
			log.warning("RESETING DATABASE");
			st.executeUpdate("TRUNCATE \"Sequence_Details\" CASCADE;");
			st.executeUpdate("TRUNCATE \"Publication\" CASCADE;");
			st.executeUpdate("TRUNCATE \"Taxonomy_Concept\" CASCADE;");
			st.executeUpdate("TRUNCATE \"Taxonomy_Division\" CASCADE;");
			st.executeUpdate("TRUNCATE \"Taxonomy_Tree\" CASCADE;");
			st.executeUpdate("ALTER SEQUENCE \"Features_Feature_ID_seq\" RESTART WITH 1");
			st.executeUpdate("ALTER SEQUENCE \"Publication_Pub_ID_seq\" RESTART WITH 1");
			st.executeUpdate("ALTER SEQUENCE \"Gene_Gene_ID_seq\" RESTART WITH 1");
			log.info("Database successfully reset");
			st.close();
		} 
		catch (SQLException e) {
			log.log(Level.SEVERE, "Impossible to truncate the tables: " + e.getMessage());
			throw new Exception("Impossible to truncate the tables: " + e.getMessage());
		}
		finally {
			//close DB connection//
			closeConn(conn);
		}
	}
	/**
	 * Prints out SQL Errors nicely
	 * @param se SQLException
	 */
	public void printSQLError(SQLException se) {
		int count = 1;
		while (se != null) {
			log.log(Level.SEVERE, "SQLException " + count);
			log.log(Level.SEVERE, "Code: " + se.getErrorCode());
			log.log(Level.SEVERE, "SqlState: " + se.getSQLState());
			log.log(Level.SEVERE, "Error Message: " + se.getMessage());
			se = se.getNextException();
			count++;
		}
	}
	@Override
	public void createTables() throws Exception {
		Connection conn = null;
		try {
			conn = getDriver();
			Statement st = conn.createStatement();
			log.info("Creating table...");
			String db_setup_query = "";
			//does not work when packaged in a .jar//
			File ddl = new File(System.getProperty("user.dir") + "/src/genBankDdl.sql");
			if (ddl.isFile()) { //running as a regular java project not a .jar //
				Scanner scan = new Scanner(ddl);
				scan.useDelimiter("\\Z");
				db_setup_query = scan.next().trim();
				scan.close();
			}
			else { //reading from .jar//
				//log.info("starting ddl read from .jar");
				InputStream in = getClass().getResourceAsStream("/genBankDdl.sql");
				BufferedReader input = new BufferedReader(new InputStreamReader(in));
				String line = input.readLine() + "\n";
			    while (line != null) {
			        db_setup_query += line + "\n";
			        line = input.readLine();
			    }
			    db_setup_query = db_setup_query.trim();
			    //log.info("query = " + db_setup_query);
			}
			st.executeUpdate(db_setup_query);
			st.close();
			log.info("Tables have been successfully created.");
		}
		catch (Exception e) {
			throw new Exception("DB could not be created. " + e.getMessage());
		}
		finally {
			closeConn(conn);
		}
	}
	
	@Override
	public List<GenBankRecord> getIndexableRecords(long limit, long offset) throws Exception {
		List<GenBankRecord> records = new LinkedList<GenBankRecord>();
		Connection conn = null;
		List<Object> queryParams = new LinkedList<Object>();
		ResultSet rs;
		try {
			conn = getDriver();
			getIndexRecordsQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, RETRIEVE_INDEX_RECORDS_BATCH);
			queryParams.add(limit);
			queryParams.add(offset);
			getIndexRecordsQuery.addBatch(queryParams);
			rs = getIndexRecordsQuery.executeSelect_MultiRows();
			while (rs.next()) {
				GenBankRecord rec = new GenBankRecord();
				final String accession = rs.getString("Accession");
				rec.setAccession(accession);
				Sequence seq = new Sequence();
				seq.setCollection_date(rs.getString("Collection_Date"));
				seq.setDefinition(rs.getString("Definition"));
				seq.setTax_id(rs.getInt("Tax_ID"));
				seq.setOrganism(rs.getString("Organism"));
				seq.setStrain(rs.getString("Strain"));
				if (rs.getString("Pub_ID") != null) {
					Publication pub = new Publication();
					pub.setPubId(Integer.valueOf(rs.getString("Pub_ID")));
					//seq.setPub(pub);
				}
				seq.setSegment_length(rs.getInt("Segment_Length"));
				seq.setPH1N1(rs.getBoolean("pH1N1"));
				rec.setSequence(seq);
				rec.setGenes(getIndexGeneName(accession));
				Host host = new Host();
				host.setName(rs.getString("Host_Name"));
				host.setTaxon(rs.getInt("Host_taxon"));
				rec.setHost(host);
				PossibleLocation loc = new PossibleLocation();
				if (rs.getLong("Geoname_ID") != 0) {
					loc.setId(rs.getLong("Geoname_ID"));
					loc.setLocation(rs.getString("Location"));
					loc.setType(rs.getString("Type"));
					loc.setCountry(rs.getString("Country"));
					rec.setGenBankLocation(loc);
				}
				records.add(rec);
			}
			rs.close();
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error pulling records for Index " + e.getMessage());
			throw e;
		}
		finally {
			closeConn(conn);
			queryParams.clear();
		}
		return records;
	}
	
	private List<Gene> getIndexGeneName(String accession) {		
		List<Gene> genes = new LinkedList<Gene>();
		Connection conn = null;
		ResultSet rs = null;
		List<Object> queryParams = new LinkedList<Object>();
		try {
			conn = getDriver();
			getIndexGenesQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, RETRIEVE_INDEX_GENES);
			queryParams.add(accession);
			getIndexGenesQuery.addBatch(queryParams);
			rs = getIndexGenesQuery.executeSelect_MultiRows();
			while (rs.next()) {				
				Gene gen = new Gene();
				gen.setName(rs.getString("Normalized_Gene_Name"));
				genes.add(gen);
			}
			rs.close();
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error pulling Genes for " + accession + ": " + e.getMessage());
		}
		finally {
			closeConn(conn);
			getIndexGenesQuery.close();
			queryParams.clear();
		}		
		return genes;
	}
	
	@Override
	public void insertGenBankLocations(List<PossibleLocation> locs) {
		try {
			log.info("Starting location insert...");
			Connection conn = getDriver();
			genLocQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, GENBANK_LOCATION_INSERT);
			emptyGeoLocQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, EMPTY_GEONAME_LOCATION_INSERT);
			int batch = 0;
			while (!locs.isEmpty()) {
				addGenBankLocation(locs.remove(0));
				batch++;
				if (batch >= 50000 || locs.isEmpty()) {
					log.info("Location batch inserted");
					genLocQuery.executeBatch();
					emptyGeoLocQuery.executeBatch();
					batch = 0;
					if (!locs.isEmpty()) {
						genLocQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, GENBANK_LOCATION_INSERT);
						emptyGeoLocQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, EMPTY_GEONAME_LOCATION_INSERT);
					}
				}
			}
			log.info("Locations inserted.");
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error inserting locations: "+e.getMessage());
		}
	}
}
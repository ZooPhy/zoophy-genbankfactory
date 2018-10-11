package edu.asu.zoophy.gbmetadataupdater.controller;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.asu.zoophy.gbmetadataupdater.GBMetadataUpdater;
import edu.asu.zoophy.gbmetadataupdater.db.DBQuery;
import edu.asu.zoophy.gbmetadataupdater.gene.GeneLuceneQuery;
import edu.asu.zoophy.gbmetadataupdater.utils.ResourceProvider;
import edu.asu.zoophy.gbmetadataupdater.utils.ResourceProvider.RP_PROVIDED_RESOURCES;

public class GBTestGeneMetadataController implements ControllerInt {

	final private Logger log = Logger.getLogger(GBMetadataUpdater.class);
	private final static String SELECT_ALL_ACCESSIONS = "Select \"Accession\" from \"Sequence_Details\"";// where \"Location\" is null";
	private final static String SELECT_TAXID = "Select \"Tax_ID\" from \"Sequence_Details\" where \"Accession\"=?";
	private final static String SELECT_GENEIDs ="Select \"Accession\", \"Value\" from \"Features\" where \"Key\"=\'db_xref\' and \"Value\" like \'GeneID:%\' and \"Accession\"=?";
	private final static String SELECT_GENE = "Select \"Accession\", \"Value\" from \"Features\" where \"Key\"=\'gene\' and \"Accession\"=?";
	private final static String SELECT_PRODUCT ="Select \"Accession\", \"Value\" from \"Features\" where \"Key\"=\'product\' and \"Accession\"=?";
	private final static String INSERT_METADATA = "Insert into \"GeneNormalized\" (\"Itv\", \"Accession\", \"Gene_Name\", \"Normalized_Gene_Name\") values (\'\', ?, ?, ?)";
	HashMap<String, Set<String>> taxMap = new HashMap<String, Set<String>>();
	
	public static void main(String[] args) {
		GeneLuceneQuery glq = new GeneLuceneQuery();
		GBTestGeneMetadataController c = new GBTestGeneMetadataController();
		String id = c.getID(glq, "VP4", "10970", true);
		System.out.println(id);
	}
	
	@Override
	public void run() throws Exception {
		log.info("Starting controller");
		//initializing/declaring db-related objects
		DBQuery accessionQuery = null;
		DBQuery geneIDQuery = null;
		DBQuery geneQuery = null;
		DBQuery prodQuery = null;
		DBQuery taxIDQuery = null;
		DBQuery insertQuery = null;
		ResultSet geneResult = null;
		ResultSet prodResult = null;
		ResultSet taxIDResult = null;
		List<Object> accessionParams = new LinkedList<Object>();

		List<Object> insertParams = new LinkedList<Object>();

		String[] accessions = {"AB523769","AB685364","AB753479","AF127992","AF238274","AF299753","AF325474","AF372393","AF548495","AJ404065","AJ968496","AM273360","AM709656","AY272068","AY371472","AY551600","AY672898","AY734492","AY827311","AY901685","C21625","CY017444","CY043231","CY075671","CY081613","CY108847","CY109801","CY130054","CY136141","CY140061","CY149069","CY171072","CY186575","D45215","DQ061747","DQ102495","DQ463566","DQ484497","DQ878753","EF161166","EF418439","EU281706","EU551817","EU698889","EU925526","EU932343","FJ649021","FJ653166","FJ692205","FJ713532","FJ713735","FN398429","FR669852","GQ180206","GQ211371","GQ918133","GU728123","HQ159608","HQ668836","HQ669420","JF979149","JN040812","JN155679","JN642964","JN650143","JN713624","JQ066760","JQ248385","JQ315101","JQ513749","JQ825145","JQ926510","JX219526","JX414070","JX425483","JX625643","JX913658","KC117010","KC506437","KC762670","KF181330","KF535168","KJ416556","KJ600776","KJ953041","KM048835","KM581055","KM879903","KP287466","KP416709","KP864082","KR075877","KR153862","KR860975","KT370422","KT735558","KT919087","M58421","S65744","U36113"};
		try {
			//query for retrieving the total number of unprocessed records
			//countQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_COUNT, null);
			//query for retrieving all unprocessed accession numbers
			accessionQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_ALL_ACCESSIONS,null);
			//query for retrieving pertinent record metadata given an accession number
			geneIDQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_GENEIDs, accessionParams);
			//query for retrieving value of "country" field in the record and checking whether the record includes the latitude/longitudes of the location
			geneQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_GENE, accessionParams);
			taxIDQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_TAXID, accessionParams);

			//query for updating the geospatial metadata of a record given it's accession number
			prodQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_SELECT_MULTIPLE_ROWS, SELECT_PRODUCT, accessionParams);
			insertQuery = new DBQuery(ResourceProvider.getDBInterfacer(RP_PROVIDED_RESOURCES.ANNOTATION_DBI).getConnection(), DBQuery.QT_INSERT_BATCH, INSERT_METADATA);
			/*
			countResult = countQuery.executeSelect_MultiRows();
			int numTotal = 0;
			if(countResult.next()) {
				numTotal = countResult.getInt("total");
			}
			log.info("Num total is ["+numTotal+"]");
			log.info("Retrieving all unprocessed accessions");
			accessionResult = accessionQuery.executeSelect_MultiRows();
			*/ 
			int numProcessed = 0;
			int numTotal = accessions.length;
			GeneLuceneQuery glq = new GeneLuceneQuery();
			for(String accession: accessions) {
				log.info("Processing acession ["+accession+"]");
				String id = glq.searchIDForAcc(accession);
				String name = "";
				boolean added = false;
				if(id!=null && id.length()>0) {
					String gene = glq.searchSymbolGivenGeneID(id); 
					if(gene!=null && gene.length()>0) {
						name=gene;
					}
				} else {
					accessionParams.clear();
					accessionParams.add(accession);
					geneResult = geneQuery.executeSelect_MultiRows();
					Set<String> genesAdded = new HashSet<String>();
					while(geneResult.next()) {
						String gene = geneResult.getString(2);
						if(gene!=null && gene.length()>0) {
							genesAdded.add(gene);
						}
					}
					if(accession.equalsIgnoreCase("KR075877")) {
						log.info("check");
					}
					Set<String> prodsAdded = new HashSet<String>();
					if(genesAdded.size()==0) {
						prodResult = prodQuery.executeSelect_MultiRows();
						while(prodResult.next()) {
							String prod = prodResult.getString(2);
							if(prod!=null && prod.length()>0 && !genesAdded.contains(prod)) {
								prodsAdded.add(prod.trim());
							}
						}
					}
					for(String gene:genesAdded) {
						if(gene!=null && gene.length()>0) {
							name = gene;
							taxIDResult = taxIDQuery.executeSelect_MultiRows();
							String taxID = "";
							if(taxIDResult.next()) {
								taxID = taxIDResult.getString(1);
							}
							id = getID(glq, name, taxID, true);
						}
						if(id==null||id.length()==0) {
							id = "0";
						} 
						if(name==null) {
							name = "";
						}
						insertParams.clear();
						insertParams.add(accession);
						insertParams.add(name);
						insertParams.add(id);
						insertQuery.addBatch(insertParams);
						added=true;
					}	
					for(String product:prodsAdded) {
						if(product==null || product.length()==0) {
							continue;
						}
						if(glq.isGeneSymbol(product)==false) {
							String[] productSplits = product.split("[,;]");
							boolean found = false;
							for(String prod: productSplits) {
								prod = prod.trim();
								if(glq.isGeneSymbol(prod)==true) {
									product = prod;
									found=true;
									break;
								}
							}
						}
						if(glq.isGeneSymbol(product)==true) {
							name = product;
							taxIDResult = taxIDQuery.executeSelect_MultiRows();
							String taxID = "";
							if(taxIDResult.next()) {
								taxID = taxIDResult.getString(1);
							}
							id = getID(glq, name, taxID, true);
							
						}	
						if(glq.isGeneSymbol(product)==false||id==null||id.length()==0) {
							if(glq.isSyn(product)==false) {
								continue;
							}
							taxIDResult = taxIDQuery.executeSelect_MultiRows();
							String taxID = "";
							if(taxIDResult.next()) {
								taxID = taxIDResult.getString(1);
							}
							id = getID(glq,product,taxID, false);
							if(id!=null && id.length()>0) {
								name = glq.searchSymbolGivenGeneID(id);
								break;
							}
							if(id==null||id.length()==0) {
								name = "";
								continue;
							}
						}
						if(id==null||id.length()==0) {
							id = "0";
						} 
						if(name==null) {
							name = "";
						}
						insertParams.clear();
						//queryParams3.add(accession);
						insertParams.add(accession);
						insertParams.add(name);
						insertParams.add(id);
						
						insertQuery.addBatch(insertParams);
						added=true;
					}	
				}
				if(!added) {
					if(id==null||id.length()==0) {
						id = "0";
					} 
					if(name==null) {
						name = "";
					}
					insertParams.clear();
					//queryParams3.add(accession);
					insertParams.add(accession);
					insertParams.add(name);
					insertParams.add(id);
					
					insertQuery.addBatch(insertParams);
				}
				numProcessed++;
				if(numProcessed%100==0||numProcessed==numTotal) {
					insertQuery.executeBatch();
					log.info("Executed. Total records completed: ["+numProcessed+"]");
				}
			}
			if(numProcessed%100==0||numProcessed==numTotal) {
				insertQuery.executeBatch();
				log.info("Executed. Total records completed: ["+numProcessed+"]");
			}
		} catch (Exception e) {
			insertQuery.executeBatch();
			log.info("Executed. Total records completed:");
		}
	}
	
	
	public String getID(GeneLuceneQuery glq, String gene, String taxID, boolean isSymbol) {
		if(taxID==null || taxID.length()==0) {
			return "";
		}
		String id = glq.searchGeneIDGivenSymbolAndTaxID(gene, taxID);
		String genusID = "";
		if(id==null || id.length()==0) {
			String curTaxID = taxID;
			while(curTaxID!=null && curTaxID.length()>0) {
				String parentTaxID = glq.searchParentSpeciesGivenTaxID(curTaxID);
				if(parentTaxID.trim().equals("1")||parentTaxID.trim().equals(curTaxID.trim())){
					break;
				}
				curTaxID = parentTaxID;
				id = glq.searchGeneIDGivenSymbolAndTaxID(gene, curTaxID);
				if(id!=null && id.length()>0) {
					break;
				}
				String rank = glq.searchRankGivenTaxID(curTaxID);
				if(rank.trim().toLowerCase().equals("genus")) {
					genusID = curTaxID;
					break;
				}
			}
		}
		if(genusID!=null && genusID.length()!=0&& (id==null || id.length()==0)) {
			Set<String> allTaxIDs =  taxMap.get(genusID);
			if(allTaxIDs==null) {
				id = searchAllChildrenForID(genusID, gene, glq, isSymbol);
			}
		}
		return id;
	}
	
	public String searchAllChildrenForID(String taxID, String gene, GeneLuceneQuery glq, boolean isSymbol) {
		Queue<String> q = new LinkedList<String>();
		String geneID = "";
		Set<String> done = new HashSet<String>();
		q.add(taxID);
		done.add(taxID);
		while(!q.isEmpty()){
			String curID = q.remove();
			if(isSymbol) {
				geneID = glq.searchGeneIDGivenSymbolAndTaxID(gene, curID);
			} else {
				geneID = glq.searchGeneIDGivenSynAndTaxID(gene, curID);
			}
			if(geneID!=null && geneID.length()>0) {
				return geneID;
			}
			else {
		 		Set<String> children = glq.searchTaxIDsGivenParent(curID);
		 		for(String child:children) {
		 			done.add(child);
		 			q.add(child);
		 		}
			}
		}
		if(taxMap.size()<5) {
			taxMap.put(taxID, done);
		}

		return geneID; 
	}
	
	

}

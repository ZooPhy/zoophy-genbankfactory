package edu.asu.zoophy.genbankfactory.utils.normalizer.gene;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.asu.zoophy.genbankfactory.database.GenBankFactory;
import edu.asu.zoophy.genbankfactory.utils.taxonomy.GenBankTree;
import edu.asu.zoophy.genbankfactory.utils.taxonomy.Node;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.database.DBQuery;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;

public class GeneNormalizer {
	private static final Logger log = Logger.getLogger("GeneNormalizer");
	//taxon ignore list: 1, 10239, 11308, 35301
	protected static GenBankFactory fac;
	private static GenBankTree gbTree;
	protected static final int[] acceptedTaxons = {186540,439488,11266,11157,1570291,186536,372255,186538,186539,86782,11598,339351,11599,286540,38016,439488,46607,308159,374423,28291,37705,104542,42894,261204,11608,37133,11571,46920,11604,64320,11157,11292,439488,11286,11270,35278,439488,11082,11050,11051,11071,370290,119213,119212,119215,119214,309433,119211,119210,119220,183666,119221,119216,119217,488106,119218,325678,157802,571502,282148,571503,127960,383597,385680,393560,309406,134367,309405,282134,1633709,215851,87514,447195,656062,416802,437442,215855,475601,273303,475602,299327,136477,1346489,286292,286293,476651,1129344,286294,311174,286295,286281,286285,387163,286284,286287,286286,387158,187402,286279,136506,35301,1396558,1082910,384619,232441,1569252,1565433,342224,136481,197911,136484,150171,147765,147762,359920,147760,439488,284164,575460,1084500,329376,184012,184009,184006,173712,184002,173714,11320,304360,211044,397549,222770,129772,222768,102793,222769,546800,145307,546801,222772,102797,129771,547380,102796,222773,352775,352774,352773,102800,114733,114732,352772,102801,352771,114731,352770,114730,352769,114729,129778,129779,114728,114727,465975,170500,352778,352777,140020,352776,1261419,384505,142943,1316904,221119,1249503,418387,367120,981614,385636,385624,1313083,195471,382835,551228,388039,388041,142947,333278,142951,402585,402586,333276,333277,142949,197912,11520,439488,197913,439488,11552};
	protected static final int[] ebolaTaxons = {186540,439488,11266,11157,1570291,186536,372255,186538,186539};
	private static HashMap<String,String> ebolaMappings = new HashMap<String,String>();
	protected static final int[] hantaTaxons = {86782,11598,339351,11599,286540,38016,439488,46607,308159,374423,28291,37705,104542,42894,261204,11608,37133,11571,46920,11604};
	private static HashMap<String,String> hantaMappings = new HashMap<String,String>();
	protected static final int[] zikaTaxons = {64320};
	private static HashMap<String,String> zikaMappings = new HashMap<String,String>();
	protected static final int[] rabiesTaxons = {11157,11292,439488,11286,11270};
	private static HashMap<String,String> rabiesMappings = new HashMap<String,String>();
	protected static final int[] wnTaxons = {35278, 439488, 11082, 11050, 11051, 11071};
	private static HashMap<String,String> wnMappings = new HashMap<String,String>();
	protected static final int[] fluATaxons = {370290,119213,119212,119215,119214,309433,119211,119210,119220,183666,119221,119216,119217,488106,119218,325678,157802,571502,282148,571503,127960,383597,385680,393560,309406,134367,309405,282134,1633709,215851,87514,447195,656062,416802,437442,215855,475601,273303,475602,299327,136477,1346489,286292,286293,476651,1129344,286294,311174,286295,286281,286285,387163,286284,286287,286286,387158,187402,286279,136506,35301,1396558,1082910,384619,232441,1569252,1565433,342224,136481,197911,136484,150171,147765,147762,359920,147760,439488,284164,575460,1084500,329376,184012,184009,184006,173712,184002,173714,11320,304360,211044,397549,222770,129772,222768,102793,222769,546800,145307,546801,222772,102797,129771,547380,102796,222773,352775,352774,352773,102800,114733,114732,352772,102801,352771,114731,352770,114730,352769,114729,129778,129779,114728,114727,465975,170500,352778,352777,140020,352776,1261419,384505,142943,1316904,221119,1249503,418387,367120,981614,385636,385624,1313083,195471,382835,551228,388039,388041,142947,333278,142951,402585,402586,333276,333277,142949};
	private static HashMap<String,String> fluAMappings = new HashMap<String,String>();
	protected static final int[] fluBTaxons = {197912,11520,439488};
	private static HashMap<String,String> fluBMappings = new HashMap<String,String>();
	protected static final int[] fluCTaxons = {197913,439488,11552};
	private static HashMap<String,String> fluCMappings = new HashMap<String,String>();
	private static HashSet<Integer> unmapped_taxons = new HashSet<Integer>();
	private static int successes;
	
	private static Connection conn = null;
	private final String UPDATE_GENE_NAME = "UPDATE \"Gene\" SET \"Normalized_Gene_Name\"=? WHERE \"Gene_ID\"=?";
	private final String PULL_GENES = "SELECT \"Gene_ID\", \"Gene_Name\", \"Tax_ID\" FROM \"Gene\" JOIN \"Sequence_Details\" ON \"Sequence_Details\".\"Accession\"=\"Gene\".\"Accession\" WHERE \"Normalized_Gene_Name\" IS NULL ORDER BY \"Gene_ID\" ASC";
	private List<TempGene> genes;
	private List<Object> queryParams;
	private final int BATCH_SIZE =50000;
	private int batch_count;
	private List<String> zikaGenes;
	private List<String> fluBGenes;
	private List<String> fluCGenes;
	private List<String> rabiesGenes;
	private List<String> wnGenes;
	private List<String> ebolaGenes;
	private List<String> hantaGenes;
	private List<String> fluAGenes;
	
	private static GeneNormalizer normalizer = null;
	
	public static GeneNormalizer getInstance() throws Exception {
		if (normalizer == null) {
			normalizer = new GeneNormalizer();
		}
		return normalizer;
	}
	
	private GeneNormalizer() throws Exception {
		log.info("Setting up Gene Normalizer..");
		prepareRuleData();
		fac = GenBankFactory.getInstance();
		gbTree = GenBankTree.getInstance();
	}
	
	public void normalizeGeneNames() throws Exception {
		successes = 0;
		log.info("Normalizing genes...");
		try {
			normalize();
			log.info("Genes Normalized.");
		} 
		catch (Exception e) {
			log.log(Level.SEVERE, "Could not normalize Genes: "+e.getMessage());
		}
	}
	
	private void normalize() throws Exception {
		log.info("Pulling genes...");
		try {
			conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
	    }
	    catch (Exception e) {
	    	log.log(Level.SEVERE, "Impossible to Initiate the Resources Provider:"+e.getMessage());
	    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
	    }
		queryParams = new LinkedList<Object>();
		DBQuery query = new DBQuery(conn, DBQuery.QT_SELECT_MULTIPLE_ROWS, PULL_GENES, queryParams);
		ResultSet rs = query.executeSelect_MultiRows();
		double tot = 0;
		genes = new LinkedList<TempGene>();
		while (rs.next()) {
			tot++;
			TempGene g = new TempGene();
			g.setId(rs.getLong("Gene_ID"));
			g.setName(rs.getString("Gene_Name"));
			g.setTaxon(findRelevantTaxon(rs.getInt("Tax_ID")));
			genes.add(g);
		}
		query.close();
		rs.close();
		log.info("Genes retreived.");
		log.info("Normalizing...");
		if (!genes.isEmpty()) {
			for (TempGene g : genes) {
				g.setName(mapName(g.getTaxon(),g.getName()));
				if (g.getName() != null && !g.getName().equalsIgnoreCase("?")) {
					successes++;
				}
			}
			double perc = (((double) successes) / tot) * 100.00;
			log.info("Successfully Normalized "+perc+"% of Gene Names.");
			log.info("Updating DB...");
			updateGenes(genes);
			log.info("Updated.");
			genes.clear();
			alertUnmappedTaxons();
		}
		else {
			log.info("DB already up to date.");
		}
	}

	private void updateGenes(List<TempGene> genes) {
		int total_updates = 0;
		try {
			DBQuery updateGeneQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, UPDATE_GENE_NAME);
			queryParams = new LinkedList<Object>();
			for (TempGene g : genes) {
				queryParams.add(g.getName());
				queryParams.add(g.getId());
				updateGeneQuery.addBatch(queryParams);
				queryParams.clear();
				total_updates++;
				batch_count++;
				if (batch_count == BATCH_SIZE) {
					log.info("Update_Gene_Name batch starting...");
					updateGeneQuery.executeBatch();
					log.info("Update_Gene_Name batch completed.");
					updateGeneQuery.close();
					updateGeneQuery = new DBQuery(conn, DBQuery.QT_INSERT_BATCH, UPDATE_GENE_NAME);
					batch_count = 0;
				}
			}
			if (batch_count != 0) {
				log.info("Final Update_Gene_Name batch starting...");
				updateGeneQuery.executeBatch();
				log.info("Update_Gene_Name batch completed.");
				updateGeneQuery.close();
				batch_count = 0;
			}
			log.info(total_updates+ " genes successfully updated.");
			total_updates = 0;
		} 
		catch (Exception e) {
			log.log(Level.SEVERE, "Error updating genes: "+e.getMessage());
		}
	}

	private int findRelevantTaxon(int taxon) {
		if (Arrays.binarySearch(acceptedTaxons, taxon) >= 0) {
			return taxon;
		}
		else {
			try {
				Node node = gbTree.getNode(taxon);
				if (node!=null) { //we should have most taxons in the tree...
					List<Node> ancestors = node.getAncestors();
					for (Node ancestor: ancestors) {
						taxon = ancestor.getID();
						if (Arrays.binarySearch(acceptedTaxons, taxon) >= 0) {
							return taxon;
						}
					}
				}
				else {
					log.log(Level.SEVERE, "Taxon not in tree: "+taxon);
					return taxon;
				}
			}
			catch (Exception e) {
				log.log(Level.SEVERE, "Error finding taxonomy for taxon: " + taxon);
				return taxon;
			}
		}
		log.log(Level.SEVERE, "Could not map taxon: "+taxon);
		return taxon;
	}

	private void alertUnmappedTaxons() {
		if (!unmapped_taxons.isEmpty()) {
			StringBuilder strBld = new StringBuilder();
			for (Integer i : unmapped_taxons) {
				strBld.append(i+", ");
			}
			log.log(Level.SEVERE, "Could not map taxons: "+strBld.toString());
		}
	}
	
	public List<String> getFullGenomeList(int virusTaxon) {
		if (Arrays.binarySearch(acceptedTaxons, virusTaxon) < 0) {
			Node node = gbTree.getNode(virusTaxon);
			if (node!=null) { //we should have most taxons in the tree...
				List<Node> ancestors = node.getAncestors();
				for (Node ancestor: ancestors) {
					virusTaxon = ancestor.getID();
					if (Arrays.binarySearch(acceptedTaxons, virusTaxon) >= 0) {
						break;
					}
				}
			}
		}
		if (Arrays.binarySearch(fluATaxons, virusTaxon) >= 0) {
			return fluAGenes;
		}
		else if (Arrays.binarySearch(ebolaTaxons, virusTaxon) >= 0) {
			return ebolaGenes;
		}
		else if (Arrays.binarySearch(fluBTaxons, virusTaxon) >= 0) {
			return fluBGenes;
		}
		else if (Arrays.binarySearch(fluCTaxons, virusTaxon) >= 0) {
			return fluCGenes;
		}
		else if (Arrays.binarySearch(rabiesTaxons, virusTaxon) >= 0) {
			return rabiesGenes;
		}
		else if (Arrays.binarySearch(wnTaxons, virusTaxon) >= 0) {
			return wnGenes;
		}
		else if (Arrays.binarySearch(hantaTaxons, virusTaxon) >= 0) {
			return hantaGenes;
		}
		else if (Arrays.binarySearch(zikaTaxons, virusTaxon) >= 0) {
			return zikaGenes;
		}
		return null;
	}

	private String mapName(int virusTaxon, String rawName) {
		if (Arrays.binarySearch(zikaTaxons, virusTaxon) >= 0) {
			return zikaMappings.get(rawName);
		}
		else if (Arrays.binarySearch(fluBTaxons, virusTaxon) >= 0) {
			return fluBMappings.get(rawName);
		}
		else if (Arrays.binarySearch(fluCTaxons, virusTaxon) >= 0) {
			return fluCMappings.get(rawName);
		}
		else if (Arrays.binarySearch(rabiesTaxons, virusTaxon) >= 0) {
			return rabiesMappings.get(rawName);
		}
		else if (Arrays.binarySearch(wnTaxons, virusTaxon) >= 0) {
			return wnMappings.get(rawName);
		}
		else if (Arrays.binarySearch(ebolaTaxons, virusTaxon) >= 0) {
			return ebolaMappings.get(rawName);
		}
		else if (Arrays.binarySearch(hantaTaxons, virusTaxon) >= 0) {
			return hantaMappings.get(rawName);
		}
		else if (Arrays.binarySearch(fluATaxons, virusTaxon) >= 0) {
			return fluAMappings.get(rawName);
		}
		else {
			unmapped_taxons.add(virusTaxon);
		}
		return "?";
	}
	
	private void prepareRuleData() {
		Arrays.sort(acceptedTaxons);
		//ebola//
		Arrays.sort(ebolaTaxons);
		ebolaMappings.put("NP", "NP");
		ebolaMappings.put("VP35", "VP35");
		ebolaMappings.put("VP40", "VP40");
		ebolaMappings.put("VP40\"", "VP40");
		ebolaMappings.put("GP", "GP");
		ebolaMappings.put("VP30", "VP30");
		ebolaMappings.put("VP24", "VP24");
		ebolaMappings.put("L", "L");
		ebolaMappings.put("L\"", "L");
		ebolaMappings.put("vp40", "VP40");
		ebolaMappings.put("N", "NP");
		ebolaMappings.put("SGP", "GP");
		ebolaMappings.put("sGP", "GP");
		ebolaMappings.put("sGP\"", "GP");
		ebolaMappings.put("vp30", "VP30");
		ebolaMappings.put("VP30\"", "VP30");
		ebolaMappings.put("VP35\"", "VP35");
		ebolaMappings.put("vp35\"", "VP35");
		ebolaMappings.put("VP24\"", "VP24");
		ebolaMappings.put("vp24", "VP24");
		ebolaMappings.put("vp35", "VP35");
		ebolaMappings.put("GP\"", "GP");
	    ebolaMappings.put("SsGP\"", "GP");
		ebolaMappings.put("gp", "GP");
		ebolaMappings.put("NP\"", "NP");
		ebolaMappings.put("NP\"                     /experiment=\"experimental evidence, no additional details                     recorded", "NP");
		ebolaMappings.put("ssGP\"", "GP");
		ebolaMappings.put("POLYMERASE", "L");
		ebolaMappings.put("vp2", "VP24");
		ebolaMappings.put("vp3", "VP35");
		//hanta//
		Arrays.sort(hantaTaxons);
		hantaMappings.put("S", "S");
		hantaMappings.put("M", "M");
		hantaMappings.put("L", "L");
		hantaMappings.put("nucleocapsid protein", "S");
		hantaMappings.put("GPC", "M");
		hantaMappings.put("Gn", "M");
		hantaMappings.put("L segment", "L");
		hantaMappings.put("N", "S");
		hantaMappings.put("NP", "S");
		hantaMappings.put("G2", "M");
		hantaMappings.put("G1-G2", "M");
		hantaMappings.put("G1", "M");
		hantaMappings.put("Puumala_virus_CG1820/POR_S_segment", "S");
		hantaMappings.put("S segment", "S");
		hantaMappings.put("N\"", "S");
		hantaMappings.put("Medium", "M");
		hantaMappings.put("Gc", "M");
		hantaMappings.put("nucleoprotein", "S");
		hantaMappings.put("GnGc", "M");
		hantaMappings.put("Large", "L");
		hantaMappings.put("n", "S");
		hantaMappings.put("gpc", "M");
		hantaMappings.put("M gene", "M");
		hantaMappings.put("NSx", "S");
		hantaMappings.put("RdRp", "L");
		hantaMappings.put("GP", "M");
		hantaMappings.put("g2", "M");
		hantaMappings.put("M segment", "M");
		hantaMappings.put("Small", "S");
		hantaMappings.put("G1G2", "M");
		hantaMappings.put("G2 glycoprotein", "M");
		hantaMappings.put("glycoprotein G1", "M");
		hantaMappings.put("Ns", "S");
		hantaMappings.put("Z10", "M");
		hantaMappings.put("precursor glycoprotein gene", "M");
		hantaMappings.put("S-gene", "S");
		hantaMappings.put("pol", "L");
		//zika//
		Arrays.sort(zikaTaxons);
		zikaMappings.put("C", "C");
		zikaMappings.put("M", "M");
		zikaMappings.put("E", "E");
		zikaMappings.put("NS", "NS");
		zikaMappings.put("NS3", "NS");
		zikaMappings.put("NS5", "NS");
		zikaMappings.put("GP1", "complete");
		zikaMappings.put("GP1\"", "complete");
		zikaMappings.put("polyprotein", "complete");
		zikaMappings.put("anchored capsid protein C", "C");
		zikaMappings.put("capsid", "C");
		zikaMappings.put("capsid peptide", "C");
		zikaMappings.put("capsid protein", "C");
		zikaMappings.put("capsid protein C", "C");
		zikaMappings.put("env", "E");
		zikaMappings.put("Env", "E");
		zikaMappings.put("envelope", "E");
		zikaMappings.put("envelope glycoprotein", "E");
		zikaMappings.put("envelope protein", "E");
		zikaMappings.put("envelope protein E", "E");
		zikaMappings.put("envelope protein peptide", "E");
		zikaMappings.put("glycoprotein", "E");
		zikaMappings.put("membane protein", "M");
		zikaMappings.put("membane protein peptide", "M");
		zikaMappings.put("membrane", "M");
		zikaMappings.put("membrane glycoprotein M", "M");
		zikaMappings.put("membrane glycoprotein precursor M", "M");
		zikaMappings.put("membrane protein", "M");
		zikaMappings.put("Pr", "M");
		zikaMappings.put("pro", "M");
		zikaMappings.put("propeptide", "M");
		zikaMappings.put("protein pr", "M");
		zikaMappings.put("non-structural protein 5", "NS");
		zikaMappings.put("nonstructural protein 5", "NS");
		zikaMappings.put("nonstructural protein NS1", "NS");
		zikaMappings.put("nonstructural protein NS2A", "NS");
		zikaMappings.put("nonstructural protein NS2B", "NS");
		zikaMappings.put("nonstructural protein NS3", "NS");
		zikaMappings.put("nonstructural protein NS4A", "NS");
		zikaMappings.put("nonstructural protein NS4B", "NS");
		zikaMappings.put("NS1", "NS");
		zikaMappings.put("NS1 peptide", "NS");
		zikaMappings.put("NS2", "NS");
		zikaMappings.put("NS2A", "NS");
		zikaMappings.put("NS2A peptide", "NS");
		zikaMappings.put("NS2B", "NS");
		zikaMappings.put("NS2B peptide", "NS");
		zikaMappings.put("NS3 peptide", "NS");
		zikaMappings.put("NS3 protein", "NS");
		zikaMappings.put("NS4A", "NS");
		zikaMappings.put("NS4A peptide", "NS");
		zikaMappings.put("NS4B", "NS");
		zikaMappings.put("NS4B peptide", "NS");
		zikaMappings.put("NS5 peptide", "NS");
		zikaMappings.put("NS5 protein", "NS");
		zikaMappings.put("NS5B", "NS");
		zikaMappings.put("peptide 2K", "NS");
		zikaMappings.put("protein 2K", "NS");
		zikaMappings.put("RNA-dependent RNA polymerase NS5", "NS");
		zikaMappings.put("6K", "NS");
		//rabies//
		Arrays.sort(rabiesTaxons);
		rabiesMappings.put("N", "N");
		rabiesMappings.put("P", "P");
		rabiesMappings.put("M", "M");
		rabiesMappings.put("G", "G");
		rabiesMappings.put("L", "L");
		rabiesMappings.put("G\"", "G");
		rabiesMappings.put("M gene", "M");
		rabiesMappings.put("n", "N");
		rabiesMappings.put("gp", "G");
		rabiesMappings.put("GP", "G");
		rabiesMappings.put("M1", "M");
		rabiesMappings.put("M2", "M");
		rabiesMappings.put("LP", "L");
		rabiesMappings.put("MP", "M");
		rabiesMappings.put("PP", "P");
		rabiesMappings.put("gpG", "G");
		rabiesMappings.put("glycoprotein", "G");
		rabiesMappings.put("glycoprotein\"", "G");
		rabiesMappings.put("NS", "?");
		rabiesMappings.put("CVSNS", "?");
		rabiesMappings.put("CVS-G", "?");
		rabiesMappings.put("NP", "?");
		rabiesMappings.put("np", "?");
		rabiesMappings.put("psi", "?");
		rabiesMappings.put("Psi", "?");
		rabiesMappings.put("NS(P)", "?");
		rabiesMappings.put("GPL", "?");
		//west nile//
		Arrays.sort(wnTaxons);
		wnMappings.put("C", "C");
		wnMappings.put("M", "M");
		wnMappings.put("E", "E");
		wnMappings.put("NS", "NS");
		wnMappings.put("env", "E");
		wnMappings.put("NS5", "NS");
		wnMappings.put("NS3", "NS");
		wnMappings.put("genomic RNA", "?");
		wnMappings.put("viral genome", "complete");
		wnMappings.put("polyprotein", "NS");
		wnMappings.put("GP1\"", "complete");
		wnMappings.put("GP2\"", "complete");
		wnMappings.put("GP3\"", "complete");
		wnMappings.put("pol", "?");
		//Flu A//
		Arrays.sort(fluATaxons);
		fluAMappings.put("PB2", "PB2");
		fluAMappings.put("PB1", "PB1");
		fluAMappings.put("PA", "PA");
		fluAMappings.put("HA", "HA");
		fluAMappings.put("NP", "NP");
		fluAMappings.put("NA", "NA");
		fluAMappings.put("M", "M");
		fluAMappings.put("NS", "NS");
		fluAMappings.put("HA2", "HA");
		fluAMappings.put("HA1", "HA");
		fluAMappings.put("ns", "NS");
		fluAMappings.put("pb1", "PB1");
		fluAMappings.put("NP\"", "NP");
		fluAMappings.put("np", "NP");
		fluAMappings.put("h9", "HA");
		fluAMappings.put("NS-1", "NS");
		fluAMappings.put("MP1", "M");
		fluAMappings.put("HA7", "HA");
		fluAMappings.put("NEP\"                     /gene_synonym=\"NS2", "NS");
		fluAMappings.put("HAH1", "HA");
		fluAMappings.put("HA0", "HA");
		fluAMappings.put("M2", "M");
		fluAMappings.put("M1", "M");
		fluAMappings.put("H7HA", "HA");
		fluAMappings.put("HA\"", "HA");
		fluAMappings.put("H\"                     /gene_synonym=\"HA", "HA");
		fluAMappings.put("M2\"", "M");
		fluAMappings.put("PA-X\"", "PA");
		fluAMappings.put("NS 1", "NS");
		fluAMappings.put("HA-NP\"", "HA");
		fluAMappings.put("MA", "M");
		fluAMappings.put("H6HA", "HA");
		fluAMappings.put("H5HA", "HA");
		fluAMappings.put("m1", "M");
		fluAMappings.put("m2", "M");
		fluAMappings.put("N1", "NA");
		fluAMappings.put("MP", "M");
		fluAMappings.put("HAI", "HA");
		fluAMappings.put("N3", "NA");
		fluAMappings.put("N2", "NA");
		fluAMappings.put("M42", "M");
		fluAMappings.put("H3HA", "HA");
		fluAMappings.put("ha", "HA");
		fluAMappings.put("NS2", "NS");
		fluAMappings.put("NS2\"", "NS");
		fluAMappings.put("NS2\"                     /gene_synonym=\"NEP", "NS");
		fluAMappings.put("NS-2", "NS");
		fluAMappings.put("NS1", "NS");
		fluAMappings.put("M1\"", "M");
		fluAMappings.put("NS3", "NS");
		fluAMappings.put("NS1\"", "NS");
		fluAMappings.put("pa", "PA");
		fluAMappings.put("H4HA", "HA");
		fluAMappings.put("HA9", "HA");
		fluAMappings.put("PB1-F2\"", "PB1");
		fluAMappings.put("MS1", "M");
		fluAMappings.put("segment 5", "NP");
		fluAMappings.put("N40", "PB1");
		fluAMappings.put("H9HA", "HA");
		fluAMappings.put("NAN2", "NA");
		fluAMappings.put("NAN1", "NA");
		fluAMappings.put("ns2\"                     /gene_synonym=\"ns1", "NS");
		fluAMappings.put("NA\"", "NA");
		fluAMappings.put("N", "NA");
		fluAMappings.put("n", "NA");
		fluAMappings.put("H", "HA");
		fluAMappings.put("PA-X", "PA");
		fluAMappings.put("NA2", "NA");
		fluAMappings.put("NA1", "NA");
		fluAMappings.put("PB2\"", "PB2");
		fluAMappings.put("PA1", "PA");
		fluAMappings.put("PB 1", "PB1");
		fluAMappings.put("H1HA", "HA");
		fluAMappings.put("HA\"                     /product=\"hemagglutinin", "HA");
		fluAMappings.put("PBP2", "PB2");
		fluAMappings.put("HA gene", "HA");
		fluAMappings.put("m", "M");
		fluAMappings.put("PBP1", "PB1");
		fluAMappings.put("PB1-F2", "PB1");
		fluAMappings.put("P2", "PB2");
		fluAMappings.put("M\"", "M");
		fluAMappings.put("H7", "HA");
		fluAMappings.put("ns1", "NS");
		fluAMappings.put("H6", "HA");
		fluAMappings.put("H5", "HA");
		fluAMappings.put("N1NA", "NA");
		fluAMappings.put("na", "NA");
		fluAMappings.put("pb2", "PB2");
		fluAMappings.put("H3", "HA");
		fluAMappings.put("H1", "HA");
		fluAMappings.put("PB1\"", "PB1");
		fluAMappings.put("PA\"", "PA");
		fluAMappings.put("NEP", "NS");
		fluAMappings.put("NEP\"                     /gene_synonym=\"NS2", "NS");
		fluAMappings.put("NEP\"                     /gene_synonym=\"NS2\"", "NS");
		fluAMappings.put("NEP\"", "NS");
		fluAMappings.put("np", "NP");
		fluAMappings.put("np\"", "NP");
		fluAMappings.put("ns2", "NS");
		fluAMappings.put("NP1", "NP");
		fluAMappings.put("NP2", "NP");
		fluAMappings.put("NS 2", "NS");
		fluAMappings.put("PB1-N40", "PB1");
		fluAMappings.put("nucleoprotein", "NP");
		fluAMappings.put("mbgl", "?");
		fluAMappings.put("MP2", "?");
		fluAMappings.put("MS2", "?");
		//Flu B//
		Arrays.sort(fluBTaxons);
		fluBMappings.put("PB2", "PB2");
		fluBMappings.put("PB1", "PB1");
		fluBMappings.put("PA", "PA");
		fluBMappings.put("HA", "HA");
		fluBMappings.put("NP", "NP");
		fluBMappings.put("NA", "NA");
		fluBMappings.put("M", "M");
		fluBMappings.put("NS", "NS");
		fluBMappings.put("BM2", "M");
		fluBMappings.put("HA1", "HA");
		fluBMappings.put("pb1", "PB1");
		fluBMappings.put("np", "NP");
		fluBMappings.put("NEP\"                     /gene_synonym=\"NS2", "NS");
		fluBMappings.put("M1", "M");
		fluBMappings.put("HA\"", "HA");
		fluBMappings.put("HA\"                     /gene_synonym=\"ha1", "HA");
		fluBMappings.put("NB-NA", "NA");
		fluBMappings.put("MP", "M");
		fluBMappings.put("ha", "HA");
		fluBMappings.put("HA\"                     /gene_synonym=\"ha1\"                     /allele=\"4", "HA");
		fluBMappings.put("NS2", "NS");
		fluBMappings.put("ns2", "NS");
		fluBMappings.put("NS1", "NS");
		fluBMappings.put("ns1", "NS");
		fluBMappings.put("pb2", "PB2");
		fluBMappings.put("pa", "PA");
		fluBMappings.put("NB", "PB1");
		fluBMappings.put("polymerase acidic", "PA");
		//Flu C//
		Arrays.sort(fluCTaxons);
		fluCMappings.put("PB2", "PB2");
		fluCMappings.put("PB1", "PB1");
		fluCMappings.put("PA", "PA");
		fluCMappings.put("HEF", "HEF");
		fluCMappings.put("NP", "NP");
		fluCMappings.put("M", "M");
		fluCMappings.put("NS", "NS");
		fluCMappings.put("NS2", "NS");
		fluCMappings.put("NS1", "NS");
		fluCMappings.put("CM2", "M");
		fluCMappings.put("HE", "HEF");
		fluCMappings.put("p3", "PA");
		fluCMappings.put("P3", "PA");
		fluCMappings.put("M1", "M");
		fluCMappings.put("M2", "M");
		fluCMappings.put("NEP\"                     /gene_synonym=\"NS2", "NS");
		fluCMappings.put("NP\"", "NP");
		fluCMappings.put("NEP/NS2", "NS");
		fluCMappings.put("P42", "?");
		//gene lists//
		zikaGenes = Arrays.asList("C","M","E","NS");
		fluBGenes = Arrays.asList("PB2","PB1","PA","HA","NP","NA","M","NS");
		fluCGenes = Arrays.asList("PB2","PB1","PA","HEF","NP","M","NS");
		rabiesGenes = Arrays.asList("N","P","M","G","L");
		wnGenes = Arrays.asList("C","M","E","NS");
		ebolaGenes = Arrays.asList("NP","VP35","VP40","GP","VP30","VP24","L");
		hantaGenes = Arrays.asList("S","M","L");
		fluAGenes = Arrays.asList("PB2","PB1","PA","HA","NP","NA","M","NS");
	}
}

package edu.asu.zoophy.genbankfactory.utils.normalizer.gene;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

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
			log.fatal( "Could not normalize Genes: "+e.getMessage());
		}
	}
	
	private void normalize() throws Exception {
		log.info("Pulling genes...");
		try {
			conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
	    }
	    catch (Exception e) {
	    	log.fatal( "Impossible to Initiate the Resources Provider:"+e.getMessage());
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
			log.fatal( "Error updating genes: "+e.getMessage());
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
					log.fatal( "Taxon not in tree: "+taxon);
					return taxon;
				}
			}
			catch (Exception e) {
				log.fatal( "Error finding taxonomy for taxon: " + taxon);
				return taxon;
			}
		}
		log.fatal( "Could not map taxon: "+taxon);
		return taxon;
	}

	private void alertUnmappedTaxons() {
		if (!unmapped_taxons.isEmpty()) {
			StringBuilder strBld = new StringBuilder();
			for (Integer i : unmapped_taxons) {
				strBld.append(i+", ");
			}
			log.fatal( "Could not map taxons: "+strBld.toString());
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
		rawName = rawName.trim().toLowerCase();
		if (Arrays.binarySearch(zikaTaxons, virusTaxon) >= 0) {
			return zikaMappings.get(rawName.toLowerCase());
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
		ebolaMappings.put("np", "NP");
		ebolaMappings.put("vp35", "VP35");
		ebolaMappings.put("vp40", "VP40");
		ebolaMappings.put("vp40\"", "VP40");
		ebolaMappings.put("gp", "GP");
		ebolaMappings.put("vp30", "VP30");
		ebolaMappings.put("vp24", "VP24");
		ebolaMappings.put("l", "L");
		ebolaMappings.put("l\"", "L");
		ebolaMappings.put("n", "NP");
		ebolaMappings.put("sgp", "GP");
		ebolaMappings.put("sgp\"", "GP");
		ebolaMappings.put("vp30\"", "VP30");
		ebolaMappings.put("vp35\"", "VP35");
		ebolaMappings.put("vp24\"", "VP24");
		ebolaMappings.put("vp35", "VP35");
		ebolaMappings.put("gp\"", "GP");
	    ebolaMappings.put("ssgp\"", "GP");
		ebolaMappings.put("np\"", "NP");
		ebolaMappings.put("np\"                     /experiment=\"experimental evidence, no additional details                     recorded", "NP");
		ebolaMappings.put("ssgp\"", "GP");
		ebolaMappings.put("polymerase", "L");
		ebolaMappings.put("vp2", "VP24");
		ebolaMappings.put("vp3", "VP35");
		//hanta//
		Arrays.sort(hantaTaxons);
		hantaMappings.put("s", "S");
		hantaMappings.put("m", "M");
		hantaMappings.put("l", "L");
		hantaMappings.put("nucleocapsid protein", "S");
		hantaMappings.put("gpc", "M");
		hantaMappings.put("gn", "M");
		hantaMappings.put("l segment", "L");
		hantaMappings.put("n", "S");
		hantaMappings.put("np", "S");
		hantaMappings.put("g2", "M");
		hantaMappings.put("g1-g2", "M");
		hantaMappings.put("g1", "M");
		hantaMappings.put("puumala_virus_cg1820/por_s_segment", "S");
		hantaMappings.put("s segment", "S");
		hantaMappings.put("n\"", "S");
		hantaMappings.put("medium", "M");
		hantaMappings.put("gc", "M");
		hantaMappings.put("nucleoprotein", "S");
		hantaMappings.put("gngc", "M");
		hantaMappings.put("large", "L");
		hantaMappings.put("n", "S");
		hantaMappings.put("gpc", "M");
		hantaMappings.put("m gene", "M");
		hantaMappings.put("nsx", "S");
		hantaMappings.put("rdrp", "L");
		hantaMappings.put("gp", "M");
		hantaMappings.put("g2", "M");
		hantaMappings.put("m segment", "M");
		hantaMappings.put("small", "S");
		hantaMappings.put("g1g2", "M");
		hantaMappings.put("g2 glycoprotein", "M");
		hantaMappings.put("glycoprotein g1", "M");
		hantaMappings.put("ns", "S");
		hantaMappings.put("z10", "M");
		hantaMappings.put("precursor glycoprotein gene", "M");
		hantaMappings.put("s-gene", "S");
		hantaMappings.put("pol", "L");
		hantaMappings.put("m-segment", "M");
		hantaMappings.put("s-segment", "S");
		hantaMappings.put("medium (m)", "M");
		hantaMappings.put("small (s)", "S");
		hantaMappings.put("l-segment", "L");
		hantaMappings.put("l; large", "L");
		hantaMappings.put("m; medium", "M");
		hantaMappings.put("s; small", "S");
		hantaMappings.put("middle", "M");
		//zika//
		Arrays.sort(zikaTaxons);
		zikaMappings.put("c", "C");
		zikaMappings.put("m", "M");
		zikaMappings.put("e", "E");
		zikaMappings.put("ns", "NS");
		zikaMappings.put("ns3", "NS");
		zikaMappings.put("ns5", "NS");
		zikaMappings.put("gp1", "complete");
		zikaMappings.put("gp1\"", "complete");
		zikaMappings.put("polyprotein", "complete");
		zikaMappings.put("anchored capsid protein c", "C");
		zikaMappings.put("capsid", "C");
		zikaMappings.put("capsid peptide", "C");
		zikaMappings.put("capsid protein", "C");
		zikaMappings.put("capsid protein c", "C");
		zikaMappings.put("env", "E");
		zikaMappings.put("envelope", "E");
		zikaMappings.put("envelope glycoprotein", "E");
		zikaMappings.put("envelope protein", "E");
		zikaMappings.put("envelope protein E", "E");
		zikaMappings.put("envelope protein peptide", "E");
		zikaMappings.put("glycoprotein", "E");
		zikaMappings.put("membane protein", "M");
		zikaMappings.put("membane protein peptide", "M");
		zikaMappings.put("membrane", "M");
		zikaMappings.put("membrane glycoprotein m", "M");
		zikaMappings.put("membrane glycoprotein precursor m", "M");
		zikaMappings.put("membrane protein", "M");
		zikaMappings.put("pr", "M");
		zikaMappings.put("pro", "M");
		zikaMappings.put("propeptide", "M");
		zikaMappings.put("protein pr", "M");
		zikaMappings.put("non-structural protein 5", "NS");
		zikaMappings.put("nonstructural protein 5", "NS");
		zikaMappings.put("nonstructural protein ns1", "NS");
		zikaMappings.put("nonstructural protein ns2a", "NS");
		zikaMappings.put("nonstructural protein ns2b", "NS");
		zikaMappings.put("nonstructural protein ns3", "NS");
		zikaMappings.put("nonstructural protein ns4a", "NS");
		zikaMappings.put("nonstructural protein ns4b", "NS");
		zikaMappings.put("ns1", "NS");
		zikaMappings.put("ns1 peptide", "NS");
		zikaMappings.put("ns2", "NS");
		zikaMappings.put("ns2a", "NS");
		zikaMappings.put("ns2a peptide", "NS");
		zikaMappings.put("ns2b", "NS");
		zikaMappings.put("ns2b peptide", "NS");
		zikaMappings.put("ns3 peptide", "NS");
		zikaMappings.put("ns3 protein", "NS");
		zikaMappings.put("ns4a", "NS");
		zikaMappings.put("ns4a peptide", "NS");
		zikaMappings.put("ns4b", "NS");
		zikaMappings.put("ns4b peptide", "NS");
		zikaMappings.put("ns5 peptide", "NS");
		zikaMappings.put("ns5 protein", "NS");
		zikaMappings.put("ns5b", "NS");
		zikaMappings.put("peptide 2k", "NS");
		zikaMappings.put("protein 2k", "NS");
		zikaMappings.put("rna-dependent rna polymerase ns5", "NS");
		zikaMappings.put("6k", "NS");
		//rabies//
		Arrays.sort(rabiesTaxons);
		rabiesMappings.put("n", "N");
		rabiesMappings.put("p", "P");
		rabiesMappings.put("m", "M");
		rabiesMappings.put("g", "G");
		rabiesMappings.put("l", "L");
		rabiesMappings.put("g\"", "G");
		rabiesMappings.put("m gene", "M");
		rabiesMappings.put("gp", "G");
		rabiesMappings.put("m1", "M");
		rabiesMappings.put("m2", "M");
		rabiesMappings.put("lp", "L");
		rabiesMappings.put("mp", "M");
		rabiesMappings.put("pp", "P");
		rabiesMappings.put("gpg", "G");
		rabiesMappings.put("glycoprotein", "G");
		rabiesMappings.put("glycoprotein\"", "G");
		rabiesMappings.put("ns", "complete");
		rabiesMappings.put("cvsns", "P");
		rabiesMappings.put("cvs-g", "complete");
		rabiesMappings.put("np", "N");
		rabiesMappings.put("psi", "L");
		rabiesMappings.put("ns(p)", "complete");
		rabiesMappings.put("gpl", "G");
		//west nile//
		Arrays.sort(wnTaxons);
		wnMappings.put("c", "C");
		wnMappings.put("m", "M");
		wnMappings.put("e", "E");
		wnMappings.put("ns", "NS");
		wnMappings.put("env", "E");
		wnMappings.put("ns5", "NS");
		wnMappings.put("ns3", "NS");
		wnMappings.put("genomic rna", "complete");
		wnMappings.put("viral genome", "complete");
		wnMappings.put("polyprotein", "NS");
		wnMappings.put("gp1\"", "complete");
		wnMappings.put("gp2\"", "complete");
		wnMappings.put("gp3\"", "complete");
		wnMappings.put("pol", "complete");
		wnMappings.put("core", "complete");
		wnMappings.put("pre m", "complete");
		wnMappings.put("prem", "complete");
		wnMappings.put("pre-membrane", "complete");
		wnMappings.put("prm", "complete");
		wnMappings.put("caspid", "C");
		wnMappings.put("nonstructural", "NS");
		wnMappings.put("premembrane", "complete");
		wnMappings.put("non-structural", "NS");
		wnMappings.put("envelope", "E");
		wnMappings.put("capsid", "C");
		wnMappings.put("membrane", "complete");
		wnMappings.put("pre-M", "complete");
		//Flu A//
		Arrays.sort(fluATaxons);
		fluAMappings.put("pb2", "PB2");
		fluAMappings.put("pb1", "PB1");
		fluAMappings.put("pa", "PA");
		fluAMappings.put("ha", "HA");
		fluAMappings.put("np", "NP");
		fluAMappings.put("na", "NA");
		fluAMappings.put("m", "M");
		fluAMappings.put("ns", "NS");
		fluAMappings.put("ha2", "HA");
		fluAMappings.put("ha1", "HA");
		fluAMappings.put("ns", "NS");
		fluAMappings.put("pb1", "PB1");
		fluAMappings.put("np\"", "NP");
		fluAMappings.put("np", "NP");
		fluAMappings.put("h9", "HA");
		fluAMappings.put("ns-1", "NS");
		fluAMappings.put("mp1", "M");
		fluAMappings.put("ha7", "HA");
		fluAMappings.put("nep\"                     /gene_synonym=\"NS2", "NS");
		fluAMappings.put("hah1", "HA");
		fluAMappings.put("ha0", "HA");
		fluAMappings.put("m2", "M");
		fluAMappings.put("m1", "M");
		fluAMappings.put("h7ha", "HA");
		fluAMappings.put("ha\"", "HA");
		fluAMappings.put("h\"                     /gene_synonym=\"HA", "HA");
		fluAMappings.put("m2\"", "M");
		fluAMappings.put("pa-x\"", "PA");
		fluAMappings.put("ns 1", "NS");
		fluAMappings.put("ha-np\"", "HA");
		fluAMappings.put("ma", "M");
		fluAMappings.put("h6ha", "HA");
		fluAMappings.put("h5ha", "HA");
		fluAMappings.put("m1", "M");
		fluAMappings.put("m2", "M");
		fluAMappings.put("n1", "NA");
		fluAMappings.put("mp", "M");
		fluAMappings.put("hai", "HA");
		fluAMappings.put("n3", "NA");
		fluAMappings.put("n2", "NA");
		fluAMappings.put("m42", "M");
		fluAMappings.put("h3hA", "HA");
		fluAMappings.put("ha", "HA");
		fluAMappings.put("ns2", "NS");
		fluAMappings.put("ns2\"", "NS");
		fluAMappings.put("ns2\"                     /gene_synonym=\"nep", "NS");
		fluAMappings.put("ns-2", "NS");
		fluAMappings.put("ns1", "NS");
		fluAMappings.put("m1\"", "M");
		fluAMappings.put("ns3", "NS");
		fluAMappings.put("ns1\"", "NS");
		fluAMappings.put("pa", "PA");
		fluAMappings.put("h4ha", "HA");
		fluAMappings.put("ha9", "HA");
		fluAMappings.put("pb1-f2\"", "PB1");
		fluAMappings.put("ms1", "M");
		fluAMappings.put("segment 5", "NP");
		fluAMappings.put("n40", "PB1");
		fluAMappings.put("h9ha", "HA");
		fluAMappings.put("nAn2", "NA");
		fluAMappings.put("nAn1", "NA");
		fluAMappings.put("ns2\"                     /gene_synonym=\"ns1", "NS");
		fluAMappings.put("na\"", "NA");
		fluAMappings.put("n", "NA");
		fluAMappings.put("h", "HA");
		fluAMappings.put("pa-x", "PA");
		fluAMappings.put("na2", "NA");
		fluAMappings.put("na1", "NA");
		fluAMappings.put("pb2\"", "PB2");
		fluAMappings.put("pa1", "PA");
		fluAMappings.put("pb 1", "PB1");
		fluAMappings.put("h1ha", "HA");
		fluAMappings.put("ha\"                     /product=\"hemagglutinin", "HA");
		fluAMappings.put("hemagglutinin", "HA");
		fluAMappings.put("pbp2", "PB2");
		fluAMappings.put("ha gene", "HA");
		fluAMappings.put("m", "M");
		fluAMappings.put("pbp1", "PB1");
		fluAMappings.put("pb1-f2", "PB1");
		fluAMappings.put("p2", "PB2");
		fluAMappings.put("m\"", "M");
		fluAMappings.put("h7", "HA");
		fluAMappings.put("ns1", "NS");
		fluAMappings.put("h6", "HA");
		fluAMappings.put("h5", "HA");
		fluAMappings.put("n1na", "NA");
		fluAMappings.put("na", "NA");
		fluAMappings.put("pb2", "PB2");
		fluAMappings.put("h3", "HA");
		fluAMappings.put("h1", "HA");
		fluAMappings.put("pb1\"", "PB1");
		fluAMappings.put("pa\"", "PA");
		fluAMappings.put("nep", "NS");
		fluAMappings.put("nep\"                     /gene_synonym=\"ns2\"", "NS");
		fluAMappings.put("nep\"", "NS");
		fluAMappings.put("np", "NP");
		fluAMappings.put("np\"", "NP");
		fluAMappings.put("ns2", "NS");
		fluAMappings.put("np1", "NP");
		fluAMappings.put("np2", "NP");
		fluAMappings.put("ns 2", "NS");
		fluAMappings.put("pb1-n40", "PB1");
		fluAMappings.put("nucleoprotein", "NP");
		fluAMappings.put("mbgl", "HA");
		fluAMappings.put("mp2", "M");
		fluAMappings.put("ms2", "M");
		fluAMappings.put("nep\"                     /gene_synonym=\"ns2", "NS");
		fluAMappings.put("nep/ns2", "NS");
		fluAMappings.put("ns2/nep", "NS");
		//Flu B//
		Arrays.sort(fluBTaxons);
		fluBMappings.put("pb2", "PB2");
		fluBMappings.put("pb1", "PB1");
		fluBMappings.put("pa", "PA");
		fluBMappings.put("ha", "HA");
		fluBMappings.put("na", "NA");
		fluBMappings.put("m", "M");
		fluBMappings.put("ns", "NS");
		fluBMappings.put("bm2", "M");
		fluBMappings.put("ha1", "HA");
		fluBMappings.put("pb1", "PB1");
		fluBMappings.put("np", "NP");
		fluBMappings.put("nep\"                     /gene_synonym=\"ns2", "NS");
		fluBMappings.put("m1", "M");
		fluBMappings.put("ha\"", "HA");
		fluBMappings.put("ha\"                     /gene_synonym=\"ha1", "HA");
		fluBMappings.put("nb-na", "NA");
		fluBMappings.put("mp", "M");
		fluBMappings.put("ha", "HA");
		fluBMappings.put("ha\"                     /gene_synonym=\"ha1\"                     /allele=\"4", "HA");
		fluBMappings.put("ns2", "NS");
		fluBMappings.put("ns1", "NS");
		fluBMappings.put("pb2", "PB2");
		fluBMappings.put("pa", "PA");
		fluBMappings.put("nb", "PB1");
		fluBMappings.put("polymerase acidic", "PA");
		//Flu C//
		Arrays.sort(fluCTaxons);
		fluCMappings.put("pb2", "PB2");
		fluCMappings.put("pb1", "PB1");
		fluCMappings.put("pa", "PA");
		fluCMappings.put("hef", "HEF");
		fluCMappings.put("np", "NP");
		fluCMappings.put("m", "M");
		fluCMappings.put("ns", "NS");
		fluCMappings.put("ns2", "NS");
		fluCMappings.put("ns1", "NS");
		fluCMappings.put("cm2", "M");
		fluCMappings.put("he", "HEF");
		fluCMappings.put("p3", "PA");
		fluCMappings.put("m1", "M");
		fluCMappings.put("m2", "M");
		fluCMappings.put("nep\"                     /gene_synonym=\"ns2", "NS");
		fluCMappings.put("np\"", "NP");
		fluCMappings.put("nep/ns2", "NS");
		fluCMappings.put("p42", "M");
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

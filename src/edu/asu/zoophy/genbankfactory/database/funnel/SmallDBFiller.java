package edu.asu.zoophy.genbankfactory.database.funnel;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.logging.Logger;

import edu.asu.zoophy.genbankfactory.database.GenBankFactory;
import edu.asu.zoophy.genbankfactory.database.GenBankRecord;


public class SmallDBFiller {
	
	private Logger log = Logger.getLogger("SmallDBFiller");
	private final String SMALL_DB;
	private final String LARGE_DB;
	private GenBankFactory fac = null;
	private List<GenBankRecord> recs = null;
	private int total = 0;
	private int curr = 0;
	
	public SmallDBFiller(String smallDBName, String largeDBName) {
		fac = GenBankFactory.getInstance();
		SMALL_DB = smallDBName;
		LARGE_DB = largeDBName;
	}
	
	public void fillDB(Queue<String> accessions) throws Exception {
		log.info("Switching to Big DB");
		fac.switchDB(LARGE_DB);
		recs = new LinkedList<GenBankRecord>();
		total = accessions.size();
		curr = 0;
		while (!accessions.isEmpty()) {
			String acc = accessions.remove();
			curr++;
			GenBankRecord rec = fac.findFullRecord(acc);
			if (rec != null && rec.getAccession() != null) {
				rec.getFeatures().clear();//unnecessary and resource intensive to insert features
				recs.add(rec);
			}
			else {
				System.err.println("Missing Accession: "+acc);
			}
			if (curr % 50000 == 0) {
				log.info("Pulled record "+curr+" of "+total);
				log.info("Switching to Small DB");
				fac.switchDB(SMALL_DB);
				log.info("Inserting records...");
				fac.insertUIonlyRecords(recs);
				recs.clear();
				log.info("Switching back to Big DB");
				fac.switchDB(LARGE_DB);
			}
		}
		if (recs.size() > 0) {
			log.info("Pull final record. Insterting last batch...");
			log.info("Switching to Small DB");
			fac.switchDB(SMALL_DB);
			log.info("Inserting records...");
			fac.insertUIonlyRecords(recs);
			recs.clear();
		}
		recs.clear();
		log.info("Successfully funneled "+curr+" records.");
	}
}

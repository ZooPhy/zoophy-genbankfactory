package edu.asu.zoophy.gbmetadataupdater.geonamelucene;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.search.spell.LevensteinDistance;
import org.apache.lucene.search.spell.SuggestWord;
;

public class SpellCheckComparator implements Comparator<SuggestWord>{

	LuceneSearcher searcher=null;
	String term=null;
	
	public SpellCheckComparator(LuceneSearcher searcher, String term) {
		this.searcher=searcher;
		this.term=term;
	}
	
	@Override
	public int compare(SuggestWord word1, SuggestWord word2) {
		Float score1 = word1.score + (1-word1.score)*permutationScore(word1.string, term);
		Float score2 = word2.score+(1-word2.score)*permutationScore(word2.string, term);
		int diff = score1.compareTo(score2);
		if(diff==0) {
			Integer freq1 = word1.freq;
			Integer freq2 = word2.freq;
			diff = freq1.compareTo(freq2);
		}
		return diff;
	}
	
	public float permutationScore(String a, String b)
    {
        char[] ac = a.toLowerCase().toCharArray();
        char[] bc = b.toLowerCase().toCharArray();

        Arrays.sort(ac);
        Arrays.sort(bc);

        a = new String(ac);
        b = new String(bc);

        LevensteinDistance l = new LevensteinDistance();
        return l.getDistance(a, b);
    }
}

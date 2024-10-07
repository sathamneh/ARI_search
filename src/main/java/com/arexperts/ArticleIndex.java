package com.arexperts;

import java.util.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ArticleIndex {
    private int n;
    private boolean hashKeys;
    private Map<String, List<String>> ngramTable;
    private Map<String, String> keyTable;
    private MessageDigest md;

    public ArticleIndex(int n, boolean hashKeys) {
        this.n = n;
        this.hashKeys = hashKeys;
        this.ngramTable = new HashMap<>();
        this.keyTable = new HashMap<>();
        try {
            this.md = MessageDigest.getInstance("SHA-256");
        } catch(NoSuchAlgorithmException ex)
        {

        }

    }

    public void addArticle(String s, String key) {
        Set<String> grams = getNGrams(s, n);
        String h = hash(key);
        keyTable.put(h, key);
        for (String g : grams) {
            ngramTable.computeIfAbsent(g, k -> new ArrayList<>()).add(h);
        }
    }

    public String[] findMatch(String s) {
        Set<String> grams = getNGrams(s, n);
        List<String> hits = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (String g : grams) {
            if (ngramTable.containsKey(g)) {
                List<String> found = ngramTable.get(g);
                hits.addAll(found);
                scores.addAll(Collections.nCopies(found.size(), 1.0 / found.size()));
            }
        }
        if (hits.isEmpty()) {
            return new String[]{null, "0"};
        }
        Map<String, Double> totals = new HashMap<>();
        for (int i = 0; i < hits.size(); i++) {
            totals.put(hits.get(i), totals.getOrDefault(hits.get(i), 0.0) + scores.get(i));
        }
        String maxKey = Collections.max(totals.entrySet(), Comparator.comparingDouble(Map.Entry::getValue)).getKey();
        return new String[]{keyTable.get(maxKey), String.valueOf(totals.get(maxKey))};
    }

    public Set<String> getNGrams(String s, int n) {
        String[] words = s.split("\\s+");
        Set<String> grams = new HashSet<>();
        for (int i = 0; i <= words.length - n; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < n; j++) {
                sb.append(words[i + j]).append(" ");
            }
            String gram = sb.toString().trim();
            if (hashKeys) {
                gram = hash(gram);
            }
            grams.add(gram);
        }
        return grams;
    }

    private String hash(String s) {
        // Try this instead https://stackoverflow.com/a/2624385/12570
        md.reset();
        md.update(s.getBytes());
        return new String(md.digest());
    }

    public  String validateMatch(String s1, String s2) {
        Set<String> ng1 = getNGrams(s1, 5);
        Set<String> ng2 = getNGrams(s2, 5);
        int inter = ng1.stream().filter(ng2::contains).mapToInt(x -> 1).sum();
        double score1 = (double) inter / ng1.size();
        double score2 = (double) inter / ng2.size();
        return "Matched with "+Integer.toOctalString(inter)+" score with intersection of "+(score1)+" and "+score2+". Verdict is "+(score1>0.5 && score2>0.5?true:false);
    }
}
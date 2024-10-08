package com.arexperts;

import java.util.*;
import java.nio.charset.StandardCharsets;

public class ArticleIndex {
    private int n;
    private boolean hashKeys;
    private Map<String, List<Integer>> ngramTable;
    private Map<Integer, String> keyTable;

    public ArticleIndex(int n, boolean hashKeys) {
        this.n = n;
        this.hashKeys = hashKeys;
        this.ngramTable = new HashMap<>();
        this.keyTable = new HashMap<>();
    }

    public void addArticle(String s, String key) {
        Set<String> grams = getNGrams(s, n);
        int h = key.hashCode();
        keyTable.put(h, key);
        for (String g : grams) {
            ngramTable.computeIfAbsent(g, k -> new ArrayList<>()).add(h);
        }
    }

    public String[] findMatch(String s) {
        Set<String> grams = getNGrams(s, n);
        List<Integer> hits = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (String g : grams) {
            List<Integer> found = ngramTable.get(g);
            if (found != null && !found.isEmpty()) {
                hits.addAll(found);
                double score = 1.0 / found.size();
                for (int i = 0; i < found.size(); i++) {
                    scores.add(score);
                }
            }
        }
        if (hits.isEmpty()) {
            return new String[] { null, "0" };
        }
        Map<Integer, Double> totals = new HashMap<>();
        for (int i = 0; i < hits.size(); i++) {
            int hit = hits.get(i);
            double score = scores.get(i);
            totals.put(hit, totals.getOrDefault(hit, 0.0) + score);
        }
        int maxKey = Collections.max(totals.entrySet(), Comparator.comparingDouble(Map.Entry::getValue)).getKey();
        double maxValue = totals.get(maxKey);
        return new String[] { keyTable.get(maxKey), String.valueOf(maxValue) };
    }

    public Set<String> getNGrams(String s, int n) {
        String[] words = s.split("\\s+");
        Set<String> grams = new HashSet<>();
        for (int i = 0; i <= words.length - n; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < n; j++) {
                sb.append(words[i + j]);
                if (j < n - 1) {
                    sb.append(" ");
                }
            }
            grams.add(sb.toString());
        }
        return grams;
    }

    public String validateMatch(String s1, String s2) {
        Set<String> ng1 = getNGrams(s1, 5);
        Set<String> ng2 = getNGrams(s2, 5);
        Set<String> intersection = new HashSet<>(ng1);
        intersection.retainAll(ng2);
        int inter = intersection.size();
        double score1 = (double) inter / ng1.size();
        double score2 = (double) inter / ng2.size();
        boolean isMatch = Math.max(score1, score2) > 0.2 && Math.min(score1, score2) > 0.1;
        return "Matched with " + inter + " score with intersection of " + score1 + " and " + score2 + ". Verdict is "
                + isMatch;
    }
}

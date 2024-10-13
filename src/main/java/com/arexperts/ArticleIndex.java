package com.arexperts;

import java.util.*;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.File;
import java.io.Serializable;

public class ArticleIndex implements Serializable {
    public static String SAVE_NAME = "article_index.ser";    
    private int n;
    private Map<Integer, List<Integer>> ngramTable;
    private Map<Integer, String> keyTable;
    private int articlesAdded = 0;

    public ArticleIndex(int n) {
        this.n = n;
        this.ngramTable = new HashMap<>();
        this.keyTable = new HashMap<>();
    }

    private void writeObject(Object object, String name)
    {
        try {
            FileOutputStream fos = new FileOutputStream(name);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(object);
            oos.close();
        }
        catch (Exception ex) {
            System.err.println("Unable to serialize " + name + ": " + ex.getLocalizedMessage());
        }
    }

    public static boolean isSaved()
    {
        File f = new File(SAVE_NAME);
        return f.exists() && !f.isDirectory(); 
    }

    public void save()
    {
        writeObject(this, SAVE_NAME);
    }

    private static Object readObject(String name)
    {
        Object theReadObject = null;
        try {
            FileInputStream fis = new FileInputStream(name);
            ObjectInputStream ois = new ObjectInputStream(fis);
            theReadObject =  ois.readObject();

            ois.close();
        }
        catch (Exception ex) {
            System.err.println("Unable to deserialize " + name + ": " + ex.getLocalizedMessage());
        }

        return theReadObject;
    }    
    public static ArticleIndex load()
    {
        return (ArticleIndex) readObject(SAVE_NAME);
    }
    
    public int NumberOfArticles() {
        return articlesAdded;
    }

    public void addArticle(String s, String key) {
        articlesAdded++;
        Set<Integer> grams = getNGrams(s, n);
        int h = key.hashCode();
        keyTable.put(h, key);
        for (Integer g : grams) {
            ngramTable.computeIfAbsent(g, k -> new ArrayList<>()).add(h);
        }
    }

    public String[] findMatch(String s) {        
        Set<Integer> grams = getNGrams(s, n);
        List<Integer> hits = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (Integer g : grams) {
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

    public Set<Integer> getNGrams(String s, int n) {
        return getNGrams(s, n, 1);
    }

    private Set<Integer> getNGrams(String s, int n, int skip) {
        String s_letters_only = s.replaceAll("[^a-zA-Z0-9\\s+]", "").toLowerCase();
        String[] words = s_letters_only.split("\\s+");
        Set<Integer> grams = new HashSet<>();
        for (int i = 0; i <= words.length - n; i += skip) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < n; j++) {
                sb.append(words[i + j]);
                if (j < n - 1) {
                    sb.append(" ");
                }
            }
            Integer hashCode = sb.toString().hashCode();
            grams.add(hashCode);
        }
        return grams;
    }

    public String validateMatch(String s1, String s2) {
        Set<Integer> ng1 = getNGrams(s1, 5);
        Set<Integer> ng2 = getNGrams(s2, 5);
        Set<Integer> intersection = new HashSet<>(ng1);
        intersection.retainAll(ng2);
        int inter = intersection.size();
        double score1 = (double) inter / ng1.size();
        double score2 = (double) inter / ng2.size();
        boolean isMatch = Math.max(score1, score2) > 0.5 && Math.min(score1, score2) > 0.2;
        return "Matched with " + inter + " score with intersection of " + score1 + " and " + score2 + ". Verdict is "
                + isMatch;
    }
}

package com.arexperts;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


public class Searcher {

    private ArticleIndex index;
    private int threads;
    private ConcurrentHashMap<String, AtomicInteger> checkedFiles = new ConcurrentHashMap<>();
    private BufferedWriter writer = null;
    private AtomicInteger articlesSearched = new AtomicInteger(0);

    public Searcher(ArticleIndex index, int threadsToUse, String watchDirectory) {
        this.index = index;
        this.threads = threadsToUse;

        File[] files = CSVReader.getFileList(watchDirectory);

        for (File oneFile : files) {            
            checkedFiles.put(oneFile.getAbsolutePath(), new AtomicInteger(0));
        }

    }

    public void search() {
        try 
        {
            if (writer == null)
                writer = new BufferedWriter(new FileWriter("results_output.csv"));
        } 
        catch (IOException ex) {
            System.err.println("Cannot open CSV result file : " + ex.getLocalizedMessage());
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(threads);

        while (getNextFile().isPresent())
        {
            executor.submit(() -> 
            {
                if (getNextFile().isPresent())
                {
                    String fileToSearch = getNextFile().get();
                    checkedFiles.put(fileToSearch, new AtomicInteger(1));
    
                    String[] articles = CSVReader.loadArticles(fileToSearch, 6);
    
                    for (String oneArticle : articles) {
                        String[] result = index.findMatch(oneArticle);
                        try 
                        {
                            writer.write(fileToSearch + "," + result[0] + "," + result[1] + "\n");
                            articlesSearched.incrementAndGet();
                        }
                        catch (IOException ex) {
                            System.err.println("Problem writing result for " + fileToSearch + " : "  + ex.getLocalizedMessage());                        
                        }
                    }
                }
            });    
        }

        System.out.println("Done!");
        executor.shutdown();
        try 
        {
            executor.awaitTermination(100, TimeUnit.SECONDS);
            writer.flush();
            writer.close();
        }
        catch (InterruptedException ex) {
            System.err.println("Error: " + ex.getLocalizedMessage());
        }
        catch (IOException ex) {
            System.err.println("Error closing writer: " + ex.getLocalizedMessage());
        }
    }

    private Optional<String> getNextFile() {
        return checkedFiles.entrySet().stream().filter(e -> e.getValue().get() == 0).map(Map.Entry::getKey).findFirst();

    }

    public int checkedArticles() {
        return articlesSearched.get();
    }

    
}

package com.arexperts;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class Searcher {

    public static int TIMEOUT_PERIOD_IN_SECONDS = 120; // Make this lower if you want to benchmark search speed.
    private ArticleIndex index;
    private int threads;
    private ConcurrentHashMap<String, AtomicInteger> checkedFiles = new ConcurrentHashMap<>();
    private BufferedWriter writer = null;
    private AtomicInteger articlesSearched = new AtomicInteger(0);
    private String watchDirectory;
    private double lastTimeNewFileFoundInSeconds = System.nanoTime() / 1_000_000_000.0;
    private AtomicBoolean newFilesNotFoundAlreadyReported = new AtomicBoolean(false);

    public Searcher(ArticleIndex index, int threadsToUse, String watchDirectory) {
        this.index = index;
        this.threads = threadsToUse;
        this.watchDirectory = watchDirectory;

        updateCheckedFilesList();
    }

    private int updateCheckedFilesList() {
        int newFilesFound = 0;
        File[] files = CSVReader.getFileList(watchDirectory);

        for (File oneFile : files) {
            if (!checkedFiles.containsKey(oneFile.getAbsolutePath()))            
            {
                checkedFiles.put(oneFile.getAbsolutePath(), new AtomicInteger(0));
                lastTimeNewFileFoundInSeconds = System.nanoTime() / 1_000_000_000.0;
                newFilesFound++;
            }
        }

        return newFilesFound;
    }

    public void search() {
        try 
        {
            if (writer == null)
            {
                writer = new BufferedWriter(new FileWriter("results_output.csv"));
            }
        } 
        catch (IOException ex) {
            System.err.println("Cannot open CSV result file : " + ex.getLocalizedMessage());
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(threads);

        while ((System.nanoTime() / 1_000_000_000.0 - lastTimeNewFileFoundInSeconds) < TIMEOUT_PERIOD_IN_SECONDS)
        {
            executor.submit(() -> 
            {
                if (getNextFile().isPresent())
                {
                    processFile();
                }
                else
                {
                    checkForNewFiles();
                }
            });

            try 
            {
                // Wait a bit to stop the executor's queue from filling up too much.
                Thread.sleep(20);
            }
            catch (Exception ex) {
                System.err.println("Waiting for checking files caused:" + ex.getLocalizedMessage());
            }
        }

        System.out.println("No longer checking for new files to scan.");
        executor.shutdown();
        try 
        {
            executor.awaitTermination(30, TimeUnit.SECONDS);
            System.out.println("Thread timeout finished.");
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

    private void checkForNewFiles() {
        int newFileCount = 0;
        if ((newFileCount = updateCheckedFilesList()) > 0)
        {
            System.out.println(newFileCount + " new files found.");
        }
        else
        {
            if (!newFilesNotFoundAlreadyReported.get())
            {
                System.out.println(Thread.currentThread().getName() + " : No new files found.");
            }
            newFilesNotFoundAlreadyReported.set(true);
        }
    }

    private void processFile() 
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
        System.out.println(Thread.currentThread().getName() + " : " + fileToSearch + ": processed " + articles.length + " articles.");
    }

    private Optional<String> getNextFile() {
        return checkedFiles.entrySet().stream().filter(e -> e.getValue().get() == 0).map(Map.Entry::getKey).findFirst();
    }

    public int checkedArticles() {
        return articlesSearched.get();
    }    
}

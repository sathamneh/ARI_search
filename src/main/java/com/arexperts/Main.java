package com.arexperts;

//   Developed by Varad Shinde @ October 5 2024  for ARI  Inc.  for further communication mail:varad@live.in 

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Main {

    public final static long startTime = System.nanoTime();
    public static void main(String[] args) {
        
        try (BufferedReader br = new BufferedReader(new FileReader("input.txt"))) {
            // Initialize variables
            String directoryPath = null;
            String watchDirectory = null;
            String jsonTextField = null;
            String jsonIDField = null;
            String prefixSeparator = null;
            String suffixSeparator = null;

            int columnIndexByte = 0;
            int threadCount = 1;
            int filesToProcess = 1154;
            int offsetFileNumber = 0;
            int nGramLength = 5;
            int maximumNumberOfNGrams = 100000;
            int scoreThreshold = 0; // Output all scores by default.
            int bufferSize = 1_000_000_000;
            int filesPerDump = 100;

            String line;
            String nextLine = null;
            while ((line = (nextLine != null ? nextLine : br.readLine())) != null) {
                nextLine = null;
                line = line.trim();
                if (line.startsWith("Folderpath:")) {
                    directoryPath = line.substring(11).trim().replaceAll("\"", "").replaceAll(":", "");
                } 
                else if (line.startsWith("Watchdirectory:")) {
                    watchDirectory = line.substring(14).trim().replaceAll("\"", "").replaceAll(":", "");
                } 
                else if (line.startsWith("JSONtextfield:")) {
                    jsonTextField = line.substring(13).trim().replaceAll("\"", "").replaceAll(":", "");
                }
                else if (line.startsWith("JSONIDfield:")) {
                    jsonIDField = line.substring(11).trim().replaceAll("\"", "").replaceAll(":", "");
                }
                else if (line.startsWith("Prefixseparator:")) {
                    prefixSeparator = line.substring(15).trim().replaceAll("\"", "").replaceAll(":", "");
                }
                else if (line.startsWith("Suffixseparator:")) {
                    suffixSeparator = line.substring(15).trim().replaceAll("\"", "").replaceAll(":", "");
                }
                else if (line.startsWith("InputColumnIndex2:")) {
                    try {
                        columnIndexByte = Integer.parseInt(line.substring(17).trim().replaceAll(":", ""));
                    }
                    catch (NumberFormatException e) {
                        System.err.println("Invalid number format for InputColumnIndex2");
                    }
                }
                else if (line.startsWith("threadcount:")) {
                    try {
                        threadCount = Integer.parseInt(line.substring(11).trim().replaceAll(":", ""));
                    }
                    catch (NumberFormatException e) {
                        System.err.println("Invalid number format for threadcount");
                    }
                }
                else if (line.startsWith("files_to_process:")) {
                    try {
                        filesToProcess = Integer.parseInt(line.substring(16).trim().replaceAll(":", ""));
                    }
                    catch (NumberFormatException e) {
                        System.err.println("Invalid number format for files_to_process");
                    }
                }
                else if (line.startsWith("offset_file_number:")) {
                    try {
                        offsetFileNumber = Integer.parseInt(line.substring(18).trim().replaceAll(":", ""));
                    }
                    catch (NumberFormatException e) {
                        System.err.println("Invalid number format for offset_file_number");
                    }
                }
                else if (line.startsWith("ngram_length:")) {
                    try {
                        nGramLength = Integer.parseInt(line.substring(12).trim().replaceAll(":", ""));
                    }
                    catch (NumberFormatException e) {
                        System.err.println("Invalid number format for ngram_length");
                    }
                }
                else if (line.startsWith("maximum_number_of_ngrams:")) {
                    try {
                        maximumNumberOfNGrams = Integer.parseInt(line.substring(24).trim().replaceAll(":", ""));
                    }
                    catch (NumberFormatException e) {
                        System.err.println("Invalid number format for maximum_number_of_ngrams");
                    }
                }
                else if (line.startsWith("score_threshold:")) {
                    try {
                        scoreThreshold = Integer.parseInt(line.substring(15).trim().replaceAll(":", ""));
                    }
                    catch (NumberFormatException e) {
                        System.err.println("Invalid number format for maximum_number_of_ngrams");
                    }
                }
                else if (line.startsWith("Buffersize:")) {
                    try {
                        bufferSize = Integer.parseInt(line.substring(10).trim().replaceAll(":", ""));
                    }
                    catch (NumberFormatException e) {
                        System.err.println("Invalid number format for Buffersize");
                    }
                }
                else if (line.startsWith("Filesperdump:")) {
                    try {
                        filesPerDump = Integer.parseInt(line.substring(12).trim().replaceAll(":", ""));
                    }
                    catch (NumberFormatException e) {
                        System.err.println("Invalid number format for Buffersize");
                    }
                }
            }
            System.out.println("Process started with the following parameters:");
            System.out.println("Ingestion directory              : " + directoryPath);
            System.out.println("# files to ingest                : " + filesToProcess);
            System.out.println("# files to skip before ingestion : " + offsetFileNumber);
            System.out.println("nGram length                     : " + nGramLength);
            System.out.println("Max # ngrams                     : " + maximumNumberOfNGrams);
            System.out.println("Score threshold                  : " + scoreThreshold);            
            System.out.println("Folder to scan                   : " + watchDirectory);
            System.out.println("CSV column to search             : " + columnIndexByte);
            System.out.println("JSON text field to use           : " + jsonTextField);
            System.out.println("JSON ID field to use             : " + jsonIDField);
            System.out.println("TXT prefix                       : " + prefixSeparator);
            System.out.println("TXT suffix                       : " + suffixSeparator);
            System.out.println("Buffer size                      : " + bufferSize);
            System.out.println("Files per state save             : " + filesPerDump);
            

            ArticleIndex articles = CSVReader.loadNGramsFromCSVFiles(directoryPath, columnIndexByte, filesToProcess, offsetFileNumber, nGramLength, maximumNumberOfNGrams);
            System.out.println("Time to load data from files : " + getElapsedTime() + "s");

            System.out.println("Using " + articles.NumberOfArticles() + " articles for match.");

            double startOfSearch = System.nanoTime() / 1_000_000_000.0;
            Searcher searcher = new Searcher(articles, threadCount, watchDirectory, columnIndexByte, jsonTextField, jsonIDField, prefixSeparator, suffixSeparator, scoreThreshold, bufferSize, filesPerDump);
            searcher.search();
            System.out.println("Time taken for threaded search is " + (System.nanoTime() / 1_000_000_000.0 - startOfSearch) + "s for " + searcher.checkedArticles() + " articles.");

        }
        catch (IOException e) {
            System.err.println("Error reading input file: " + e.getMessage());
        }

        System.out.println("Total execution time: " + getElapsedTime() + "s");
    }

    private static double getElapsedTime()
    {        
        return (System.nanoTime() - startTime) / 1_000_000_000.0; // Converts to seconds
    }
}
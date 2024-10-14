package com.arexperts;

// Developed by Varad Shinde @ October 5, 2024 for ARI Inc. for further communication mail: varad@live.in

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Main {

    public final static long startTime = System.nanoTime();

    public static void main(String[] args) {

        try (BufferedReader br = new BufferedReader(new FileReader("input.txt"))) {
            // Initialize variables
            String processFolderPath = null;
            String processOutputFilePath = null;
            String processorType = null; // "json" or "text"
            String textField = null;
            String urlField = null;
            int columnIndexByte = 0; // For CSVReader
            int threadCount = 1;
            String prefixSeparator = null;
            String suffixSeparator = null;
            int filesToProcess = 1154;
            int offsetFileNumber = 0;
            int nGramLength = 5;

            // Read input.txt parameters
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                // Process-specific configurations
                if (line.startsWith("process.folderPath:")) {
                    processFolderPath = line.substring(18).trim().replaceAll("\"", "").replaceAll(":", "");
                } else if (line.startsWith("process.outputPath:")) {
                    processOutputFilePath = line.substring(18).trim().replaceAll("\"", "").replaceAll(":", "");
                } else if (line.startsWith("process.processor:")) {
                    processorType = line.substring(18).trim().replaceAll("\"", "").replaceAll(":", "");
                } else if (line.startsWith("process.threadCount:")) {
                    try {
                        threadCount = Integer.parseInt(line.substring(19).trim().replaceAll(":", ""));
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid number format for process.threadCount");
                    }
                } else if (line.startsWith("process.urlField:")) {
                    urlField = line.substring(16).trim().replaceAll("\"", "").replaceAll(":", "");
                } else if (line.startsWith("process.textField:")) {
                    textField = line.substring(17).trim().replaceAll("\"", "").replaceAll(":", "");
                } else if (line.startsWith("process.prefixSeparator:")) {
                    prefixSeparator = line.substring(22).trim().replaceAll("\"", "").replaceAll(":", "");
                } else if (line.startsWith("process.suffixSeparator:")) {
                    suffixSeparator = line.substring(22).trim().replaceAll("\"", "").replaceAll(":", "");
                } else if (line.startsWith("process.filesToProcess:")) {
                    try {
                        filesToProcess = Integer.parseInt(line.substring(21).trim().replaceAll(":", ""));
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid number format for process.filesToProcess");
                    }
                } else if (line.startsWith("process.offsetFileNumber:")) {
                    try {
                        offsetFileNumber = Integer.parseInt(line.substring(24).trim().replaceAll(":", ""));
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid number format for process.offsetFileNumber");
                    }
                } else if (line.startsWith("process.ngramLength:")) {
                    try {
                        nGramLength = Integer.parseInt(line.substring(19).trim().replaceAll(":", ""));
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid number format for process.ngramLength");
                    }
                } else if (line.startsWith("process.inputColumnIndex2:")) {
                    try {
                        columnIndexByte = Integer.parseInt(line.substring(23).trim().replaceAll(":", ""));
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid number format for process.inputColumnIndex2");
                    }
                }
            }

            // Output processing parameters
            System.out.println("Process started with the following parameters:");
            System.out.println("Directory path    : " + processFolderPath);
            System.out.println("Thread count      : " + threadCount);
            System.out.println("Files to process  : " + filesToProcess);
            System.out.println("Offset file number: " + offsetFileNumber);
            System.out.println("nGram length      : " + nGramLength);

            // Load or create ArticleIndex
            ArticleIndex articles;
            if (ArticleIndex.isSaved()) {
                System.out.println("Loading data from " + ArticleIndex.SAVE_NAME);
                articles = ArticleIndex.load();
                System.out.println("Time to load saved data: " + getElapsedTime() + "s");
            } else {
                System.out.println(ArticleIndex.SAVE_NAME + " does not exist. Processing files.");
                articles = CSVReader.loadFiles(processFolderPath, columnIndexByte, filesToProcess, offsetFileNumber, nGramLength);
                System.out.println("File processing finished. Saving to disk.");
                System.out.println("Time to load new data: " + getElapsedTime() + "s");
                articles.save();
                System.out.println("Saving finished.");
                System.out.println("Time when new data is saved: " + getElapsedTime() + "s");
            }

            // Execute based on processorType after loading ArticleIndex
            if (processorType == null) {
                System.err.println("No processor type specified.");
                return;
            }

            if (processorType.equalsIgnoreCase("json")) {
                System.out.println("Running JsonMatcher...");
                runJsonMatcher(processFolderPath, processOutputFilePath, threadCount, urlField, textField, articles);
            } else if (processorType.equalsIgnoreCase("text")) {
                System.out.println("Running TextExtractor...");
                runTextExtractor(processFolderPath, processOutputFilePath, threadCount, prefixSeparator, suffixSeparator);
            } else {
                System.err.println("Invalid processor type. Use 'json' or 'text'.");
            }

            // Matches (relevant for the json processor)
            if (articles != null && processorType.equalsIgnoreCase("json")) {
                String[] matches = articles.findMatch("Sample Text for Matching");
                for (String matched : matches) {
                    System.out.println(matched);
                }
            }

            System.out.println("Total execution time: " + getElapsedTime() + "s");
        } catch (IOException e) {
            System.err.println("Error reading input file: " + e.getMessage());
        }
    }

    // Run JsonMatcher
    private static void runJsonMatcher(String directoryPath, String outputFilePath, int threadCount,
                                       String urlField, String textField, ArticleIndex articleIndex) {
        if (directoryPath == null || outputFilePath == null || urlField == null || textField == null) {
            System.err.println("Missing parameters for JsonMatcher.");
            return;
        }
        JsonMatcher.processJsonFiles(directoryPath, outputFilePath, threadCount, urlField, textField, articleIndex);
    }

    // Run TextExtractor
    private static void runTextExtractor(String directoryPath, String outputFilePath, int threadCount,
                                         String prefixSeparator, String suffixSeparator) {
        if (directoryPath == null || outputFilePath == null || prefixSeparator == null || suffixSeparator == null) {
            System.err.println("Missing parameters for TextExtractor.");
            return;
        }
        TextExtractor.processTextFiles(directoryPath, outputFilePath, threadCount, prefixSeparator, suffixSeparator);
    }

    private static double getElapsedTime() {
        return (System.nanoTime() - startTime) / 1_000_000_000.0; // Converts to seconds
    }
}

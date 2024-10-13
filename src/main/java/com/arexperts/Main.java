package com.arexperts;

//   Developed by Varad Shinde @ October 5 2024  for ARI  Inc.  for further communication mail:varad@live.in 

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Main {

    public final static long startTime = System.nanoTime();
    public static void main(String[] args) {
        
        try (BufferedReader br = new BufferedReader(new FileReader("input.txt"))) {
            // Initialize variables
            String directoryPath = null;
            String columnNameUrl = null;
            int columnIndexUrl = 0;
            String comparisonValueUrl = null;
            String columnNameByte = null;
            int columnIndexByte = 0;
            String str1 = null;
            int outputColumn = 0;
            int threadCount = 1;
            int filesToProcess = 1154;
            int offsetFileNumber = 0;
            int nGramLength = 5;

            // Define parameter prefixes
            Set<String> parameterPrefixes = new HashSet<>(Arrays.asList("Folderpath:", "InputColumnname1:",
                    "InputColumnindex1:", "Comparisonvalue1:", "InputColumnName2:", "InputColumnIndex2:",
                    "Comparisonvalue2:", "OutputColumnIndex:", "threadcount:"));

            String line;
            String nextLine = null;
            while ((line = (nextLine != null ? nextLine : br.readLine())) != null) {
                nextLine = null;
                line = line.trim();
                if (line.startsWith("Folderpath:")) {
                    directoryPath = line.substring(11).trim().replaceAll("\"", "").replaceAll(":", "");
                }
                else if (line.startsWith("InputColumnname1:")) {
                    columnNameUrl = line.substring(17).trim().replaceAll("\"", "").replaceAll(":", "");
                }
                else if (line.startsWith("InputColumnindex1:")) {
                    try {
                        columnIndexUrl = Integer.parseInt(line.substring(17).trim().replaceAll(":", ""));
                    }
                    catch (NumberFormatException e) {
                        System.err.println("Invalid number format for InputColumnindex1");
                    }
                }
                else if (line.startsWith("Comparisonvalue1:")) {
                    comparisonValueUrl = line.substring(16).trim().replaceAll("\"", "").replaceAll(":", "");
                }
                else if (line.startsWith("InputColumnName2:")) {
                    columnNameByte = line.substring(17).trim().replaceAll("\"", "").replaceAll(":", "");
                }
                else if (line.startsWith("InputColumnIndex2:")) {
                    try {
                        columnIndexByte = Integer.parseInt(line.substring(17).trim().replaceAll(":", ""));
                    }
                    catch (NumberFormatException e) {
                        System.err.println("Invalid number format for InputColumnIndex2");
                    }
                }
                else if (line.startsWith("Comparisonvalue2:")) {
                    StringBuilder sb = new StringBuilder();
                    String value = line.substring(16).trim().replaceAll("\"", "").replaceAll(":", "");
                    sb.append(value);
                    while ((nextLine = br.readLine()) != null) {
                        nextLine = nextLine.trim();
                        if (isParameterLine(nextLine, parameterPrefixes)) {
                            // It's a new parameter, process it in the next iteration
                            break;
                        }
                        else {
                            sb.append(System.lineSeparator());
                            sb.append(nextLine);
                            nextLine = null; // Continue reading lines
                        }
                    }
                    str1 = sb.toString();
                    // If nextLine is a parameter line, it will be processed in the next iteration
                }
                else if (line.startsWith("OutputColumnIndex:")) {
                    try {
                        outputColumn = Integer.parseInt(line.substring(17).trim().replaceAll(":", ""));
                    }
                    catch (NumberFormatException e) {
                        System.err.println("Invalid number format for OutputColumnIndex");
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
            }
            System.out.println("Process started with the following parameters:");
            System.out.println("Directory path    : " + directoryPath);
            System.out.println("Thread count      : " + threadCount);
            System.out.println("Files to process  : " + filesToProcess);
            System.out.println("Offset file number: " + offsetFileNumber);
            System.out.println("nGram length      : " + nGramLength);
            // CSVReader.processFiles(directoryPath, columnNameUrl, columnIndexUrl, comparisonValueUrl, columnNameByte,
            //         columnIndexByte, str1, outputColumn, threadCount);

            ArticleIndex articles;
            if (ArticleIndex.isSaved())
            {
                System.out.println("Loading data from " + ArticleIndex.SAVE_NAME);
                articles = ArticleIndex.load();
                System.out.println("Time to load saved data: " + getElapsedTime() + "s");
            }
            else
            {
                System.out.println(ArticleIndex.SAVE_NAME + " does not exist. Processing files.");
                articles = CSVReader.loadFiles(directoryPath, columnIndexByte, filesToProcess, offsetFileNumber, 5);
                System.out.println("File processing finished. Saving to disk.");
                System.out.println("Time to load new data: " + getElapsedTime() + "s");
                articles.save();
                System.out.println("Saving finished.");
                System.out.println("Time when new data is saved: " + getElapsedTime() + "s");
            }

            System.out.println("Using " + articles.NumberOfArticles() + " articles for match.");

            String[] matches = articles.findMatch(str1);

            for (String matched : matches) {
                System.out.println(matched);
            }
        }
        catch (IOException e) {
            System.err.println("Error reading input file: " + e.getMessage());
        }

        System.out.println("Total execution time: " + getElapsedTime() + "s");
    }

    // Helper method to check if a line is a parameter line
    private static boolean isParameterLine(String line, Set<String> parameterPrefixes) {
        for (String prefix : parameterPrefixes) {
            if (line.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static double getElapsedTime()
    {        
        return (System.nanoTime() - startTime) / 1_000_000_000.0; // Converts to seconds
    }
}
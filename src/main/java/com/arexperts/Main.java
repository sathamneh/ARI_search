package com.arexperts;

//   Developed by Varad Shinde @ October 5 2024  for ARI  Inc.  for further communication mail:varad@live.in 

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Main {

    public final static long startTime = System.nanoTime();
    public static void main(String[] args) {
        
        try (BufferedReader br = new BufferedReader(new FileReader("input.txt"))) {
            // Initialize variables
            String directoryPath = null;
            int columnIndexByte = 0;
            String str1 = null;
            int threadCount = 1;
            int filesToProcess = 1154;
            int offsetFileNumber = 0;
            int nGramLength = 5;
            int maximumNumberOfNGrams = 100000;

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
            }
            System.out.println("Process started with the following parameters:");
            System.out.println("Directory path    : " + directoryPath);
            System.out.println("Thread count      : " + threadCount);
            System.out.println("Files to process  : " + filesToProcess);
            System.out.println("Offset file number: " + offsetFileNumber);
            System.out.println("nGram length      : " + nGramLength);
            System.out.println("Max # ngrams      : " + maximumNumberOfNGrams);

            ArticleIndex articles = CSVReader.loadFiles(directoryPath, columnIndexByte, filesToProcess, offsetFileNumber, nGramLength, maximumNumberOfNGrams);
            System.out.println("Time to load data from files : " + getElapsedTime() + "s");

            System.out.println("Using " + articles.NumberOfArticles() + " articles for match.");

            String[] matches = articles.findMatch(str1);

            System.out.println("File " + matches[0] + " matched best with " + Double.parseDouble(matches[1])/maximumNumberOfNGrams );

            String[] articlesToCheck = CSVReader.loadArticles("/Volumes/Macintosh HD - Data/Users/kootsoop/Downloads/Machina_NYT - Vol 8 Stories/NYT_00192705.CSV", columnIndexByte);

            double startOfSearch = System.nanoTime()/1_000_000_000.0;
            try (FileWriter writer = new FileWriter("results.csv")) {

                for (String oneArticle : articlesToCheck) 
                {
                    matches = articles.findMatch(oneArticle);
                    writer.write(matches[0] + "," + Double.parseDouble(matches[1])/maximumNumberOfNGrams + "\n");                    
                }         

                writer.flush();
            }
            catch (IOException e) {
                System.err.println("Error reading writing file: " + e.getMessage());
            }
                
            System.out.println("Time taken is " + (System.nanoTime() / 1_000_000_000.0 - startOfSearch) + "s for " + articlesToCheck.length + " articles.");
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
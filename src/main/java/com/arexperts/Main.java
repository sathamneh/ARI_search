package com.arexperts;

//   Developed by Varad Shinde @ October 5 2024  for ARI  Inc.  for further communication mail:varad@live.in 

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        long startTime = System.nanoTime();
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
            }
            System.out.println("Process started with the following parameters:");
            System.out.println("Directory path: " + directoryPath);
            System.out.println("Thread count: " + threadCount);
            CSVReader.processFiles(directoryPath, columnNameUrl, columnIndexUrl, comparisonValueUrl, columnNameByte,
                    columnIndexByte, str1, outputColumn, threadCount);
        }
        catch (IOException e) {
            System.err.println("Error reading input file: " + e.getMessage());
        }

        long endTime = System.nanoTime();
        double executionTime = (endTime - startTime) / 1_000_000_000.0; // Converts to seconds
        System.out.println("Execution time: " + executionTime + "s");
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
}
package com.arexperts;

//   Developed by Varad Shinde @ October 5 2024  for ARI  Inc.  for further communication mail:varad@live.in 

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        Long startTime = System.nanoTime();
        // Create an instance of the AnagramChecker class
        try (BufferedReader br = new BufferedReader(new FileReader("input.txt"))) {
            String directoryPath = null;
            String columnNameUrl = null;
            int columnIndexUrl = 0;
            String comparisonValueUrl = null;
            String columnNameByte = null;
            int columnIndexByte = 0;
            String str1 = null;
            int outputcolumn = 0;

            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("Folderpath:")) {
                    directoryPath = line.substring(11).trim().replaceAll("\"", "").replaceAll(":", "");
                }
                else if (line.startsWith("InputColumnname1:")) {
                    columnNameUrl = line.substring(17).trim().replaceAll("\"", "").replaceAll(":", "");
                }
                else if (line.startsWith("InputColumnindex1:")) {
                    columnIndexUrl = Integer.parseInt(line.substring(17).trim().replaceAll(":", ""));
                }
                else if (line.startsWith("Comparisonvalue1:")) {
                    comparisonValueUrl = line.substring(16).trim().replaceAll("\"", "").replaceAll(":", "");
                }
                else if (line.startsWith("InputColumnName2:")) {
                    columnNameByte = line.substring(17).trim().replaceAll("\"", "").replaceAll(":", "");
                }
                else if (line.startsWith("InputColumnIndex2:")) {
                    columnIndexByte = Integer.parseInt(line.substring(17).trim().replaceAll(":", ""));
                }
                else if (line.startsWith("Comparisonvalue2:")) {
                    str1 = line.substring(16).trim().replaceAll("\"", "").replaceAll(":", "");
                }
                else if (line.startsWith("OutputColumnIndex:")) {
                    outputcolumn = Integer.parseInt(line.substring(17).trim().replaceAll(":", ""));
                }
            }
            CSVReader.processFiles(directoryPath, columnNameUrl, columnIndexUrl, comparisonValueUrl, columnNameByte,
                    columnIndexByte, str1, outputcolumn, 8);
        }
        catch (IOException e) {
            System.err.println("Error reading input file: " + e.getMessage());
        }

        Long endTime = System.nanoTime();
        Float executionTime = (float) (endTime - startTime) / 1_000_000000; // Converts to
                                                                            // mvalues[columnurl].trim().contains(urlvalue)
                                                                            // milliseconds
        System.out.println("Execution time: " + executionTime + "s");
    }
}
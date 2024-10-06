package com.arexperts;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class CSVReader {
    public static boolean hasColumn(String filePath, String columnName, int columnIndex) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line = br.readLine(); // read the header row
            String[] headers = line.split(",");
            if (columnIndex >= headers.length) {
                return false;
            }
            return headers[columnIndex].trim().equals(columnName);
        } catch (IOException e) {
            System.err.println("Error reading CSV file: " + e.getMessage());
            return false;
        }
    }

  

    public static void comparengram(String filePath, String columnName, int columnIndex, String comparisonValue, BufferedWriter bw,int columnurl,int outputcolumnindex,String urlvalue) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line = br.readLine(); // read the header row

            bw.write(filePath+ "\n");
            bw.flush();
            String[] headers = line.split(",");
            if (columnIndex >= headers.length) {
                return;
            }
            
            if (!headers[columnIndex].trim().equals(columnName)) {
                return;
            }
            ArticleIndex article = new ArticleIndex(10,true);
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

                // String[] values = line.split(",");
                if (values.length <= columnIndex) {

                    continue;
                }
                  
                String column = values[columnIndex].trim();
                String result = article.validateMatch( comparisonValue,column);
                if (urlvalue.contains("None"))
                {
                    if ( result.contains("true")) 
                    {
                        if(outputcolumnindex!=0) 
                        {
                            bw.write(values[outputcolumnindex].trim() + "," + result + "\n");
                        }
                        else 
                        {
                            bw.write("0" + "," + result + "\n");
                        }
                        bw.flush();    
                    }
                }
                else
                {
                    if (values[columnurl].trim().contains(urlvalue) && result.contains("true")) 
                    {
                        if(outputcolumnindex!=0) 
                        {
                            bw.write(values[outputcolumnindex].trim() + "," + result + "\n");
                        }
                        else 
                        {
                            bw.write("0" + "," + result + "\n");
                        }    
                        bw.flush();
                    }
                }
            }
        }
    }

    public static void processFiles(String directoryPath, String columnName, int columnIndex, String comparisonValue,String column,int index,String value,int op) {
        File directory = new File(directoryPath);
        if (!directory.exists() || !directory.isDirectory()) {
            System.err.println("Invalid directory path");
            return;
        }
        File[] files = directory.listFiles();
        if (files == null) {
            System.err.println("No files found in directory");
            return;
        }
        try (BufferedWriter bw = new BufferedWriter(new FileWriter("result/intersection.csv"))) {
            bw.write("Directory "+directoryPath+ "\n" + "InputColumn1 "+columnName  +"\n"+ "InputColumn1Index "+columnIndex + "\n"+"Comparisonvalue1 "+ comparisonValue +"\n"+ "InputColumn2 "+column+"\n"+"InputColumn2Index " +index +"\n"+"Comparisonvalue2 "+ value+"\n"+ "`OutputColumnIndex "+op+"\n");
            bw.write("Index,Result\n"); // write header
            bw.flush();
            for (File file : files) {
                if (file.getName().endsWith(".CSV")) {
                    if (hasColumn(file.getAbsolutePath(), columnName, columnIndex)) {
                        comparengram(file.getAbsolutePath(), column, index, value, bw,columnIndex,op,comparisonValue);
                    } else {
                        System.err.println("Column " + columnName + " not found!");
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading or writing CSV file: " + e.getMessage());
        }
    }

  
}
package com.arexperts;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.FileWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CSVReader {

    public static boolean hasColumn(String filePath, String columnName, int columnIndex) {
        try (Reader reader = new FileReader(filePath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {
            return csvParser.getHeaderMap().containsKey(columnName);
        }
        catch (IOException e) {
            System.out.println("Error reading CSV file: " + e.getMessage());
            return false;
        }
    }

    public static void comparengram(String filePath, String columnName, int columnIndex, String comparisonValue,
            BufferedWriter bw, int columnurl, int outputcolumnindex, String urlvalue, AtomicInteger totalRecords)
            throws IOException {
        try (Reader reader = new FileReader(filePath);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {

            String fileName = new File(filePath).getName();
            synchronized (bw) {
                bw.write(fileName + "\n");
                bw.flush();
            }

            ArticleIndex article = new ArticleIndex(10, true);
            for (CSVRecord record : csvParser) {
                totalRecords.incrementAndGet();
                if (record.size() <= columnIndex) {
                    continue;
                }

                String column = record.get(columnIndex).trim();
                String result = article.validateMatch(comparisonValue, column);
                synchronized (bw) {
                    if (urlvalue.contains("None")) {
                        if (result.contains("true")) {
                            if (outputcolumnindex != 0) {
                                bw.write(fileName + "," + record.get(outputcolumnindex).trim() + "," + result + "\n");
                            }
                            else {
                                bw.write(fileName + ",0," + result + "\n");
                            }
                            bw.flush();
                        }
                    }
                    else {
                        if (record.get(columnurl).trim().contains(urlvalue) && result.contains("true")) {
                            if (outputcolumnindex != 0) {
                                bw.write(fileName + "," + record.get(outputcolumnindex).trim() + "," + result + "\n");
                            }
                            else {
                                bw.write(fileName + ",0," + result + "\n");
                            }
                            bw.flush();
                        }
                    }
                }
            }
        }
    }

    public static void processFiles(String directoryPath, String columnName, int columnIndex, String comparisonValue,
            String column, int index, String value, int op, int threadCount) {
        File directory = new File(directoryPath);
        if (!directory.exists()){
            System.out.println("Given path '" + directoryPath + "' does not exist.");
            return;
        }
        if (!directory.isDirectory()) {
            System.out.println("Given path '" + directoryPath + "' is not a directory.");
            return;
        }
        File[] files = directory.listFiles();
        if (files == null) {
            System.out.println("No files found in directory");
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        BufferedWriter bw = null;
        AtomicInteger processedFileCount = new AtomicInteger(0);
        AtomicInteger errorFileCount = new AtomicInteger(0);
        AtomicInteger totalRecords = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        try {
            bw = new BufferedWriter(new FileWriter("result/intersection.csv"));
            bw.write("Directory " + directoryPath + "\n" + "InputColumn1 " + columnName + "\n" + "InputColumn1Index "
                    + columnIndex + "\n" + "Comparisonvalue1 " + comparisonValue + "\n" + "InputColumn2 " + column
                    + "\n" + "InputColumn2Index " + index + "\n" + "Comparisonvalue2 " + value + "\n"
                    + "`OutputColumnIndex " + op + "\n");
            bw.write("Index,Result\n"); // write header
            bw.flush();

            for (File file : files) {
                if (file.getName().toLowerCase().endsWith(".csv") && !file.getName().toLowerCase().startsWith("._")) {
                    final BufferedWriter finalBw = bw;
                    executor.submit(() -> {
                        if (hasColumn(file.getAbsolutePath(), columnName, columnIndex)) {
                            try {
                                comparengram(file.getAbsolutePath(), column, index, value, finalBw, columnIndex, op,
                                        comparisonValue, totalRecords);
                                processedFileCount.incrementAndGet();
                            }
                            catch (IOException e) {
                                System.out.println("Error processing file: " + file.getName() + " - " + e.getMessage());
                                errorFileCount.incrementAndGet();
                            }
                        }
                        else {
                            System.out.println("Column " + columnName + " not found in file: " + file.getName());
                            errorFileCount.incrementAndGet();
                        }
                    });
                }
            }
        }
        catch (IOException e) {
            System.out.println("Error reading or writing CSV file: " + e.getMessage());
        }
        finally {
            executor.shutdown();
            try {
                // Wait for all tasks to finish
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            finally {
                if (bw != null) {
                    try {
                        bw.close();
                    }
                    catch (IOException e) {
                        System.out.println("Error closing BufferedWriter: " + e.getMessage());
                    }
                }
                long endTime = System.currentTimeMillis();
                long duration = endTime - startTime;
                double seconds = duration / 1000.0;
                double recordsPerSecond = totalRecords.get() / seconds;
                double filesPerSecond = processedFileCount.get() / seconds;

                System.out.println("Processed files: " + processedFileCount.get());
                System.out.println("Errored files: " + errorFileCount.get());
                System.out.println("Total records: " + totalRecords.get());
                System.out.println("Total time (seconds): " + seconds);
                System.out.println("Records processed per second: " + recordsPerSecond);
                System.out.println("Files processed per second: " + filesPerSecond);
            }
        }
    }

}
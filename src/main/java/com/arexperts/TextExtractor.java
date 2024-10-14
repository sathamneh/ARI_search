package com.arexperts;

import java.io.*;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

public class TextExtractor {

    public static void processTextFiles(String directoryPath, String outputFilePath, int threadCount, 
                                        String prefixSeparator, String suffixSeparator) {
        File[] files = new File(directoryPath).listFiles((dir, name) -> name.endsWith(".txt"));
        if (files == null) {
            System.err.println("No text files found in directory.");
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            for (File file : files) {
                executor.submit(() -> processFile(file, prefixSeparator, suffixSeparator, writer));
            }
        } catch (IOException e) {
            System.err.println("Error opening output file: " + e.getMessage());
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void processFile(File file, String prefixSeparator, String suffixSeparator, BufferedWriter writer) {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            StringBuilder contentBuilder = new StringBuilder();
            String line;
            boolean capture = false;

            while ((line = reader.readLine()) != null) {
                if (line.contains(prefixSeparator)) {
                    capture = true;  
                    continue;
                }

                if (line.contains(suffixSeparator)) {
                    capture = false;  
                    break;
                }

                if (capture) {
                    contentBuilder.append(line).append(" ");
                }
            }

            String extractedText = cleanUpText(contentBuilder.toString());

            synchronized (writer) {
                writer.write("File: " + file.getName() + "\nExtracted Content: " + extractedText + "\n\n");
                writer.flush();
            }
        } catch (IOException e) {
            System.err.println("Error processing file " + file.getName() + ": " + e.getMessage());
        }
    }

    private static String cleanUpText(String text) {
        return text.trim().replaceAll("\\s+", " ");
    }

        

}

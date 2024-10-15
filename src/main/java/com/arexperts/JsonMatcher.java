package com.arexperts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

public class JsonMatcher {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void processJsonFiles(String directoryPath, String outputFilePath, int threadCount, 
                                        String urlField, String textField, ArticleIndex articleIndex) {
        File[] files = new File(directoryPath).listFiles((dir, name) -> name.endsWith(".json.gz"));
        if (files == null) {
            System.err.println("No JSON files found in directory.");
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            for (File file : files) {
                executor.submit(() -> processFile(file, urlField, textField, articleIndex, writer));
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

    private static void processFile(File file, String urlField, String textField, 
                                    ArticleIndex articleIndex, BufferedWriter writer) {
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(Files.newInputStream(file.toPath()))) {
            JsonNode jsonNode = objectMapper.readTree(gzipInputStream);
            String url = jsonNode.get(urlField).asText();
            String text = jsonNode.get(textField).asText();

            String[] matchResult = articleIndex.findMatch(text);
            synchronized (writer) {
                writer.write("URL: " + url + ", Match: " + matchResult[0] + ", Score: " + matchResult[1] + "\n");
                writer.flush();
            }
        } catch (IOException e) {
            System.err.println("Error processing file " + file.getName() + ": " + e.getMessage());
        }
    }
}
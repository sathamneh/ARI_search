package com.arexperts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

import java.util.ArrayList;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;


public class ArticleLoader {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static ObjectNode[] loadArticlesForSearching(String fileName, int csvColumnIndex, String jsonTextField, String prefixSeparator, String suffixSeparator) {
        ArrayList<ObjectNode> returnedArticles = new ArrayList<ObjectNode>();

        if (fileName.toLowerCase().endsWith(".csv")) {
            //returnedArticles = loadArticlesFromCSV(fileName, csvColumnIndex, returnedArticles);
        }
        else if (fileName.toLowerCase().endsWith(".txt")) {
            //returnedArticles = loadArticlesFromText( fileName,  prefixSeparator,  suffixSeparator, returnedArticles);
        }
        else if (fileName.toLowerCase().endsWith(".json.gz")) {
            returnedArticles = loadArticlesFromGZippedJSON(fileName, jsonTextField, returnedArticles);
        }

        return returnedArticles.toArray(new ObjectNode[returnedArticles.size()]);
    }

    private static ArrayList<String> loadArticlesFromCSV(String fileName, int columnIndex, ArrayList<String> returnedArticles) {
        try (Reader reader = new FileReader(fileName);
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {

            for (CSVRecord record : csvParser) {
                if (record.size() <= columnIndex) {
                    continue;
                }        
                returnedArticles.add(record.get(columnIndex).trim());                            
            } 
        }
        catch(IOException ex) {
            System.err.println("File '" + fileName +  "' caught exception: " + ex.getLocalizedMessage());
        }

        return returnedArticles;
    } 

    private static ArrayList<ObjectNode> loadArticlesFromGZippedJSON(String fileName, String jsonTextField, ArrayList<ObjectNode> returnedArticles) {
        try (FileInputStream fis = new FileInputStream(fileName);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzis.read(buffer)) > 0) {
                baos.write(buffer, 0, len);
            }
            
            String content = baos.toString("UTF-8");
            String[] lines = content.split("\n");
            
            for (String line : lines) {
                JsonNode jsonNode = objectMapper.readTree(line); 
                String text = jsonNode.get(jsonTextField).asText();
                String score = jsonNode.get("score").asText();
                String url = jsonNode.get("url").asText();
                int length = text.length();
                ObjectNode myJson = objectMapper.createObjectNode();
                myJson.put("text", text);
                myJson.put("score", score);
                myJson.put("url", url);
                myJson.put("filename", fileName);
                myJson.put("text_size", length);
                returnedArticles.add(myJson);
            }
        } catch (IOException e) {
            System.err.println("Error processing file " + fileName + ": " + e.getMessage());
        }
        
        return returnedArticles;
    }

    private static ArrayList<String> loadArticlesFromText(String fileName, String prefixSeparator, String suffixSeparator, ArrayList<String> returnedArticles) {
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(fileName)))) {
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

            returnedArticles.add(cleanUpText(contentBuilder.toString()));

        } catch (IOException e) {
            System.err.println("Error processing file " + fileName + ": " + e.getMessage());
        }

        return returnedArticles;
    }

    private static String cleanUpText(String text) {
        return text.trim().replaceAll("\\s+", " ");
    }


}
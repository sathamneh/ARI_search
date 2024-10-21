package com.arexperts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    public static Article[] loadArticlesForSearching(String fileName, int csvColumnIndex, String jsonTextField, String jsonIDField, String prefixSeparator, String suffixSeparator) {
        ArrayList<Article> returnedArticles = new ArrayList<Article>();

        if (fileName.toLowerCase().endsWith(".csv")) {
            returnedArticles = loadArticlesFromCSV(fileName, csvColumnIndex, returnedArticles);
        }
        else if (fileName.toLowerCase().endsWith(".txt")) {
            returnedArticles = loadArticlesFromText( fileName,  prefixSeparator,  suffixSeparator, returnedArticles);
        }
        else if (fileName.toLowerCase().endsWith(".json.gz")) {
            returnedArticles = loadArticlesFromGZippedJSON(fileName, jsonTextField, jsonIDField, returnedArticles);
        }

        return returnedArticles.toArray(new Article[returnedArticles.size()]);
    }

    private static ArrayList<Article> loadArticlesFromCSV(String fileName, int columnIndex, ArrayList<Article> returnedArticles) {
        try (Reader reader = new FileReader(fileName);
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {

            for (CSVRecord record : csvParser) {
                if (record.size() <= columnIndex) {
                    continue;
                }
                
                returnedArticles.add(Article.build(record.get(columnIndex).trim(), fileName));                            
            } 
        }
        catch(IOException ex) {
            System.err.println("File '" + fileName +  "' caught exception: " + ex.getLocalizedMessage());
        }

        return returnedArticles;
    } 

    private static ArrayList<Article> loadArticlesFromGZippedJSON(String fileName, String jsonTextField, String jsonIDField, ArrayList<Article> returnedArticles) {

        try {
            GZIPInputStream gzipInputStream = new GZIPInputStream(Files.newInputStream(Paths.get(fileName)));
            InputStreamReader reader = new InputStreamReader(gzipInputStream);
            BufferedReader in = new BufferedReader(reader);
            String readString;
            while ((readString = in.readLine()) != null){
                JsonNode jsonNode = objectMapper.readTree(readString);
                if (jsonNode.has(jsonTextField) && jsonNode.has(jsonIDField))
                {
                    Article article = Article.build(jsonNode.get(jsonTextField).asText(), jsonNode.get(jsonIDField).asText());
                    returnedArticles.add(article);    
                }
            }
        } catch (IOException e) {
            System.err.println("Error processing file " + fileName + ": " + e.getMessage());
        }

        return returnedArticles;
    }

    private static ArrayList<Article> loadArticlesFromText(String fileName, String prefixSeparator, String suffixSeparator, ArrayList<Article> returnedArticles) {
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

            returnedArticles.add(Article.build(cleanUpText(contentBuilder.toString()), fileName));

        } catch (IOException e) {
            System.err.println("Error processing file " + fileName + ": " + e.getMessage());
        }

        return returnedArticles;
    }

    private static String cleanUpText(String text) {
        return text.trim().replaceAll("\\s+", " ");
    }


}

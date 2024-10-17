package com.arexperts;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class CSVReader {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static ArticleIndex loadNGramsFromCSVFiles(String directoryPath,  int columnIndex, int filesToProcess, int offsetFileNumber, int nGramLength, int maximumNumberOfNGrams) {
        int fileCount = 0;
        int filesProcessed = 0;
        File[] files = getFileList(directoryPath);
        ArticleIndex article = new ArticleIndex(nGramLength, maximumNumberOfNGrams);
        double startTime = System.nanoTime()/1_000_000_000.0;


        for (File file : files) {
            fileCount++;
            double loopStartTime = System.nanoTime()/1_000_000_000.0;
            if (file.getName().toLowerCase().startsWith("._")) {
                System.out.println("Skipping ._ file:" + file.getName());        
                continue;
            }
            if (fileCount <= offsetFileNumber) {
                System.out.println("Skipping offset file:" + file.getName() );        
                continue;
            }
            if (filesProcessed >= filesToProcess)
            {
                System.out.println("All processed files done! " + (System.nanoTime()/1_000_000_000.0 - startTime));        
                break;
            }
            if (file.getName().toLowerCase().endsWith(".csv")) {
                try (Reader reader = new FileReader(file.getAbsolutePath());
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {

                    for (CSVRecord record : csvParser) {
                        if (record.size() <= columnIndex) {
                            continue;
                        }        
                        String articleToAdd = record.get(columnIndex).trim();   
                        ObjectNode myJson = objectMapper.createObjectNode();
                        myJson.put("text", articleToAdd);
                        myJson.put("score", "score");
                        myJson.put("url", "url");
                        myJson.put("filename", file.getName().toLowerCase());
                        myJson.put("text_size", "length");
                        //returnedArticles.add(myJson);     
                        article.addArticle(myJson);
                    } 
                }
                catch(IOException ex) {
                    System.err.println("File '" + file.getName() +  "' caught exception: " + ex.getLocalizedMessage());
                }
                catch(OutOfMemoryError exMemoryError)
                {
                    System.err.println("File '" + file.getName() +  "' caught exception: " + exMemoryError.getLocalizedMessage() + " after " + fileCount + " files.");
                }
                filesProcessed++;
            } else {
                System.out.println("Skipping : " + file.getName() + " " + (System.nanoTime()/1_000_000_000.0 - startTime));        
            }
            
            System.out.println("File #" + filesProcessed + ". Processed " + file.getName() + " in " +   (System.nanoTime()/1_000_000_000.0 - loopStartTime));        
        }

        System.out.println("End:" + (System.nanoTime()/1_000_000_000.0 - startTime));        

        return article;
    }

    public static File[] getFileList(String directoryPath)
    {
        File directory = new File(directoryPath);
        if (!directory.exists()){
            System.err.println("Given path '" + directoryPath + "' does not exist.");
            return null;
        }
        if (!directory.isDirectory()) {
            System.err.println("Given path '" + directoryPath + "' is not a directory.");
            return null;
        }
        File[] files = directory.listFiles();
        if (files == null) {
            System.err.println("No files found in directory");
            return null;
        }

        return files;
    }
}
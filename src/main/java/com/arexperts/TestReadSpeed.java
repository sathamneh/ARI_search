package com.arexperts;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

public class TestReadSpeed {

    private static final double startTime = System.nanoTime();

    public static void main(String[] args) {
        int row = 0;        
        File file = new File("test.csv");
        ArticleIndex articles = new ArticleIndex(5);
        try (Reader reader = new FileReader(file.getAbsolutePath());
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {

            for (CSVRecord record : csvParser) {

                String articleToAdd = record.get(6).trim();        
                articles.addArticle(articleToAdd, "row" + row++);
            } 
        }
        catch(IOException ex) {
            System.err.println("File '" + file.getName() +  "' caught exception: " + ex.getLocalizedMessage());
        }
        catch(OutOfMemoryError exMemoryError)
        {
            System.err.println("File '" + file.getName() +  "' caught exception: " + exMemoryError.getLocalizedMessage());
        }
        System.out.println("Total elapsed time : " + getElapsedTime());               
    }
    private static double getElapsedTime()
    {        
        return (System.nanoTime() - startTime) / 1_000_000_000.0; // Converts to seconds
    }    
}

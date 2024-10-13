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

    private static void doRead() {
        int row = 0;        
        File file = new File("test.csv");
        ArticleIndex articles = new ArticleIndex(5);
        try (Reader reader = new FileReader(file.getAbsolutePath());
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {

            for (CSVRecord record : csvParser) {

                String articleToAdd = record.get(6).trim();        
                articles.addArticle(articleToAdd, "row" + row++);
            }
            System.out.println(articles.findMatch("It is undoubtedly an opulent and striking pictorial spectacle that Cecil B. DeMille has wrought from Wilson Barrett's famous old stage work, \"\"The Sign of the Cross.\"\" It was offered last night at the Rialto before a brilliant audience, which revealed no little interest in its variety of handsome scenes, its battling gladiators, its orgies in Nero's court, its chanting Christians, its music and its screams.Throughout this really mammoth production the fine DeMillean hand is noticeable. Where there was a chance to touch up episodes it has been done. It is as though Nero were living in the twentieth century, with some of the lines and the squabbling in the Rome arena for places to see the big bill, which includes many combats between Nero's own subjects and scores of Christians and others marching to their deaths. The hungry lions rush up stone steps, eager to get to their human prey, and, just before that, a dying man is supposed to have his head trampled on by an elephant.The principal rôles are all well played, even though they are more or less in the modern manner. But the outstanding histrionic achievement comes from Charles Laughton, who shoulders the responsibility for Nero. He is a petulent Nero, a man who has no thought for other than himself, and when he is asked to grant the life of")[1]); 
        }
        catch(IOException ex) {
            System.err.println("File '" + file.getName() +  "' caught exception: " + ex.getLocalizedMessage());
        }
        catch(OutOfMemoryError exMemoryError)
        {
            System.err.println("File '" + file.getName() +  "' caught exception: " + exMemoryError.getLocalizedMessage());
        }
    }
    public static void main(String[] args) {
        for (int i = 0; i < 100; i++) {
            doRead();
        }
        System.out.println("Total elapsed time : " + getElapsedTime());               
    }
    private static double getElapsedTime()
    {        
        return (System.nanoTime() - startTime) / 1_000_000_000.0; // Converts to seconds
    }    
}

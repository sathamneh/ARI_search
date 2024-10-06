import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CSVReader {
    public static void readCSV(String filePath) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                // for (String value : values) {
                //     // System.out.print(value + " | ");
                // }
                // System.out.println();
            }
        } catch (IOException e) {
            // System.err.println("Error reading CSV file: " + e.getMessage());
        }
    }

    public static boolean hasColumn(String filePath, String columnName, int columnIndex) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line = br.readLine(); // read the header row
            String[] headers = line.split(",");
            if (columnIndex >= headers.length) {
                return false;
            }
            return headers[columnIndex].trim().equals(columnName);
        } catch (IOException e) {
            // System.err.println("Error reading CSV file: " + e.getMessage());
            return false;
        }
    }

  

    public static void comparengram(String filePath, String columnName, int columnIndex, String comparisonValue, BufferedWriter bw,int columnurl,int outputcolumnindex,String urlvalue) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line = br.readLine(); // read the header row

            // System.out.println(filePath);
            bw.write(filePath+ "\n");
            bw.flush();
            String[] headers = line.split(",");
            if (columnIndex >= headers.length) {
                return;
            }
            
            if (!headers[columnIndex].trim().equals(columnName)) {
                return;
            }
            // System.out.println("here");
            ArticleIndex article = new ArticleIndex(10,true);
            while ((line = br.readLine()) != null) {

                String[] values = line.split(",");
                if (values.length <= columnIndex) {

                    continue;
                }
                  
                String column = values[columnIndex].trim();
                if (values.length>6){
                    for (int i = 7; i < values.length; i++) {
                        column=column+" "+values[i].trim();
                        
                    }



                }
              
                System.out.println(column);

                String result = article.validateMatch( comparisonValue,column);
                // System.out.println(result);
                if (urlvalue.contains("None")){
                // System.out.println(result.contains(urlvalue));
                if ( result.contains("true")) {
                // System.out.println(result);
                if(outputcolumnindex!=0) bw.write(values[outputcolumnindex].trim() + "," + result + "\n");
                else bw.write("0" + "," + result + "\n");
                bw.flush();    
                    }
                }else{
                    // System.out.println(result.contains("true"));
                    // System.out.println(result);
                    if (values[columnurl].trim().contains(urlvalue) && result.contains("true")) {
                    System.out.println(result);
                    if(outputcolumnindex!=0) bw.write(values[outputcolumnindex].trim() + "," + result + "\n");
                    else bw.write("0" + "," + result + "\n");    
                    bw.flush();
                    }

                }
            }
        }
    }

    public static void processFiles(String directoryPath, String columnName, int columnIndex, String comparisonValue,String column,int index,String value,int op) {
        File directory = new File(directoryPath);
        if (!directory.exists() || !directory.isDirectory()) {
            System.out.println("Invalid directory path");
            return;
        }
        File[] files = directory.listFiles();
        if (files == null) {
            System.out.println("No files found in directory");
            return;
        }
        try (BufferedWriter bw = new BufferedWriter(new FileWriter("result/intersection.csv"))) {
            bw.write("Directory "+directoryPath+ "\n" + "InputColumn1 "+columnName  +"\n"+ "InputColumn1Index "+columnIndex + "\n"+"Comparisonvalue1 "+ comparisonValue +"\n"+ "InputColumn2 "+column+"\n"+"InputColumn2Index " +index +"\n"+"Comparisonvalue2 "+ value+"\n"+ "`OutputColumnIndex "+op+"\n");
            bw.write("Index,Result\n"); // write header
            bw.flush();
            for (File file : files) {
                if (file.getName().endsWith(".CSV")) {
                    // System.out.println("Processing file: " + file.getName());
                    readCSV(file.getAbsolutePath());
                    if (hasColumn(file.getAbsolutePath(), columnName, columnIndex)) {
                        // System.out.println("Column found!");
                        comparengram(file.getAbsolutePath(), column, index, value, bw,columnIndex,op,comparisonValue);
                    } else {
                        // System.out.println("Column not found!");
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading or writing CSV file: " + e.getMessage());
        }
    }

  
}
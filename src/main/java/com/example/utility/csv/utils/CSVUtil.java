package com.example.utility.csv.utils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

public class CSVUtil {

    private static final String CSV_DELIMITER = ",";

    public static ArrayList<String> getHeaders(Path csvFilePath) throws IOException {
        ArrayList<String> headers = new ArrayList<>();
        BufferedReader bufferedReader= new BufferedReader(new FileReader(csvFilePath.toString()));
        String firstLine = bufferedReader.readLine();
        if (firstLine != null) {
            headers.addAll(Arrays.stream(firstLine.split(CSV_DELIMITER)).toList());
        }
        return headers;
    }

    public static ArrayList<ArrayList<String>> getRowsWithoutHeaders(Path csvFilePath, int numOfRows) throws IOException {
        ArrayList<ArrayList<String>> rows = new ArrayList<>();
        BufferedReader bufferedReader= new BufferedReader(new FileReader(csvFilePath.toString()));
        String line = bufferedReader.readLine();
        for (int i = 0; line != null && (line = bufferedReader.readLine()) != null && i < numOfRows; i++) {
           rows.add(new ArrayList<>(Arrays.stream(line.split(CSV_DELIMITER)).toList()));
        }
        return rows;
    }

    public static boolean doesContainEmptyHeader(Path csvFilePath) throws IOException {
        BufferedReader bufferedReader= new BufferedReader(new FileReader(csvFilePath.toString()));
        String line = bufferedReader.readLine();

        if (line == null) {
            return true;
        }

        for (String header: line.split(CSV_DELIMITER)) {
            if (header.trim().equals("") || header.trim().equals("\"\"") || header.trim().equals("''")) {
                return true;
            }
        }

        return false;
    }

    public static void checkAndProcessRows(Path csvFilePath) throws IOException {
        Path tempFile = Files.createTempFile(null, null);

        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(tempFile.toString()));
        BufferedReader bufferedReader= new BufferedReader(new FileReader(csvFilePath.toString()));

        String line;
        while ((line = bufferedReader.readLine()) != null) {
            bufferedWriter.write(line.replaceAll("\"\"", "").replaceAll("''", ""));
        }

        Files.move(tempFile, csvFilePath);
    }
}

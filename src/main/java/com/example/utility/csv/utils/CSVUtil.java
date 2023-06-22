package com.example.utility.csv.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;

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
}

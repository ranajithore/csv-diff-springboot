package com.example.utility.csv.utils;

import org.dhatim.fastexcel.Color;
import org.dhatim.fastexcel.Workbook;
import org.dhatim.fastexcel.Worksheet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

public class SQLiteUtil {
    private static final String applicationName = "Utility Application";
    private static final String applicationVersion = "1.0";
    private static final String OUTPUT_DIR_NAME = "output";
    private static final String ROWS_DELETED_FILE_NAME = "rows-deleted";
    private static final String ROWS_ADDED_FILE_NAME = "rows-added";
    private static final String ROWS_UPDATED_FILE_NAME = "rows-updated";
    private static final String COLOR_RGB_RED = Color.ORANGE_RED;
    private static final String COLOR_RGB_GREEN = Color.APPLE_GREEN;

    private static final int EXCEL_ROW_LIMIT = 1000;

    private static final String dbName = "database.sqlite";

    private static final Logger logger = LoggerFactory.getLogger(SQLiteUtil.class);

//    PUBLIC METHODS

    public static void createSchemaFromOldCSV(Path csvFilePath) throws IOException {
        createSchemaFromCSV(csvFilePath, "OLD");
    }

    public static void createSchemaFromNewCSV(Path csvFilePath) throws IOException {
        createSchemaFromCSV(csvFilePath, "NEW");
    }

    public static void createOldTableFromSchemaFile(Path csvFilePath) throws IOException {
        createTableFromSchemaFile(csvFilePath);
    }

    public static void createNewTableFromSchemaFile(Path csvFilePath) throws IOException {
        createTableFromSchemaFile(csvFilePath);
    }

    public static void importDataFromOldCSV(Path csvFilePath) throws IOException {
        importDataFromCSV(csvFilePath, "OLD");
    }

    public static void importDataFromNewCSV(Path csvFilePath) throws IOException {
        importDataFromCSV(csvFilePath, "NEW");
    }

    public static void getRowsDeleted(Path csvFilePath, String oldId, String newId) throws SQLException {
        Path dbPath = csvFilePath.getParent().resolve(dbName);
        String query = "CREATE TABLE DELETED AS SELECT * FROM OLD WHERE \"%s\" NOT IN (SELECT \"%s\" FROM NEW)".formatted(oldId, newId);
        executeSQLQuery(dbPath, query);
    }

    public static void getRowsAdded(Path csvFilePath, String oldId, String newId) throws SQLException {
        Path dbPath = csvFilePath.getParent().resolve(dbName);
        String query = "CREATE TABLE ADDED AS SELECT * FROM NEW WHERE \"%s\" NOT IN (SELECT \"%s\" FROM OLD)".formatted(newId, oldId);
        executeSQLQuery(dbPath, query);
    }

    public static void getRowsUpdated(Path csvFilePath, String oldId, String newId) throws SQLException {
        Path dbPath = csvFilePath.getParent().resolve(dbName);
        String query = "CREATE TABLE UPDATED AS SELECT * FROM OLD INNER JOIN NEW ON OLD.\"" + oldId + "\" = NEW.\"" + newId + "\" WHERE OLD.\"" + oldId + "\" NOT IN (SELECT \"" + oldId + "\" FROM OLD NATURAL JOIN NEW)";
        executeSQLQuery(dbPath, query);
    }

    public static void exportToExcel(Path oldCSVFilePath, Path newCSVFilePath) throws SQLException, IOException {
        Path dbPath = oldCSVFilePath.getParent().resolve(dbName);
        exportRowsDeleted(oldCSVFilePath, dbPath);
        exportRowsAdded(newCSVFilePath, dbPath);
        exportRowsUpdated(oldCSVFilePath, newCSVFilePath, dbPath);
    }

    //    PRIVATE METHODS

    private static void runUnixScript(String arguments) throws IOException {
        Process process = UnixScriptRunner.run(arguments);
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader stdErr = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        for (String s; (s = stdIn.readLine()) != null; ) {
            logger.info(s);
        }
        for (String s; (s = stdErr.readLine()) != null; ) {
            logger.error(s);
        }
    }

    private static void createSchemaFromCSV(Path csvFilePath, String tableName) throws IOException {
        Path schemaFilePath = csvFilePath.getParent().resolve(csvFilePath.getFileName() + ".sql");
        int numTopRows = 50;
        String arguments = String.format("head -n %d %s | csvsql -i sqlite --tables %s > %s", numTopRows, csvFilePath, tableName, schemaFilePath);
        runUnixScript(arguments);
    }

    private static void createTableFromSchemaFile(Path csvFilePath) throws IOException {
        Path dbPath = csvFilePath.getParent().resolve(dbName);
        Path schemaFilePath = csvFilePath.getParent().resolve(csvFilePath.getFileName() + ".sql");
        String arguments = "sqlite3 %s \".read %s\"".formatted(dbPath, schemaFilePath);
        runUnixScript(arguments);
    }

    private static void importDataFromCSV(Path csvFilePath, String tableName) throws IOException {
        Path dbPath = csvFilePath.getParent().resolve(dbName);
        String arguments = "sqlite3 %s \".import --csv --skip 1 %s %s\"".formatted(dbPath, csvFilePath, tableName);
        runUnixScript(arguments);
    }

    private static void executeSQLQuery(Path dbPath, String query) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        Statement statement = connection.createStatement();
        statement.execute(query);
        statement.close();
        connection.close();
    }

    private static void exportRowsDeleted(Path oldCSVFilePath, Path dbPath) throws SQLException, IOException {
//        Get results from SQLITE TABLE
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        Statement statement = connection.createStatement();
        ArrayList<String> headers = CSVUtil.getHeaders(oldCSVFilePath);
        String query = "SELECT * FROM DELETED";
        ResultSet resultSet = statement.executeQuery(query);

//        Create Workbook
        int fileSuffix = 0;
        OutputStream oStream = createExcelFileOutputStream(oldCSVFilePath, ROWS_DELETED_FILE_NAME, ++fileSuffix);
        Workbook wb = new Workbook(oStream, applicationName, applicationVersion);
        Worksheet ws = wb.newWorksheet("Sheet 1");

//        Write Headers
        writeHeadersToExcel(headers, ws);

//        Write Rows Deleted
        int row = 1;
        while (resultSet.next()) {
            for (int col = 0; col < headers.size(); col++) {
                ws.style(row, col).fillColor(COLOR_RGB_RED).set();
                ws.value(row, col, resultSet.getString(col + 1));
            }
            row++;

            if (row == EXCEL_ROW_LIMIT) {
                wb.finish();
                oStream = createExcelFileOutputStream(oldCSVFilePath, ROWS_DELETED_FILE_NAME, ++fileSuffix);
                wb = new Workbook(oStream, applicationName, applicationVersion);
                ws = wb.newWorksheet("Sheet 1");
                writeHeadersToExcel(headers, ws);
                row = 1;
            }
        }

        wb.finish();
        statement.close();
        connection.close();
    }

    private static void exportRowsAdded(Path newCSVFilePath, Path dbPath) throws SQLException, IOException {
//        Get results from SQLITE TABLE
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        Statement statement = connection.createStatement();
        ArrayList<String> headers = CSVUtil.getHeaders(newCSVFilePath);
        String query = "SELECT * FROM ADDED";
        ResultSet resultSet = statement.executeQuery(query);

//        Create Workbook
        int fileSuffix = 0;
        OutputStream oStream = createExcelFileOutputStream(newCSVFilePath, ROWS_ADDED_FILE_NAME, ++fileSuffix);
        Workbook wb = new Workbook(oStream, applicationName, applicationVersion);
        Worksheet ws = wb.newWorksheet("Sheet 1");

//        Write Headers
        writeHeadersToExcel(headers, ws);

//        Write Rows Deleted
        int row = 1;
        while (resultSet.next()) {
            for (int col = 0; col < headers.size(); col++) {
                ws.style(row, col).fillColor(COLOR_RGB_GREEN).set();
                ws.value(row, col, resultSet.getString(col + 1));
            }
            row++;

            if (row == EXCEL_ROW_LIMIT) {
                wb.finish();
                oStream = createExcelFileOutputStream(newCSVFilePath, ROWS_ADDED_FILE_NAME, ++fileSuffix);
                wb = new Workbook(oStream, applicationName, applicationVersion);
                ws = wb.newWorksheet("Sheet 1");
                writeHeadersToExcel(headers, ws);
                row = 1;
            }
        }

        wb.finish();
        statement.close();
        connection.close();
    }

    private static void exportRowsUpdated(Path oldCSVFilePath, Path newCSVFilePath, Path dbPath) throws SQLException, IOException {
//        Get results from SQLITE TABLE
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        Statement statement = connection.createStatement();
        String query = "SELECT * FROM UPDATED";
        ResultSet resultSet = statement.executeQuery(query);

        ArrayList<String> oldHeaders = CSVUtil.getHeaders(oldCSVFilePath);
        ArrayList<String> newHeaders = CSVUtil.getHeaders(newCSVFilePath);
        ArrayList<String> headers = oldHeaders.size() > newHeaders.size() ? oldHeaders : newHeaders;

        HashSet<String> oldHeadersSet = new HashSet<>(oldHeaders);
        HashSet<String> newHeadersSet = new HashSet<>(newHeaders);



//        Create Workbook
        int fileSuffix = 0;
        OutputStream oStream = createExcelFileOutputStream(oldCSVFilePath, ROWS_UPDATED_FILE_NAME, ++fileSuffix);
        Workbook wb = new Workbook(oStream, applicationName, applicationVersion);
        Worksheet ws = wb.newWorksheet("Sheet 1");

//        Write Headers
        writeHeadersToExcel(headers, ws);

//        Write Rows Deleted
        int row = 1;
        while (resultSet.next()) {
            for (int col1 = 0, col2 = oldHeaders.size(); col1 < headers.size(); col1++, col2++) {
                String colName = headers.get(col1);
                String row1Value = oldHeadersSet.contains(colName) ? resultSet.getString(colName) : "";
                String row2Value = newHeadersSet.contains(colName) ? resultSet.getString(oldHeadersSet.contains(colName) ? processDuplicateHeader(colName) : colName) : "";
                if (!row1Value.equals(row2Value)) {
                    if (!row1Value.equals("")) ws.style(row, col1).fillColor(COLOR_RGB_RED).set();
                    if (!row2Value.equals("")) ws.style(row + 1, col1).fillColor(COLOR_RGB_GREEN).set();
                }
                ws.value(row, col1, row1Value);
                ws.value(row + 1, col1, row2Value);
            }
            row = row + 3;  // ADD extra blank row

            if (row == EXCEL_ROW_LIMIT) {
                wb.finish();
                oStream = createExcelFileOutputStream(oldCSVFilePath, ROWS_UPDATED_FILE_NAME, ++fileSuffix);
                wb = new Workbook(oStream, applicationName, applicationVersion);
                ws = wb.newWorksheet("Sheet 1");
                writeHeadersToExcel(headers, ws);
                row = 1;
            }
        }

        wb.finish();
        statement.close();
        connection.close();
    }

    private static OutputStream createExcelFileOutputStream(Path csvFilePath, String fileName, int fileSuffix) throws IOException {
        Path parentDir = Files.createDirectories(Paths.get(csvFilePath.getParent().toString(), OUTPUT_DIR_NAME, fileName));
        Path outputFilePath = Files.createFile(Paths.get(parentDir.toString(), fileName + "-" + fileSuffix +".xlsx"));
        return new FileOutputStream(outputFilePath.toString());
    }

    private static void writeHeadersToExcel(ArrayList<String> headers, Worksheet ws) {
        final int row = 0;
        for (int col = 0; col < headers.size(); col++) {
            ws.style(row, col).bold().set();
            ws.value(row, col, headers.get(col));
        }
    }

    private static String processDuplicateHeader(String header) {
        return header + ":1";
    }
}

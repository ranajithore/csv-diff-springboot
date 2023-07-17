package com.example.utility.csv.utils;

import com.example.utility.csv.records.*;
import net.sf.jasperreports.engine.*;
import net.sf.jasperreports.engine.data.JsonDataSource;
import org.dhatim.fastexcel.Workbook;
import org.dhatim.fastexcel.Worksheet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.*;

public class SQLiteUtil {
    private static final String applicationName = "Utility Application";
    private static final String applicationVersion = "1.0";
    private static final String OUTPUT_DIR_NAME = "output";
    private static final String ROWS_DELETED_FILE_NAME = "rows-deleted";
    private static final String ROWS_ADDED_FILE_NAME = "rows-added";
    private static final String ROWS_UPDATED_FILE_NAME = "rows-updated";
    private static final String COLOR_RGB_LIGHT_RED = "F1AEB5";
    private static final String COLOR_RGB_LIGHT_GREEN = "A6E9D5";
    private static final String COLOR_RGB_LIGHT_YELLOW = "FFE69C";
    private static final int EXCEL_ROW_LIMIT = 50000;
    private static final String FILE_EXTENSION = "xlsx";
    private static final String DEFAULT_SHEET_NAME = "Sheet 1";
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

    public static void exportToExcel(Path oldCSVFilePath, Path newCSVFilePath, String oldPrimaryKey, String newPrimaryKey) throws SQLException, IOException {
        Path dbPath = oldCSVFilePath.getParent().resolve(dbName);
        exportRowsDeleted(oldCSVFilePath, dbPath);
        exportRowsAdded(newCSVFilePath, dbPath);
//        exportRowsUpdated(oldCSVFilePath, newCSVFilePath, dbPath);
        exportRowsUpdatedDataInColumnFormat(oldCSVFilePath, newCSVFilePath, dbPath, newPrimaryKey);
    }

    public static Report generateReport(double operationTime, Path oldCSVFilePath, Path newCSVFilePath, String oldPrimaryKey, String newPrimaryKey) throws SQLException, IOException {
        Path dbPath = oldCSVFilePath.getParent().resolve(dbName);

        final ArrayList<String> oldHeaders = CSVUtil.getHeaders(oldCSVFilePath);
        final ArrayList<String> newHeaders = CSVUtil.getHeaders(newCSVFilePath);

        TableStructure oldTable = getTableStructure(dbPath, "OLD", oldPrimaryKey);
        TableStructure newTable = getTableStructure(dbPath, "NEW", newPrimaryKey);

        return new Report(
                operationTime,
                oldTable,
                newTable,
                getRowWiseComparisonData(dbPath),
                getColWiseComparisonData(oldHeaders, newHeaders),
                getColumnComparisonData(dbPath, oldTable, newTable, oldHeaders, newHeaders)
        );
    }

    public static void exportReportToPdf(Path jsonReport, Path sourceReportFile) throws JRException, FileNotFoundException {
        JRDataSource jsonDataSource = new JsonDataSource(jsonReport.toFile());
        JasperReport jasperReport = JasperCompileManager.compileReport(sourceReportFile.toString());
        Map<String, Object> params = new HashMap<>();
        JasperPrint jasperPrint = JasperFillManager.fillReport(jasperReport, params, jsonDataSource);

        JasperExportManager.exportReportToPdfFile(jasperPrint,jsonReport.getParent().resolve(OUTPUT_DIR_NAME).resolve("report.pdf").toString());
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
                ws.style(row, col).fillColor(COLOR_RGB_LIGHT_RED).set();
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
                ws.style(row, col).fillColor(COLOR_RGB_LIGHT_GREEN).set();
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
                    if (!row1Value.equals("")) ws.style(row, col1).fillColor(COLOR_RGB_LIGHT_RED).set();
                    if (!row2Value.equals("")) ws.style(row + 1, col1).fillColor(COLOR_RGB_LIGHT_GREEN).set();
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

    private static void exportRowsUpdatedDataInColumnFormat(Path oldCSVFilePath, Path newCSVFilePath, Path dbPath, String primaryKey) throws SQLException, IOException {
//        Get CSV headers
        final ArrayList<String> oldHeaders = CSVUtil.getHeaders(oldCSVFilePath);
        final ArrayList<String> newHeaders = CSVUtil.getHeaders(newCSVFilePath);
        String processedPrimaryKey = processDuplicateHeader(primaryKey);

        final ArrayList<String> deletedHeaders = getDeletedHeaders(oldHeaders, newHeaders);
        final ArrayList<String> addedHeaders = getAddedHeaders(oldHeaders, newHeaders);
        final ArrayList<String> commonHeaders = getCommonHeaders(oldHeaders, newHeaders);

//        Sort headers alphabetically
        Collections.sort(deletedHeaders);
        Collections.sort(addedHeaders);
        Collections.sort(commonHeaders);

//        Create Workbook
        int fileSuffix = 0;
        Workbook wb = openWorkBook(oldCSVFilePath, ROWS_UPDATED_FILE_NAME, ++fileSuffix);
        Worksheet ws = wb.newWorksheet(DEFAULT_SHEET_NAME);

//        Write Headers
        final String[] excelHeaders = {"COLUMN NAME", primaryKey.toUpperCase(), "OLD VALUE", "NEW VALUE"};
        writeHeadersToExcel(new ArrayList<>(Arrays.asList(excelHeaders)), ws);

        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        Statement statement = connection.createStatement();

        int row = 1;

        for (String header : deletedHeaders) {
//          Get results from SQLITE TABLE
            String query = "SELECT \"%s\", \"%s\" FROM UPDATED".formatted(processedPrimaryKey, header);
            ResultSet resultSet =  statement.executeQuery(query);

            for (; resultSet.next() ; row++) {
                String primaryKeyValue = resultSet.getString(1);
                String oldValue = resultSet.getString(2);

                setCellColors(ws, row, 4, COLOR_RGB_LIGHT_RED);

                ws.value(row, 0, header);
                ws.value(row, 1, primaryKeyValue);
                ws.value(row, 2, oldValue);

                if (row == EXCEL_ROW_LIMIT) {
                    wb.finish();
                    wb = openWorkBook(oldCSVFilePath, ROWS_UPDATED_FILE_NAME, ++fileSuffix);
                    ws = wb.newWorksheet(DEFAULT_SHEET_NAME);
                    row = 1;
                }
            }
        }

        for (String header : addedHeaders) {
//          Get results from SQLITE TABLE
            String query = "SELECT \"%s\", \"%s\" FROM UPDATED".formatted(processedPrimaryKey, processDuplicateHeader(header));
            ResultSet resultSet =  statement.executeQuery(query);

            for (; resultSet.next() ; row++) {
                String primaryKeyValue = resultSet.getString(1);
                String newValue = resultSet.getString(2);

                setCellColors(ws, row, 4, COLOR_RGB_LIGHT_GREEN);

                ws.value(row, 0, header);
                ws.value(row, 1, primaryKeyValue);
                ws.value(row, 3, newValue);

                if (row == EXCEL_ROW_LIMIT) {
                    wb.finish();
                    wb = openWorkBook(oldCSVFilePath, ROWS_UPDATED_FILE_NAME, ++fileSuffix);
                    ws = wb.newWorksheet(DEFAULT_SHEET_NAME);
                    row = 1;
                }
            }
        }

        for (String header : commonHeaders) {
//          Get results from SQLITE TABLE
            String query = "SELECT \"%s\", \"%s\", \"%s\" FROM UPDATED WHERE \"%s\" != \"%s\"".formatted(
                    processedPrimaryKey, header, processDuplicateHeader(header), header, processDuplicateHeader(header));
            ResultSet resultSet =  statement.executeQuery(query);

            for (; resultSet.next() ; row++) {
                String primaryKeyValue = resultSet.getString(1);
                String oldValue = resultSet.getString(2);
                String newValue = resultSet.getString(3);

                setCellColors(ws, row, 4, COLOR_RGB_LIGHT_YELLOW);

                ws.value(row, 0, header);
                ws.value(row, 1, primaryKeyValue);
                ws.value(row, 2, oldValue);
                ws.value(row, 3, newValue);

                if (row == EXCEL_ROW_LIMIT) {
                    wb.finish();
                    wb = openWorkBook(oldCSVFilePath, ROWS_UPDATED_FILE_NAME, ++fileSuffix);
                    ws = wb.newWorksheet(DEFAULT_SHEET_NAME);
                    row = 1;
                }
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

    private static ArrayList<String> processNewHeaders(ArrayList<String> newHeaders) {
        ArrayList<String> processedHeaders = new ArrayList<>();
        newHeaders.forEach(header -> {
            processedHeaders.add(header + ":1");
        });
        return processedHeaders;
    }

    private static ArrayList<String> getCommonHeaders(List<String> oldHeaders, List<String> newHeaders) {
        HashSet<String> oldHeadersSet = new HashSet<>(oldHeaders);
        HashSet<String> newHeadersSet = new HashSet<>(newHeaders);
        oldHeadersSet.retainAll(newHeadersSet);
        return new ArrayList<>(oldHeadersSet);
    }

    private static ArrayList<String> getDeletedHeaders(List<String> oldHeaders, List<String> newHeaders) {
        HashSet<String> oldHeadersSet = new HashSet<>(oldHeaders);
        HashSet<String> newHeadersSet = new HashSet<>(newHeaders);
        oldHeadersSet.removeAll(newHeadersSet);
        return new ArrayList<>(oldHeadersSet);
    }

    private static ArrayList<String> getAddedHeaders(List<String> oldHeaders, List<String> newHeaders) {
        HashSet<String> oldHeadersSet = new HashSet<>(oldHeaders);
        HashSet<String> newHeadersSet = new HashSet<>(newHeaders);
        newHeadersSet.removeAll(oldHeadersSet);
        return new ArrayList<>(newHeadersSet);
    }

    private static Workbook openWorkBook(Path csvFilePath, String fileName, int fileSuffix) throws IOException {
        Path parentDir = Files.createDirectories(Paths.get(csvFilePath.getParent().toString(), OUTPUT_DIR_NAME, fileName));
        Path outputFilePath = Files.createFile(Paths.get(parentDir.toString(), fileName + "-" + fileSuffix + "." + FILE_EXTENSION));
        OutputStream oStream = new FileOutputStream(outputFilePath.toString());
        return new Workbook(oStream, applicationName, applicationVersion);
    }

    private static void setCellColors(Worksheet ws, int row, int numOfCols, String color) {
        for (int col = 0; col < numOfCols; col++) {
            ws.style(row, col).fillColor(color).set();
        }
    }

    private static TableStructure getTableStructure(Path dbPath, String tableName, String primaryKey) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        Statement statement = connection.createStatement();

        String query = "PRAGMA table_info(\"%s\")".formatted(tableName);
        ResultSet resultSet = statement.executeQuery(query);
        List<Column> columns = new ArrayList<>();
        for (; resultSet.next(); ) {
            String colName = resultSet.getString(2);
            String dataType = resultSet.getString(3);
            columns.add(new Column(colName, dataType));
        }

        query = "SELECT COUNT(*) FROM \"%s\"".formatted(tableName);
        resultSet = statement.executeQuery(query);
        resultSet.next();

        long numOfRows = resultSet.getLong(1);
        long numOfCols = columns.size();

        statement.close();
        connection.close();

        return new TableStructure(numOfRows, numOfCols, columns, primaryKey);
    }

    private static List<ComparisonData> getRowWiseComparisonData(Path dbPath) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        Statement statement = connection.createStatement();

        List<ComparisonData> comparisonData = new ArrayList<>();

        String query = "SELECT COUNT(*) FROM DELETED";
        ResultSet resultSet = statement.executeQuery(query);
        resultSet.next();
        long numOfRowsDeleted = resultSet.getLong(1);
        comparisonData.add(new ComparisonData("Deleted", "Total number of rows deleted from the source file", numOfRowsDeleted));

        query = "SELECT COUNT(*) FROM ADDED";
        resultSet = statement.executeQuery(query);
        resultSet.next();
        long numOfRowsAdded = resultSet.getLong(1);
        comparisonData.add(new ComparisonData("Added", "Total number of new rows added in the target file", numOfRowsAdded));


        query = "SELECT COUNT(*) FROM UPDATED";
        resultSet = statement.executeQuery(query);
        resultSet.next();
        long numOfRowsUpdated = resultSet.getLong(1);
        comparisonData.add(new ComparisonData("Updated", "Total number of rows updated", numOfRowsUpdated));

        return comparisonData;
    }

    private static List<ComparisonData> getColWiseComparisonData(List<String> oldHeaders, List<String> newHeaders) {
        long numOfColsDeleted = getDeletedHeaders(oldHeaders, newHeaders).size();
        long numOfColsAdded = getAddedHeaders(oldHeaders, newHeaders).size();
        long numOfColsCommonInBoth = getCommonHeaders(oldHeaders, newHeaders).size();

        List<ComparisonData> comparisonData = new ArrayList<>();

        comparisonData.add(new ComparisonData("Deleted", "Total number of columns deleted from the source file", numOfColsDeleted));
        comparisonData.add(new ComparisonData("Added", "Total number of new columns added in the target file", numOfColsAdded));
        comparisonData.add(new ComparisonData("Common", "Total number of columns common in both file", numOfColsCommonInBoth));

        return comparisonData;
    }

    private static ColumnComparisonData getColComparisonDataForHeader(Path dbPath, String header, String dataType, long totalNumOfRows) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        Statement statement = connection.createStatement();

        DecimalFormat decimalFormat = new DecimalFormat("#.##");
        decimalFormat.setRoundingMode(RoundingMode.CEILING);

        String query = "SELECT COUNT(*) FROM UPDATED WHERE \"%s\" != \"%s\"".formatted(header, processDuplicateHeader(header));
        ResultSet resultSet = statement.executeQuery(query);
        resultSet.next();
        long numOfRowsUpdated = resultSet.getLong(1);
        float percentOfRowsUpdated = Float.parseFloat(decimalFormat.format(((float) numOfRowsUpdated / totalNumOfRows) * 100));

        statement.close();
        connection.close();

        return new ColumnComparisonData(header, dataType, numOfRowsUpdated, percentOfRowsUpdated);
    }

    private static List<ColumnComparisonData> getColumnComparisonData(Path dbPath, TableStructure oldTable, TableStructure newTable,  List<String> oldHeaders, List<String> newHeaders) throws SQLException {
        long totalNumOfRows = oldTable.numOfRows();

        HashMap<String, String> columnDataTypes = new HashMap<>();
        for (Column col: oldTable.columns()) {
            if (!columnDataTypes.containsKey(col.name())) {
                columnDataTypes.put(col.name(), col.dataType());
            }
        }
        for (Column col: newTable.columns()) {
            if (!columnDataTypes.containsKey(col.name())) {
                columnDataTypes.put(col.name(), col.dataType());
            }
        }

        ArrayList<String> deletedHeaders = getDeletedHeaders(oldHeaders, newHeaders);
        ArrayList<String> addedHeaders = getAddedHeaders(oldHeaders, newHeaders);
        ArrayList<String> commonHeaders = getCommonHeaders(oldHeaders, newHeaders);

//        Sort headers alphabetically
        Collections.sort(deletedHeaders);
        Collections.sort(addedHeaders);
        Collections.sort(commonHeaders);

        List<ColumnComparisonData> columnComparisonData = new ArrayList<>();

        for (String header: deletedHeaders) {
            columnComparisonData.add(getColComparisonDataForHeader(
                    dbPath, header, columnDataTypes.get(header), totalNumOfRows
            ));
        }

        for (String header: addedHeaders) {
            columnComparisonData.add(getColComparisonDataForHeader(
                    dbPath, header, columnDataTypes.get(header), totalNumOfRows
            ));
        }

        for (String header: commonHeaders) {
            columnComparisonData.add(getColComparisonDataForHeader(
                    dbPath, header, columnDataTypes.get(header), totalNumOfRows
            ));
        }

        return columnComparisonData;
    }
}

package com.example.utility.csv.records;

import java.util.List;

public record Report(
    double operationTime,
    TableStructure oldFileStructure,
    TableStructure newFileStructure,
    long numOfDeletedHeaders,
    long numOfAddedHeaders,
    long numOfCommonHeaders,
    List<Column> deletedColumns,
    List<Column> addedColumns,
    List<Column> commonColumns,
    List<ComparisonData> rowWiseComparisonData,
    List<ComparisonData> colWiseComparisonData,
    List<ColumnComparisonData> colComparisonData,
    List<RowDifferenceColumnFormat> rowDifferenceSampleData
) {
}

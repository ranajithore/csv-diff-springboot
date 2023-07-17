package com.example.utility.csv.records;

import java.util.List;

public record Report(
    double operationTime,
    TableStructure oldFileStructure,
    TableStructure newFileStructure,
    List<ComparisonData> rowWiseComparisonData,
    List<ComparisonData> colWiseComparisonData,
    List<ColumnComparisonData> colComparisonData
) {
}

package com.example.utility.csv.records;

import java.util.List;

public record TableStructure(
        long numOfRows,
        long numOfCols,
        List<Column> columns,
        String primaryKey
) {
}

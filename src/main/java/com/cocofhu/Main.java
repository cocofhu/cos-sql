package com.cocofhu;

import com.cocofhu.data.FieldType;
import com.cocofhu.data.csv.CSVTable;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Pair;

import java.sql.*;
import java.util.*;


public class Main {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("caseSensitive", "false");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        List<Pair<String, FieldType>> fields = new ArrayList<>();
        fields.add(new Pair<>("a", FieldType.INT));
        fields.add(new Pair<>("b", FieldType.STRING));
        fields.add(new Pair<>("c", FieldType.STRING));
        rootSchema.add("test", new CSVTable(fields, 10, "#"));
        System.out.println("Data Tool Started.");
        StringBuilder sql = new StringBuilder();
        try (Statement statement = calciteConnection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select * from test where a = 1 and a*5 = 5");
            List<List<Object>> lists = resultList(resultSet, true);
            showTable(lists);
        }catch (SQLException e){
            System.out.printf("[ERROR #%d] %s. %n",e.getErrorCode(), e.getMessage());
        }

    }

    public static List<List<Object>> resultList(ResultSet resultSet, boolean printHeader) throws SQLException {
        ArrayList<List<Object>> results = new ArrayList<>();
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        if (printHeader) {
            ArrayList<Object> header = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                header.add(metaData.getColumnName(i) + ":" +metaData.getColumnType(i) );
            }
            results.add(header);
        }
        while (resultSet.next()) {
            ArrayList<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
//                System.out.print(resultSet.getObject(i) +"#");
                row.add(resultSet.getObject(i));
            }
            System.out.println();
            results.add(row);
        }
        return results;
    }

    static void showTable(List<List<Object>> objects){
        AsciiTable table = new AsciiTable();

        objects.forEach(row->{
            table.addRule();
            for (int i = 0; i < row.size(); i++) {
                if(row.get(i) == null) row.set(i,"NULL");
            }

            table.addRow(row).setPadding(0).setPaddingRight(1);
        });
        table.addRule();
        table.getRenderer().setCWC(new CWC_LongestLine());
        System.out.println(table.render());
    }
}
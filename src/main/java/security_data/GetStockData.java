package security_data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;

public class GetStockData implements RequestHandler<HashMap<String, Object>, GetStockDataResponse> {

    Connection conn = null;

    public GetStockDataResponse handleRequest(HashMap<String, Object> input, Context context) {
        try {
            Map<String, String> event = (Map<String, String>) input.get("queryStringParameters");
            if (!event.containsKey("ticker"))
                return new GetStockDataResponse(200, new HashMap<String, String>(), "missing ticker parameter", false);
            if (!event.containsKey("field"))
                return new GetStockDataResponse(200, new HashMap<String, String>(), "missing fields parameter", false);

            String period = !event.containsKey("period") ? "a" : event.get("period");
            String ticker = event.get("ticker");
            List<String> tickers = new ArrayList<String>();
            if (ticker.contains(",")) {
                String[] ts = ticker.split(",");
                for (String t : ts)
                    tickers.add(t);
            } else
                tickers.add(ticker);
            String query_fields = event.get("field");
            List<String> fields = new ArrayList<String>();
            for (String f : query_fields.split(","))
                fields.add(f.toLowerCase());

            String url = System.getenv("database");
            String user = System.getenv("database_user");
            String password = System.getenv("database_password");
            this.conn = DriverManager.getConnection(url, user, password);
            String result = retreive(tickers, fields, period, this.conn);
            return new GetStockDataResponse(200, new HashMap<String, String>(), result, false);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        } finally {
            if (this.conn != null)
                try {
                    this.conn.close();
                } catch (Exception e) {
                }
        }
    }

    private static Hashtable<String, List<String>> GetMapTable(List<String> fields, String period, Connection conn)
            throws SQLException {
        Hashtable<String, List<String>> TableMap = new Hashtable<String, List<String>>();

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < fields.size(); i++)
            builder.append(i == fields.size() - 1 ? "?" : "?,");

        String query = "SELECT table_name, field_name FROM fundamental.ff_v3_ff_metadata WHERE field_name IN ("
                + builder.toString() + ") AND RIGHT(table_name, 3) = '_" + period
                + "f' AND table_name NOT LIKE '%/_r_%' ESCAPE '/' ";
        PreparedStatement stmt = conn.prepareStatement(query);
        for (int i = 1; i <= fields.size(); i++)
            stmt.setString(i, fields.get(i - 1));
        ResultSet rs = stmt.executeQuery();

        String FieldName, TableName;
        while (rs.next()) {
            TableName = rs.getString("table_name");
            FieldName = rs.getString("field_name");
            if (!TableMap.containsKey(TableName))
                TableMap.put(TableName, new ArrayList<String>());
            TableMap.get(TableName).add(FieldName);
        }
        stmt.close();
        return TableMap;
    }

    private static String retreive(List<String> tickers, List<String> fields, String period, Connection conn)
            throws SQLException {
        // retrieve field-table map from metadata table
        Hashtable<String, List<String>> fieldTableMap = GetMapTable(fields, period, conn);

        HashMap<String, TreeMap<String, HashMap<String, String>>> result = new HashMap<String, TreeMap<String, HashMap<String, String>>>();
        for (String table_name : fieldTableMap.keySet()) {
            StringBuilder field_param = new StringBuilder();
            field_param.append("ticker_region as ticker, date,");
            for (int i = 0; i < fieldTableMap.get(table_name).size(); i++)
                field_param.append(fieldTableMap.get(table_name).get(i)
                        + (i == fieldTableMap.get(table_name).size() - 1 ? "" : ","));

            StringBuilder ticker_param = new StringBuilder();
            for (int i = 0; i < tickers.size(); i++)
                ticker_param.append("?" + (i == tickers.size() - 1 ? "" : ","));

            String query = "SELECT " + field_param.toString() + " FROM fundamental.ff_v3_" + table_name
                    + " data_table JOIN fundamental.sym_v1_sym_ticker_region ticker_table ON "
                    + "data_table.fsym_id = ticker_table.fsym_id WHERE ticker_region IN (" + ticker_param.toString()
                    + ")";
            PreparedStatement stmt = conn.prepareStatement(query);
            for (int i = 1; i <= tickers.size(); i++)
                stmt.setString(i, tickers.get(i - 1));
            ResultSet rs = stmt.executeQuery();

            ResultSetMetaData rsmd = rs.getMetaData();
            int numColumns = rsmd.getColumnCount();
            while (rs.next()) {
                String ticker = rs.getString("ticker");
                String date = rs.getString("date");
                HashMap<String, String> rowData = new HashMap<String, String>();
                for (int i = 3; i <= numColumns; i++) {
                    String column_name = rsmd.getColumnName(i);
                    String value = rs.getString(column_name);
                    rowData.put(column_name, value);
                }
                if (!result.containsKey(ticker))
                    result.put(ticker, new TreeMap<String, HashMap<String, String>>());
                if (!result.get(ticker).containsKey(date))
                    result.get(ticker).put(date, rowData);
                else
                    result.get(ticker).get(date).putAll(rowData);
            }
            stmt.close();
        }
        return new Gson().toJson(result);
    }
}
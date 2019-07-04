package security_data;

import java.util.Map;

//import org.json.JSONArray;

public class GetStockDataResponse {

    private int statusCode;
    private Map<String, String> headers;
    private String body;
    private boolean isBase64Encoded;

    public GetStockDataResponse(int statusCode, Map<String, String> headers, String body, boolean isBase64Encoded) {
        this.statusCode = statusCode;
        this.headers = headers;
        this.body = body;
        this.isBase64Encoded = isBase64Encoded;
    }

    public int getStatusCode() {
        return this.statusCode;
    }

    public void setStatusCode(int data) {
        this.statusCode = data;
    }

    public String getBody() {
        return this.body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public boolean getIsBase64Encoded() {
        return this.isBase64Encoded;
    }

    public void setIsBase64Encoded(boolean data) {
        this.isBase64Encoded = data;
    }

    public Map<String, String> getHeaders() {
        return this.headers;
    }

    public void setHeaders(Map<String, String> data) {
        this.headers = data;
    }
}
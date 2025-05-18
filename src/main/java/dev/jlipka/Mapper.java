package dev.jlipka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Mapper<T> {

    private final ObjectMapper objectMapper;
    private final Class<T> targetClass;

    public Mapper(Class<T> targetClass) {
        this.objectMapper = new ObjectMapper();
        this.targetClass = targetClass;
    }

    public List<T> map(ResultSet rs) throws SQLException {
        List<T> results = new ArrayList<>();
        int columnCount = rs.getMetaData().getColumnCount();
        try {
            while (rs.next()) {
                Map<String, Object> jsonMap = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = rs.getMetaData().getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    jsonMap.put(columnName, columnValue);
                }
                String jsonString = objectMapper.writeValueAsString(jsonMap);
                T result = objectMapper.readValue(jsonString, targetClass);
                results.add(result);
            }
            return results;
        } catch (SQLException e) {
            throw new RuntimeException("Could not read values from ResultSet", e);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not parse value to JSON", e);
        }
    }
}
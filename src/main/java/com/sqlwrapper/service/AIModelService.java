package com.sqlwrapper.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sqlwrapper.config.AppConfig;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AIModelService {
    private static final Logger logger = LoggerFactory.getLogger(AIModelService.class);
    private final AppConfig config;
    private final HttpClient client;
    private final DatabaseService databaseService;
    private static final ObjectMapper mapper = new ObjectMapper();

    public AIModelService(AppConfig config, DatabaseService databaseService) {
        this.config = config;
        this.databaseService = databaseService;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();

        logger.info("Initialized AI service with provider: {}, model: {}",
                config.getAiProvider(), config.getAiModel());
    }

    public String generateSQL(String question) throws Exception {
        Map<String, List<String>> schemaInfo = databaseService.getSchemaInfo();
        String formattedSchemaInfo = formatSchemaForPrompt(schemaInfo);
        String systemPrompt = "You are a SQL assistant. Generate safe, executable SQL queries. " +
                "Never include dangerous operations like DROP, ALTER, DELETE, etc. " +
                "Only return the SQL query with no additional text. " +
                "Database Schema Details:\\n\\n" + formattedSchemaInfo +
                "Important: Only use tables and columns that exist in the schema above.\\n" +
                "Never generate queries for tables or columns that aren't listed here.\\n" +
                "For date ranges, use the appropriate database-specific functions. " +
                "Use table aliases for clarity. " +
                "Only use columns that exist in the schema.";

        String apiUrl;
        String provider = config.getAiProvider().toLowerCase();

        switch (provider) {
            case "openai":
                apiUrl = config.getAiApiBaseUrl() + "/chat/completions";
                break;
            case "anthropic":
                apiUrl = config.getAiApiBaseUrl() + "/messages";
                break;
            case "groq":
            default:
                apiUrl = config.getAiApiBaseUrl() + "/chat/completions";
        }

        String requestBody = String.format(
                "{\"model\": \"%s\", \"messages\": [{\"role\": \"system\", \"content\": \"%s\"}, {\"role\": \"user\", \"content\": \"%s\"}]}",
                config.getAiModel(),
                systemPrompt.replace("\"", "\\\"").replace("\n", "\\n"),
                question.replace("\"", "\\\"").replace("\n", "\\n"));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(apiUrl))
                .header("Authorization", "Bearer " + config.getGroqApiKey())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            logger.error("API request failed with status code: {}", response.statusCode());
            logger.error("Response body: {}", response.body());
            throw new RuntimeException("API request failed: " + response.statusCode() + " - " + response.body());
        }

        JsonNode rootNode = mapper.readTree(response.body());
        JsonNode choices = rootNode.path("choices");
        if (choices.isArray() && choices.size() > 0) {
            JsonNode message = choices.get(0).path("message");
            String sql = message.path("content").asText().trim();

            if (sql.startsWith("```sql") && sql.endsWith("```")) {
                sql = sql.substring(6, sql.length() - 3).trim();
            } else if (sql.startsWith("```") && sql.endsWith("```")) {
                sql = sql.substring(3, sql.length() - 3).trim();
            }

            return sql;
        }

        throw new RuntimeException("Invalid response format: no choices found");
    }

    private String formatSchemaForPrompt(Map<String, List<String>> schemaInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append("Database Schema Details:\\n\\n");

        if (schemaInfo.isEmpty()) {
            sb.append("No tables found in the database.\\n");
            return sb.toString();
        }

        for (Map.Entry<String, List<String>> entry : schemaInfo.entrySet()) {
            String tableName = entry.getKey();
            List<String> columns = entry.getValue();

            sb.append("Table: ").append(tableName).append("\\n");
            sb.append("Columns: ").append(String.join(", ", columns)).append("\\n\\n");
        }

        sb.append("Important: Only use tables and columns that exist in the schema above.\\n");
        sb.append("Never generate queries for tables or columns that aren't listed here.\\n");

        return sb.toString();
    }
}
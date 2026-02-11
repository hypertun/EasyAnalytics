package com.sqlwrapper.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sqlwrapper.config.AppConfig;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AIModelService {
    private static final Logger logger = LoggerFactory.getLogger(AIModelService.class);
    private final AppConfig config;
    private final HttpClient client;
    private static final ObjectMapper mapper = new ObjectMapper();
    
    public AIModelService(AppConfig config) {
        this.config = config;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
    }

    public String generateSQL(String question) throws Exception {
        String prompt = String.format(
                "You are a SQL assistant. Generate safe, executable SQL queries. Never include dangerous operations like DROP, ALTER, DELETE, etc. Only return the SQL query with no additional text. " +
                "The database schema is: [your schema details here]. " +
                "For date ranges, use the appropriate database-specific functions. " +
                "Use table aliases for clarity. " +
                "Only use columns that exist in the schema. " +
                "Question: %s",
                question
        );

        String requestBody = String.format(
                """
                {
                    "model": "llama-3.1-8b-instant",
                    "messages": [
                        {
                            "role": "system",
                            "content": "%s"
                        },
                        {
                            "role": "user",
                            "content": "%s"
                        }
                    ]
                }
                """,
                "You are a SQL assistant. Generate safe, executable SQL queries. Never include dangerous operations like DROP, ALTER, DELETE, etc. Only return the SQL query with no additional text.",
                question
        );

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://api.groq.com/openai/v1/chat/completions"))
                .header("Authorization", "Bearer " + config.getGroqApiKey())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        try {
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
                
                // Clean up the response - remove any markdown formatting
                if (sql.startsWith("```sql") && sql.endsWith("```")) {
                    sql = sql.substring(6, sql.length() - 3).trim();
                } else if (sql.startsWith("```") && sql.endsWith("```")) {
                    sql = sql.substring(3, sql.length() - 3).trim();
                }
                
                return sql;
            }
            
            throw new RuntimeException("Invalid response format: no choices found");
        } catch (Exception e) {
            logger.error("Error calling AI service", e);
            throw e;
        }
    }
}
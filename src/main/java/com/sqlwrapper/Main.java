package com.sqlwrapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sqlwrapper.config.AppConfig;
import com.sqlwrapper.service.AIModelService;
import com.sqlwrapper.service.DatabaseService;
import com.sqlwrapper.service.SQLValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        AppConfig config = null;
        DatabaseService dbService = null;
        Scanner scanner = null;
        
        try {
            config = AppConfig.load();
            dbService = new DatabaseService(config);
            AIModelService aiService = new AIModelService(config);
            SQLValidator validator = new SQLValidator();

            scanner = new Scanner(System.in);

            System.out.println("Welcome to SQL Analytics Assistant");
            System.out.println("Type 'exit' to quit");
            System.out.println("Enter your analytics question:");
            
            // Get the actual database schema ONCE at startup
            Map<String, List<String>> schemaInfo;
            try {
                schemaInfo = dbService.getSchemaInfo();
                logger.info("Database schema loaded with {} tables", schemaInfo.size());
                
                // Show schema summary to user
                System.out.println("\n✅ Connected to database with " + schemaInfo.size() + " tables");
                System.out.println("Available tables:");
                for (String table : schemaInfo.keySet()) {
                    System.out.println("  - " + table + " (" + schemaInfo.get(table).size() + " columns)");
                }
            } catch (Exception e) {
                logger.error("Failed to retrieve database schema", e);
                System.err.println("❌ Error: Failed to connect to database - " + e.getMessage());
                // Don't return immediately - close resources first
                throw e; // Rethrow to be caught by outer try-catch
            }

            while (true) {
                System.out.print("\n> ");
                String question = scanner.nextLine().trim();

                if ("exit".equalsIgnoreCase(question)) {
                    break;
                }

                if (question.isEmpty()) {
                    continue;
                }

                try {
                    // Pass the ACTUAL schema information to the AI service
                    String sql = aiService.generateSQL(question, formatSchemaForPrompt(schemaInfo));
                    logger.info("Generated SQL: {}", sql);

                    if (!validator.isValid(sql)) {
                        logger.error("Invalid SQL detected: {}", sql);
                        System.out.println("❌ Error: Generated SQL is not safe");
                        continue;
                    }

                    System.out.println("\n✅ Executing query:");
                    System.out.println(sql);

                    List<Map<String, Object>> results = dbService.executeQuery(sql, config.getMaxQueryRows());
                    logger.info("Query results: {}", mapper.writeValueAsString(results));

                    System.out.println("\n📊 Query Results:");
                    if (results.isEmpty()) {
                        System.out.println("No results found");
                    } else {
                        // Print header
                        for (Map<String, Object> row : results) {
                            for (String key : row.keySet()) {
                                System.out.printf("%-20s", key);
                            }
                            System.out.println();
                            break;
                        }

                        // Print separator
                        for (int i = 0; i < results.get(0).keySet().size() * 20; i++) {
                            System.out.print("-");
                        }
                        System.out.println();

                        // Print rows
                        for (Map<String, Object> row : results) {
                            for (Object value : row.values()) {
                                System.out.printf("%-20s", value != null ? value.toString() : "NULL");
                            }
                            System.out.println();
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error processing query", e);
                    System.err.println("Error: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            // Handle initialization errors
            logger.error("Application initialization failed", e);
        } finally {
            // Clean up resources in all cases
            if (dbService != null) {
                dbService.close();
            }
            if (scanner != null) {
                scanner.close();
            }
            System.out.println("\nGoodbye!");
        }
    }
    
    /**
     * Formats the database schema into a human-readable string for the AI prompt
     */
    private static String formatSchemaForPrompt(Map<String, List<String>> schemaInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append("Database Schema Details:\n\n");
        
        if (schemaInfo.isEmpty()) {
            sb.append("No tables found in the database.\n");
            return sb.toString();
        }
        
        for (Map.Entry<String, List<String>> entry : schemaInfo.entrySet()) {
            String tableName = entry.getKey();
            List<String> columns = entry.getValue();
            
            sb.append("Table: ").append(tableName).append("\n");
            sb.append("Columns: ").append(String.join(", ", columns)).append("\n\n");
        }
        
        sb.append("Important: Only use tables and columns that exist in the schema above.\n");
        sb.append("Never generate queries for tables or columns that aren't listed here.\n");
        
        return sb.toString();
    }
}
package com.sqlwrapper.config;

import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppConfig {
    private static final Logger logger = LoggerFactory.getLogger(AppConfig.class);
    
    private final String groqApiKey;
    private final String databaseName;
    private final String databaseUser;
    private final String databasePassword;
    private final String databaseHost;
    private final int databasePort;
    private final int maxQueryRows;
    private final int queryTimeoutSeconds;
    // SSH Config
    private final boolean sshEnabled;
    private final String sshHost;
    private final int sshPort;
    private final String sshUsername;
    private final String sshPrivateKeyPath;
    private final String databaseType;
    

    public static AppConfig load() {
        Dotenv dotenv = Dotenv.configure()
            .directory(".")
            .load();

        String groqApiKey = dotenv.get("GROQ_API_KEY");
        String databaseName = dotenv.get("DATABASE_NAME");
        String databaseUser = dotenv.get("DATABASE_USER");
        String databasePassword = dotenv.get("DATABASE_PASSWORD");
        String databaseHost = dotenv.get("DATABASE_HOST");
        String databasePortStr = dotenv.get("DATABASE_PORT", "3306");
        String maxQueryRowsStr = dotenv.get("MAX_QUERY_ROWS", "1000");
        String queryTimeoutSecondsStr = dotenv.get("QUERY_TIMEOUT_SECONDS", "30");
        String databaseType = dotenv.get("DATABASE_TYPE", "mysql");

        boolean sshEnabled = Boolean.parseBoolean(dotenv.get("SSH_ENABLED", "false"));
        String sshHost = dotenv.get("SSH_HOST");
        String sshPortStr = dotenv.get("SSH_PORT", "22");
        String sshUsername = dotenv.get("SSH_USERNAME");
        String sshPrivateKeyPath = dotenv.get("SSH_PRIVATE_KEY_PATH");

        // Validate required fields
        if (groqApiKey == null || groqApiKey.trim().isEmpty()) {
            throw new RuntimeException("GROQ_API_KEY is missing in .env!");
        }
        if (databaseName == null || databaseName.trim().isEmpty()) {
            throw new RuntimeException("DATABASE_NAME is missing in .env!");
        }
        if (databaseUser == null || databaseUser.trim().isEmpty()) {
            throw new RuntimeException("DATABASE_USER is missing in .env!");
        }
        if (databasePassword == null || databasePassword.trim().isEmpty()) {
            throw new RuntimeException("DATABASE_PASSWORD is missing in .env!");
        }
        if (databaseHost == null || databaseHost.trim().isEmpty()) {
            throw new RuntimeException("DATABASE_HOST is missing in .env!");
        }

        // Parse numeric values with validation
        int databasePort;
        try {
            databasePort = Integer.parseInt(databasePortStr);
            if (databasePort <= 0 || databasePort > 65535) {
                throw new NumberFormatException("Invalid port range");
            }
        } catch (NumberFormatException e) {
            throw new RuntimeException("DATABASE_PORT must be a valid port number (1-65535)", e);
        }

        int maxQueryRows;
        try {
            maxQueryRows = Integer.parseInt(maxQueryRowsStr);
            if (maxQueryRows <= 0) {
                throw new NumberFormatException("Must be positive");
            }
        } catch (NumberFormatException e) {
            throw new RuntimeException("MAX_QUERY_ROWS must be a positive integer", e);
        }

        int queryTimeoutSeconds;
        try {
            queryTimeoutSeconds = Integer.parseInt(queryTimeoutSecondsStr);
            if (queryTimeoutSeconds <= 0) {
                throw new NumberFormatException("Must be positive");
            }
        } catch (NumberFormatException e) {
            throw new RuntimeException("QUERY_TIMEOUT_SECONDS must be a positive integer", e);
        }

        int sshPort;
        try {
            sshPort = Integer.parseInt(sshPortStr);
            if (sshPort <= 0 || sshPort > 65535) {
                throw new NumberFormatException("Invalid port range");
            }
        } catch (NumberFormatException e) {
            throw new RuntimeException("SSH_PORT must be a valid port number (1-65535)", e);
        }

        // If SSH is enabled, validate SSH fields
        if (sshEnabled) {
            if (sshHost == null || sshHost.trim().isEmpty()) {
                throw new RuntimeException("SSH_HOST is required when SSH_ENABLED=true!");
            }
            if (sshUsername == null || sshUsername.trim().isEmpty()) {
                throw new RuntimeException("SSH_USERNAME is required when SSH_ENABLED=true!");
            }
            if (sshPrivateKeyPath == null || sshPrivateKeyPath.trim().isEmpty()) {
                throw new RuntimeException("SSH_PRIVATE_KEY_PATH is required when SSH_ENABLED=true!");
            }
        }

        logger.info("Configuration loaded successfully");
        return new AppConfig(
            groqApiKey,
            databaseName,
            databaseUser,
            databasePassword,
            databaseHost,
            databasePort,
            maxQueryRows,
            queryTimeoutSeconds,
            sshEnabled,
            sshHost,
            sshPort,
            sshUsername,
            sshPrivateKeyPath,
            databaseType
        );
    }

    public AppConfig(String groqApiKey, String databaseName, String databaseUser, String databasePassword,
                     String databaseHost, int databasePort, int maxQueryRows, int queryTimeoutSeconds,
                     boolean sshEnabled, String sshHost, int sshPort, String sshUsername, String sshPrivateKeyPath,
                     String databaseType) {
        this.groqApiKey = groqApiKey;
        this.databaseName = databaseName;
        this.databaseUser = databaseUser;
        this.databasePassword = databasePassword;
        this.databaseHost = databaseHost;
        this.databasePort = databasePort;
        this.maxQueryRows = maxQueryRows;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
        this.sshEnabled = sshEnabled;
        this.sshHost = sshHost;
        this.sshPort = sshPort;
        this.sshUsername = sshUsername;
        this.sshPrivateKeyPath = sshPrivateKeyPath;
        this.databaseType = databaseType;
    }

    // Getters
    public String getGroqApiKey() { return groqApiKey; }
    public String getDatabaseName() { return databaseName; }
    public String getDatabaseUser() { return databaseUser; }
    public String getDatabasePassword() { return databasePassword; }
    public String getDatabaseHost() { return databaseHost; }
    public int getDatabasePort() { return databasePort; }
    public int getMaxQueryRows() { return maxQueryRows; }
    public int getQueryTimeoutSeconds() { return queryTimeoutSeconds; }
    public String getDatabaseType() { return databaseType; }

    // SSH getters
    public boolean isSshEnabled() { return sshEnabled; }
    public String getSshHost() { return sshHost; }
    public int getSshPort() { return sshPort; }
    public String getSshUsername() { return sshUsername; }
    public String getSshPrivateKeyPath() { return sshPrivateKeyPath; }
}
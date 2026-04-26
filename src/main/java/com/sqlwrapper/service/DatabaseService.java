package com.sqlwrapper.service;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.client.keyverifier.AcceptAllServerKeyVerifier;
import org.apache.sshd.common.util.net.SshdSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.sshd.common.config.keys.FilePasswordProvider;
import org.apache.sshd.common.keyprovider.FileKeyPairProvider;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.sqlwrapper.config.AppConfig;
import java.nio.file.Paths;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.security.KeyPair;
import java.util.Iterator;

public class DatabaseService {
    private final HikariDataSource dataSource;
    private ClientSession sshSession;
    private SshClient sshClient;
    private final AppConfig config;  // Added to store config for database type
    private static final Logger logger = LoggerFactory.getLogger(DatabaseService.class);
    private static final int SSH_TUNNEL_LOCAL_PORT = 3307;  // Configurable SSH port
    private Map<String, List<String>> cachedSchema;
    private long lastSchemaFetchTime; // Timestamp in milliseconds
    private final long schemaCacheTtlMs; // Configurable TTL
    private final String schemaCacheFilePath; // Path to cache file
    private final ObjectMapper objectMapper = new ObjectMapper(); // Jackson ObjectMapper

    // New constructor for dependency injection (testability)
    public DatabaseService(AppConfig config, HikariDataSource dataSource) {
        this.config = config;
        this.dataSource = dataSource;
        this.sshSession = null;
        this.sshClient = null; // Ensure sshClient is null initially
        this.cachedSchema = null;
        this.lastSchemaFetchTime = 0;

        // Initialize from config
        this.schemaCacheTtlMs = config.getSchemaCacheTtlMs();
        this.schemaCacheFilePath = config.getSchemaCacheFilePath();

        logger.info("DatabaseService initialized with injected DataSource. Host: {}, Port: {}",
                config.getDatabaseHost(), config.getDatabasePort());
    }

    public DatabaseService(AppConfig config) {
        // Validate database type early
        String dbType = config.getDatabaseType().toLowerCase();
        if (!"mysql".equals(dbType) && !"postgresql".equals(dbType) &&
            !"mssql".equals(dbType) && !"oracle".equals(dbType)) {
            throw new IllegalArgumentException("Unsupported database type: " + dbType +
                ". Supported types: mysql, postgresql, mssql, oracle");
        }

        // Store config for use in other methods
        this.config = config;

        // Initialize from config
        this.schemaCacheTtlMs = config.getSchemaCacheTtlMs();
        this.schemaCacheFilePath = config.getSchemaCacheFilePath();

        this.sshSession = null;
        this.cachedSchema = null;
        this.lastSchemaFetchTime = 0;

        if (config.isSshEnabled()) {
            this.sshSession = setupSSHTunnel(config);
        }

        String host = config.isSshEnabled() ? "localhost" : config.getDatabaseHost();
        int port = config.isSshEnabled() ? SSH_TUNNEL_LOCAL_PORT : config.getDatabasePort();

        HikariConfig hikariConfig = new HikariConfig();
        String jdbcUrl;
        switch (config.getDatabaseType().toLowerCase()) {
            case "mysql":
                jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?%s",
                        host, port, config.getDatabaseName(), config.getDatabaseProperties());
                break;
            case "postgresql":
                jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s?%s",
                        host, port, config.getDatabaseName(), config.getDatabaseProperties());
                break;
            case "mssql":
                jdbcUrl = String.format("jdbc:sqlserver://%s:%d;databaseName=%s;%s",
                        host, port, config.getDatabaseName(), config.getDatabaseProperties());
                break;
            case "oracle":
                jdbcUrl = String.format("jdbc:oracle:thin:@//%s:%d/%s",
                        host, port, config.getDatabaseName());
                break;
            default:
                throw new IllegalArgumentException("Unsupported database type: " + config.getDatabaseType());
        }
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(config.getDatabaseUser());
        hikariConfig.setPassword(config.getDatabasePassword());
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setConnectionTimeout(config.getQueryTimeoutSeconds() * 1000);
        hikariConfig.setIdleTimeout(60000);
        hikariConfig.setMaxLifetime(1800000);

        this.dataSource = new HikariDataSource(hikariConfig);

        logger.info("Database connection established. Host: {}, Port: {}", host, port);
    }

    private ClientSession setupSSHTunnel(AppConfig config) {
        try {
            sshClient = SshClient.setUpDefaultClient();
            sshClient.setServerKeyVerifier(AcceptAllServerKeyVerifier.INSTANCE);
            sshClient.start();

            logger.info("Establishing SSH tunnel to {}:{}...", config.getSshHost(), config.getSshPort());

            ClientSession session = sshClient.connect(
                    config.getSshUsername(),
                    config.getSshHost(),
                    config.getSshPort()).verify(10000).getSession();

            FileKeyPairProvider keyProvider = new FileKeyPairProvider(Paths.get(config.getSshPrivateKeyPath()));
            keyProvider.setPasswordFinder(FilePasswordProvider.EMPTY);
            Iterable<KeyPair> keys = keyProvider.loadKeys(session);

            for (KeyPair kp : keys) {
                session.addPublicKeyIdentity(kp);
            }

            session.auth().verify(10000);

            SshdSocketAddress local = new SshdSocketAddress("localhost", SSH_TUNNEL_LOCAL_PORT);
            SshdSocketAddress remote = new SshdSocketAddress(config.getDatabaseHost(), config.getDatabasePort());
            session.startLocalPortForwarding(local, remote);

            logger.info("SSH tunnel established successfully");
            return session;
        } catch (Exception e) {
            logger.error("Failed to establish SSH tunnel", e);
            throw new RuntimeException("Failed to establish SSH tunnel", e);
        }
    }

    public List<Map<String, Object>> executeQuery(String sql, int maxRows) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            int rowCount = 0;
            while (rs.next() && rowCount < maxRows) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);
                    row.put(columnName, value);
                }
                results.add(row);
                rowCount++;
            }
        }

        logger.info("Executed query with {} rows returned", results.size());
        return results;
    }

    public void close() {
        try {
            if (dataSource != null && !dataSource.isClosed()) {
                dataSource.close();
                logger.info("Database connection closed");
            }
            if (sshSession != null) {
                sshSession.close();
                logger.info("SSH session closed");
            }
            if (sshClient != null) {
                sshClient.stop();
                logger.info("SSH client stopped");
            }
        } catch (IOException e) {
            logger.error("Error closing resources", e);
        }
    }

    // New method to load schema from cache file
    private Map<String, List<String>> loadSchemaFromCacheFile() throws IOException {
        File cacheFile = new File(schemaCacheFilePath);
        if (!cacheFile.exists()) {
            logger.info("Schema cache file not found at: {}", schemaCacheFilePath);
            return null;
        }

        FileTime lastModified = Files.getLastModifiedTime(cacheFile.toPath());
        long lastModifiedTimeMillis = lastModified.toMillis();

        if ((System.currentTimeMillis() - lastModifiedTimeMillis) >= schemaCacheTtlMs) {
            logger.info("Schema cache file is stale (last modified: {}). TTL: {}", lastModifiedTimeMillis, schemaCacheTtlMs);
            // Optionally delete stale file: cacheFile.delete();
            return null;
        }

        logger.info("Loading schema from cache file: {}", schemaCacheFilePath);
        try {
            String content = new String(Files.readAllBytes(cacheFile.toPath()));
            TypeReference<HashMap<String, List<String>>> typeRef = new TypeReference<>() {};
            return objectMapper.readValue(content, typeRef);
        } catch (IOException e) {
            logger.error("Error reading schema cache file: {}", schemaCacheFilePath, e);
            // Optionally delete corrupted file: cacheFile.delete();
            return null; // Treat as cache miss if file is corrupted
        }
    }

    // New method to save schema to cache file
    private void saveSchemaToCacheFile(Map<String, List<String>> schema) throws IOException {
        File cacheFile = new File(schemaCacheFilePath);
        // Ensure parent directory exists
        File parentDir = cacheFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            if (!parentDir.mkdirs()) {
                logger.error("Failed to create parent directories for schema cache file: {}", parentDir.getAbsolutePath());
                // Decide how to handle this: throw exception or log and continue without saving
                return;
            }
        }

        try {
            String jsonContent = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
            Files.write(cacheFile.toPath(), jsonContent.getBytes());
            logger.info("Schema saved to cache file: {}", schemaCacheFilePath);
        } catch (IOException e) {
            logger.error("Error writing schema to cache file: {}", schemaCacheFilePath, e);
            // Decide how to handle this: throw exception or log and continue
        }
    }

    // Thread-safety improvement: Synchronize access to schema operations
    public synchronized Map<String, List<String>> getSchemaInfoThreadSafe() throws SQLException {
        // Check in-memory cache first
        if (cachedSchema != null && (System.currentTimeMillis() - lastSchemaFetchTime) < schemaCacheTtlMs) {
            logger.info("Returning schema from in-memory cache.");
            return cachedSchema;
        }

        // Try to load from persistent cache file
        Map<String, List<String>> schema = null;
        try {
            schema = loadSchemaFromCacheFile();
            if (schema != null) {
                // Update in-memory cache and timestamp
                this.cachedSchema = schema;
                this.lastSchemaFetchTime = System.currentTimeMillis(); // Update timestamp to reflect file load time
                logger.info("Schema loaded from persistent cache file.");
                return schema;
            }
        } catch (IOException e) {
            logger.error("Failed to load schema from cache file, proceeding to fetch from DB.", e);
            // Continue to fetch from DB if file loading fails
        }

        // If not found in memory or persistent cache, fetch from database
        logger.info("Fetching schema from database.");
        Map<String, List<String>> dbSchema = new HashMap<>();

        try (Connection conn = dataSource.getConnection()) {
            String tableNameQuery;
            String columnQueryTemplate;

            switch (config.getDatabaseType().toLowerCase()) {
                case "mysql":
                    tableNameQuery = "SHOW TABLES";
                    columnQueryTemplate = "DESCRIBE %s";
                    break;
                case "postgresql":
                    tableNameQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";
                    columnQueryTemplate = "SELECT column_name FROM information_schema.columns WHERE table_name = '%s'";
                    break;
                case "mssql":
                    tableNameQuery = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";
                    columnQueryTemplate = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'";
                    break;
                case "oracle":
                    tableNameQuery = "SELECT TABLE_NAME FROM USER_TABLES";
                    columnQueryTemplate = "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '%s'";
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported database type: " + config.getDatabaseType());
            }

            try (Statement tableStmt = conn.createStatement();
                 ResultSet tables = tableStmt.executeQuery(tableNameQuery)) {

                while (tables.next()) {
                    String tableName = tables.getString(1);
                    List<String> columns = new ArrayList<>();

                    try (Statement columnStmt = conn.createStatement();
                         ResultSet columnsRS = columnStmt
                                 .executeQuery(String.format(columnQueryTemplate, tableName))) {

                        while (columnsRS.next()) {
                            columns.add(columnsRS.getString(1));
                        }
                    }

                    dbSchema.put(tableName, columns);
                }
            }
        }

        // Update in-memory cache and save to persistent cache file
        this.cachedSchema = dbSchema;
        this.lastSchemaFetchTime = System.currentTimeMillis();
        try {
            saveSchemaToCacheFile(dbSchema);
        } catch (IOException e) {
            logger.error("Failed to save schema to cache file after fetching from DB.", e);
        }

        return dbSchema;
    }
    
    // Method to get the current schema, using the thread-safe version
    public Map<String, List<String>> getSchemaInfo() throws SQLException {
        return getSchemaInfoThreadSafe();
    }
    
    // Method to explicitly clear the cache (useful for testing or manual refresh)
    public synchronized void clearSchemaCache() {
        this.cachedSchema = null;
        this.lastSchemaFetchTime = 0;
        File cacheFile = new File(schemaCacheFilePath);
        if (cacheFile.exists()) {
            if (cacheFile.delete()) {
                logger.info("Schema cache file deleted: {}", schemaCacheFilePath);
            } else {
                logger.error("Failed to delete schema cache file: {}", schemaCacheFilePath);
            }
        }
    }
}
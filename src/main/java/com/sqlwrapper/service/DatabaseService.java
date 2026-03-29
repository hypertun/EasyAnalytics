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
import java.security.KeyPair;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.IOException;

public class DatabaseService {
    private final HikariDataSource dataSource;
    private ClientSession sshSession;
    private SshClient sshClient;
    private final AppConfig config;  // Added to store config for database type
    private static final Logger logger = LoggerFactory.getLogger(DatabaseService.class);
    private static final int SSH_TUNNEL_LOCAL_PORT = 3307;  // Configurable SSH port

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
        
        this.sshSession = null;

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

    public Map<String, List<String>> getSchemaInfo() throws SQLException {
        Map<String, List<String>> schema = new HashMap<>();
        
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

                    // Use a separate statement for getting columns
                    try (Statement columnStmt = conn.createStatement();
                            ResultSet columnsRS = columnStmt
                                    .executeQuery(String.format(columnQueryTemplate, tableName))) {

                        while (columnsRS.next()) {
                            columns.add(columnsRS.getString(1));
                        }
                    }

                    schema.put(tableName, columns);
                }
            }
        }
        
        return schema;
    }
}
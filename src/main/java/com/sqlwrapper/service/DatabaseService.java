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
    private static final Logger logger = LoggerFactory.getLogger(DatabaseService.class);

    public DatabaseService(AppConfig config) {
        this.sshSession = null;

        if (config.isSshEnabled()) {
            this.sshSession = setupSSHTunnel(config);
        }

        String host = config.isSshEnabled() ? "localhost" : config.getDatabaseHost();
        int port = config.isSshEnabled() ? 3307 : config.getDatabasePort();

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(String.format("jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                host, port, config.getDatabaseName()));
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
                    config.getSshPort()
            ).verify(10000).getSession();

            FileKeyPairProvider keyProvider = new FileKeyPairProvider(Paths.get(config.getSshPrivateKeyPath()));
            keyProvider.setPasswordFinder(FilePasswordProvider.EMPTY);
            Iterable<KeyPair> keys = keyProvider.loadKeys(session);
            
            for (KeyPair kp : keys) {
                session.addPublicKeyIdentity(kp);
            }
            
            session.auth().verify(10000);

            SshdSocketAddress local = new SshdSocketAddress("localhost", 3307);
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

    try (Connection conn = dataSource.getConnection();
         Statement stmt = conn.createStatement();
         ResultSet tables = stmt.executeQuery("SHOW TABLES")) {

        while (tables.next()) {
            String tableName = tables.getString(1);
            List<String> columns = new ArrayList<>();

            ResultSet columnsRS = stmt.executeQuery("DESCRIBE " + tableName);
            while (columnsRS.next()) {
                columns.add(columnsRS.getString("Field"));
            }
            columnsRS.close();

            schema.put(tableName, columns);
        }
    }

    return schema;
}
}
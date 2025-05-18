package dev.jlipka.pool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class GoodConnectionPool implements ObjectPool<Connection> {

    private static final int MAX_DB_PING_TIMEOUT = 2;

    private final Queue<Connection> pool;

    private final Set<Connection> usedConnections;

    private GoodConnectionPool(QuizDataSourceBuilder builder) {
        this.pool = new LinkedBlockingQueue<>();
        this.usedConnections = ConcurrentHashMap.newKeySet();

        try {
            if (builder.driver != null && !builder.driver.isEmpty()) {
                Class.forName(builder.driver);
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load database driver");
        }

        try {
            for (int i = 0; i < builder.poolSize; i++) {
                Connection connection = DriverManager.getConnection(
                        builder.url, builder.user, builder.password);
                pool.add(connection);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<Connection> take() {
        Connection connection = pool.poll();

        if (!isConnectionValid(connection)) {
            return Optional.empty();
        }

        usedConnections.add(connection);
        return Optional.of(connection);
    }

    public void release(Connection connection) {
        if (isConnectionValid(connection) && usedConnections.remove(connection)) {
            pool.offer(connection);
        }
    }

    @Override
    public int getSize() {
        return pool.size();
    }

    private boolean isConnectionValid(Connection connection) {
        if (connection == null) {
            return false;
        }

        try {
            return !connection.isClosed() && connection.isValid(MAX_DB_PING_TIMEOUT);
        } catch (SQLException e) {
            return false;
        }
    }

    public static class QuizDataSourceBuilder {
        private String url;
        private String user;
        private String password;
        private int poolSize;
        private String driver;

        private QuizDataSourceBuilder() {
        }

        public static QuizDataSourceBuilder builder() {
            return new QuizDataSourceBuilder();
        }

        public GoodConnectionPool build() {
            validate();
            return new GoodConnectionPool(this);
        }

        private void validate() {
            if (url == null || url.trim()
                    .isEmpty()) {
                throw new IllegalStateException("URL cannot be null or empty");
            }
            if (user == null) {
                throw new IllegalStateException("User cannot be null");
            }
            if (password == null) {
                throw new IllegalStateException("Password cannot be null");
            }
            if (poolSize <= 0) {
                throw new IllegalStateException("Pool size must be positive");
            }
        }

        public QuizDataSourceBuilder url(String url) {
            this.url = url;
            return this;
        }

        public QuizDataSourceBuilder user(String user) {
            this.user = user;
            return this;
        }

        public QuizDataSourceBuilder password(String password) {
            this.password = password;
            return this;
        }

        public QuizDataSourceBuilder poolSize(int poolSize) {
            this.poolSize = poolSize;
            return this;
        }

        public QuizDataSourceBuilder driver(String driver) {
            this.driver = driver;
            return this;
        }
    }
}
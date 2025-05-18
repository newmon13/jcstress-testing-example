package dev.jlipka.pool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

public class BadConnectionPool implements ObjectPool<Connection> {

    private static final int MAX_DB_PING_TIMEOUT = 2;

    private final Queue<Connection> pool;

    private final Set<Connection> usedConnections;

    private BadConnectionPool(QuizDataSourceBuilder builder) {
        this.pool = new LinkedList<>();
        this.usedConnections = new HashSet<>();
        try {
            if (builder.driver != null && !builder.driver.isEmpty())
                Class.forName(builder.driver);
            for (int i = 0; i < builder.poolSize; i++) {
                Connection connection = DriverManager.getConnection(
                        builder.url, builder.user, builder.password
                );
                pool.add(connection);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Optional<Connection> take() {
        Connection connection = pool.poll();

        if (!isConnectionValid(connection))
            return Optional.empty();

        usedConnections.add(connection);
        return Optional.of(connection);
    }

    public void release(Connection connection) {
        if (isConnectionValid(connection) && usedConnections.remove(connection)) {
            pool.offer(connection);
        }
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

    @Override
    public int getSize() {
        return pool.size();
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

        public BadConnectionPool build() {
            validate();
            return  new BadConnectionPool(this);
        }

        private void validate() {
            if (url == null || url.trim().isEmpty()) {
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
package dev.jlipka;

import dev.jlipka.pool.BadConnectionPool;
import dev.jlipka.pool.ObjectPool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {
    public static void main(String[] args) throws SQLException {
        ObjectPool<Connection> pool = BadConnectionPool.QuizDataSourceBuilder.builder()
                .url("jdbc:mysql://localhost:3306/quizdb")
                .user("root")
                .password("root")
                .driver("com.mysql.cj.jdbc.Driver")
                .poolSize(100)
                .build();

        int testValue = 0;
        Optional<Connection> initialConnectionOpt = pool.take();
        if (initialConnectionOpt.isPresent()) {
            Connection connection = initialConnectionOpt.get();
            try {
                Statement statement = connection.createStatement();
                statement.execute("DELETE FROM quizdb.test");
                String sql = String.format("INSERT INTO test (value) VALUES (%d);", testValue);
                statement.execute(sql);
                statement.close();
            } finally {
                pool.release(connection);
            }
        }

        Mapper<TestEntity> mapper = new Mapper<>(TestEntity.class);
        ExecutorService executorService = Executors.newFixedThreadPool(100);

        for (int i = 0; i < 100; i++) {
            executorService.submit(() -> {
                Optional<Connection> optionalConnection = pool.take();
                if (optionalConnection.isPresent()) {
                    Connection connection = optionalConnection.get();
                    try {
                        connection.setAutoCommit(false);
                        Statement stat = connection.createStatement();
                        ResultSet resultSet = stat.executeQuery("SELECT * FROM quizdb.test FOR UPDATE");
                        List<TestEntity> map = mapper.map(resultSet);
                        TestEntity testEntity = map.get(0);
                        String updateQuery = String.format(
                                "UPDATE test SET value=%d WHERE id=%d;", testEntity.value() + 1, testEntity.id());
                        stat.executeUpdate(updateQuery);

                        connection.commit();
                        resultSet.close();
                        stat.close();
                        connection.setAutoCommit(true);

                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    } finally {
                        pool.release(connection);
                    }
                }
            });
        }
        executorService.shutdown();
    }
}
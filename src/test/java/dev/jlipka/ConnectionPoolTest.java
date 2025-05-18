package dev.jlipka;

import dev.jlipka.pool.BadConnectionPool;
import dev.jlipka.pool.ObjectPool;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

@JCStressTest
@Outcome(id = "99, 0", expect = ACCEPTABLE,
        desc = "All connections were returned and corrupted"
                + " connection was rejected by pool")
@Outcome(id = "99, 1", expect = FORBIDDEN,
        desc = "One connection was closed and pool did not take it back,"
                + " registered one corrupted connection")
@Outcome(id = "100, 0", expect = FORBIDDEN,
        desc = "One connection was closed and successfully returned to pool")
@Outcome(id = "100, 1", expect = FORBIDDEN,
        desc = "One connection was closed and successfully returned"
                + " to pool plus registered one corrupted connection")
@State
public class ConnectionPoolTest {
    private final ObjectPool<Connection> pool;
    private final AtomicInteger incorrectConnectionCount = new AtomicInteger(0);
    private final AtomicInteger activeConnectionsMax = new AtomicInteger(0);

    public ConnectionPoolTest() {
        pool = BadConnectionPool.QuizDataSourceBuilder.builder()
                .url("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1")
                .user("sa")
                .password("")
                .poolSize(100)
                .driver("org.h2.Driver")
                .build();
    }

    @Actor
    public void actor1() {
        Optional<Connection> take = pool.take();
        if (take.isPresent()) {
            Connection connection = take.get();
            try {
                connection.getSchema();
                connection.close();
            } catch (SQLException e) {
                incorrectConnectionCount.incrementAndGet();
            } finally {
                pool.release(connection);
                activeConnectionsMax.decrementAndGet();
            }
        } else {
            incorrectConnectionCount.incrementAndGet();
        }
    }

    @Actor
    public void actor2() {
        for (int i = 0; i < 10; i++) {
            Optional<Connection> connectionOpt = pool.take();
            if (connectionOpt.isPresent()) {
                Connection connection = connectionOpt.get();
                pool.release(connection);
            } else {
                incorrectConnectionCount.incrementAndGet();
            }
        }
    }

    @Arbiter
    public void arbiter(II_Result r) {
        r.r1 = pool.getSize();
        r.r2 = incorrectConnectionCount.get();
    }
}
package org.apache.cmueller.camel.samples.camelone;

import javax.sql.DataSource;

import org.apache.camel.model.ModelCamelContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

public class DatabaseUtil {

    public static JdbcTemplate createJdbcTemplate(ModelCamelContext context) {
        DataSource ds = context.getRegistry().lookupByNameAndType("dataSource", DataSource.class);
        return new JdbcTemplate(ds);
    }

    public static TransactionTemplate createTransactionTemplate(ModelCamelContext context, String transactionManagerName) {
        PlatformTransactionManager transactionManager = context.getRegistry().lookupByNameAndType(transactionManagerName, PlatformTransactionManager.class);
        return new TransactionTemplate(transactionManager);
    }

    public static void createAndInitializeDatabase(TransactionTemplate transactionTemplate, final JdbcTemplate jdbc) {
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                jdbc.execute("CREATE TABLE account (name VARCHAR(50), balance BIGINT)");
                jdbc.execute("INSERT INTO account VALUES('foo',1000)");
                jdbc.execute("INSERT INTO account VALUES('bar',1000)");
            }
        });
    }

    public static void dropDatabase(TransactionTemplate transactionTemplate, final JdbcTemplate jdbc) {
        if (transactionTemplate != null && jdbc != null) {
            transactionTemplate.execute(new TransactionCallbackWithoutResult() {
                @Override
                protected void doInTransactionWithoutResult(TransactionStatus status) {
                    jdbc.execute("DROP TABLE account");
                }
            });
        }
    }

    public static long queryForLong(TransactionTemplate transactionTemplate, final JdbcTemplate jdbc, final String query) {
        return transactionTemplate.execute(new TransactionCallback<Long>() {
            @Override
            public Long doInTransaction(TransactionStatus status) {
                return jdbc.queryForObject(query, Long.class);
            }
        });
    }
}
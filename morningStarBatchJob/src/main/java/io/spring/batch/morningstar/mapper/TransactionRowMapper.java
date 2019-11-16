package io.spring.batch.morningstar.mapper;

import io.spring.batch.morningstar.domain.Transaction;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TransactionRowMapper implements RowMapper<Transaction> {
    public static final String ACCOUNT_COLUMN = "account";
    public static final String AMOUNT_COLUMN = "amount";
    public static final String TIMESTAMP_COLUMN = "timeStamp";


    @Override
    public Transaction mapRow(ResultSet resultSet, int i) throws SQLException {
        Transaction transaction = new Transaction();
        transaction.setAccount(resultSet.getString(ACCOUNT_COLUMN));
        transaction.setAmount(resultSet.getBigDecimal(AMOUNT_COLUMN));
        transaction.setTimeStamp(resultSet.getTimestamp(TIMESTAMP_COLUMN));
        return transaction;
    }
}

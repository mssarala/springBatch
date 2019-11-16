package io.spring.batch.morningstar.mapper;

import io.spring.batch.morningstar.domain.Transactionxml;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TransactionXmlMapper implements RowMapper<Transactionxml> {
    public static final String ISIN_COLUMN = "isin";
    public static final String RAWDATA_COLUMN = "rawdata";



    @Override
    public Transactionxml mapRow(ResultSet resultSet, int i) throws SQLException {
        Transactionxml transactionXml = new Transactionxml();
        transactionXml.setIsin(resultSet.getString(ISIN_COLUMN));
        transactionXml.setRawData(resultSet.getString(RAWDATA_COLUMN));
        return transactionXml;
    }
}

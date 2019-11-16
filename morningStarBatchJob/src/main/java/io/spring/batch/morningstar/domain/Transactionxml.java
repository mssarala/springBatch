package io.spring.batch.morningstar.domain;

import org.springframework.stereotype.Repository;

@Repository
public class Transactionxml {

  private String isin;

  private String rawData;

  public String getRawData() {
    return rawData;
  }

  public void setRawData(String rawData) {
    this.rawData = rawData;
  }

  public String getIsin() {
    return isin;
  }

  public void setIsin(String isin) {
    this.isin = isin;
  }




}

package org.example;

import java.util.List;

import org.apache.spark.sql.Row;

public class ReferenceDataInput {

    private List<AnExampleClass1> dataset1;
    private List<AnExampleClass2> dataset2;
    private List<Row> dataset3;

    public List<AnExampleClass1> getDataset1() { return dataset1; }

    public void setDataset1(List<AnExampleClass1> dataset1) { this.dataset1 = dataset1; }

    public List<AnExampleClass2> getDataset2() { return dataset2; }

    public void setDataset2(List<AnExampleClass2> dataset2) { this.dataset2 = dataset2; }

    public List<Row> getDataset3() { return dataset3; }

    public void setDataset3(List<Row> dataset3) { this.dataset3 = dataset3; }

}

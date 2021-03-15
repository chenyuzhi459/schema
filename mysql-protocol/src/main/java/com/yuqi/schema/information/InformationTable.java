package com.yuqi.schema.information;

import com.google.common.collect.ImmutableList;
import com.yuqi.sql.EnhanceSlothColumn;
import com.yuqi.sql.SlothColumn;
import com.yuqi.sql.SlothSchema;
import com.yuqi.sql.SlothTable;
import com.yuqi.storage.constant.FileConstants;
import com.yuqi.storage.lucene.SlothTableEngine;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class InformationTable extends SlothTable {
    private volatile RelDataType rowType;
    private String table;
    private Map<String, SqlTypeName> columns;
    private SlothSchema schema;
    private InformationTableEngine slothTableEngine;

    public InformationTable(String table, Map<String, SqlTypeName> columns) {
        this.table = table;
        this.columns = columns;
    }

    public SlothTableEngine getSlothTableEngine() {
        return slothTableEngine;
    }
    @Override
    public Schema.TableType getJdbcTableType()
    {
        return Schema.TableType.SYSTEM_TABLE;
    }

    @Override
    public Statistic getStatistic()
    {
        return Statistics.UNKNOWN;
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory)
    {
        if(rowType == null){
            synchronized (InformationTable.class){
                if(rowType == null){
                    final RelDataTypeFactory.Builder builder = typeFactory.builder();
                    for(Map.Entry<String, SqlTypeName> col : columns.entrySet()){
                        RelDataType type = typeFactory.createSqlType(col.getValue());
                        builder.add(col.getKey(), type);
                    }
                    rowType = builder.build();
                }
            }
        }
        return rowType;
    }

    @Override
    public boolean isRolledUp(final String column)
    {
        return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(
            final String column,
            final SqlCall call,
            final SqlNode parent,
            final CalciteConnectionConfig config
    )
    {
        return true;
    }

//    @Override
//    public RelNode toRel(final RelOptTable.ToRelContext context, final RelOptTable table)
//    {
//        return LogicalTableScan.create(context.getCluster(), table, ImmutableList.of());
//    }

    public String getTableName() {
        return table;
    }

    public void setTableName(String tableName) {
        throw new UnsupportedOperationException();
    }

    public SlothSchema getSchema() {
        return schema;
    }

    public void setSchema(SlothSchema schema) {
        this.schema = schema;
    }

    public List<SlothColumn> getColumns() {
        return columns.entrySet().stream().map(e -> {
            EnhanceSlothColumn enhanceSlothColumn = new EnhanceSlothColumn();
            enhanceSlothColumn.setColumName(e.getKey());
            enhanceSlothColumn.setColumnType(e.getValue());
            SlothColumn slothColumn = new SlothColumn(e.getKey(), enhanceSlothColumn);
            return slothColumn;
        }).collect(Collectors.toList());
    }

    public String getEngineName() {
//        return engineName;
        throw new UnsupportedOperationException();
    }

    public void setEngineName(String engineName) {
//        this.engineName = engineName;
        throw new UnsupportedOperationException();
    }

    public String getTableComment() {
//        return tableComment;
        throw new UnsupportedOperationException();
    }

    public void setTableComment(String tableComment) {
//        this.tableComment = tableComment;
        throw new UnsupportedOperationException();
    }

    public int getShardNum() {
//        return shardNum;
        throw new UnsupportedOperationException();
    }

    public void setShardNum(int shardNum) {
        throw new UnsupportedOperationException();
    }

    public String buildTableEnginePath() {
        throw new UnsupportedOperationException();
    }


    public void initTableEngine() {
        slothTableEngine = new InformationTableEngine(this);
    }

    @Override
    public String toString() {
        return "InformationTable{" +
                "table='" + table + '\'' +
                ", columns=" + columns +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InformationTable that = (InformationTable) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, columns);
    }
}

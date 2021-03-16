package com.yuqi.schema.information;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.yuqi.engine.SlothRow;
import com.yuqi.engine.data.type.DataType;
import com.yuqi.engine.data.value.Value;
import com.yuqi.protocol.meta.Sloth;
import com.yuqi.sql.SlothColumn;
import com.yuqi.sql.SlothSchema;
import com.yuqi.sql.SlothSchemaHolder;
import com.yuqi.sql.SlothTable;
import com.yuqi.storage.lucene.*;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.NumberUtil;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.NIOFSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static com.yuqi.engine.data.type.DataTypes.*;

/**
 * @author yuqi
 * @mail yuqi4733@gmail.com
 * @description your description
 * @time 13/8/20 16:50
 **/
public class InformationStorageEngine implements StorageEngine {

    public static final Logger LOGGER = LoggerFactory.getLogger(InformationStorageEngine.class);

    private SlothTableEngine slothTableEngine;


    public InformationStorageEngine(SlothTableEngine slothTableEngine) {
        this.slothTableEngine = slothTableEngine;
    }

    @Override
    public void init() {
    }

    @Override
    public boolean insert(List<List<Value>> rows) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<SlothRow> query(QueryContext queryContext) throws IOException {
        String table = slothTableEngine.getSlothTable().getTableName();
//        List<SlothSchema> schemas = SlothSchemaHolder.INSTANCE.getSchemaMap().stream()
//                .filter(slothSchema -> !slothSchema.getSchemaName().equalsIgnoreCase(SlothSchemaHolder.INSTANCE.INFORMATION_SCHEMA_NAME))
//                .collect(Collectors.toList());
        switch (table.toUpperCase()){
            case "COLUMNS":
            return queryForColumns(SlothSchemaHolder.INSTANCE.getSchemaMap());
            case "SCHEMATA"://should include information
                return queryForSchemata(SlothSchemaHolder.INSTANCE.getSchemaMap());
            case "TABLES":
                return queryForTables(SlothSchemaHolder.INSTANCE.getSchemaMap());
            default:

        }
        return new ArrayList<SlothRow>().iterator();

    }

    public Iterator<SlothRow> queryForSchemata(Collection<SlothSchema> schemas){
        Map<String, DataType> columnAndDataType = slothTableEngine.getColumnAndDataType();

        List<SlothRow> rows = new ArrayList<>();
        for(SlothSchema schema : schemas){
            List<Value> row = Lists.newArrayList();
            for(Map.Entry<String, DataType> entry : columnAndDataType.entrySet()) {
                String colName = entry.getKey();
                DataType colType = entry.getValue();
                Value v;
                switch (colName.toUpperCase()){
                    case "SCHEMA_NAME":
                        v = obj2Value(schema.getSchemaName(), colType);
                        break;
                    case "DEFAULT_CHARACTER_SET_NAME":
                        v = obj2Value("utf8", colType);
                        break;
                    case "DEFAULT_COLLATION_NAME":
                        v = obj2Value("utf8_general_ci", colType);
                        break;
                    default:
                        v = obj2Value(null ,colType);
                        break;

                }
                row.add(v);
            }
            rows.add(new SlothRow(row));
        }

        return rows.iterator();

    }

    public Iterator<SlothRow> queryForTables(Collection<SlothSchema> schemas){
        Map<String, DataType> columnAndDataType = slothTableEngine.getColumnAndDataType();

        List<SlothRow> rows = new ArrayList<>();
        for(SlothSchema schema : schemas){
            Collection<Table> tables = schema.getAllTable();
            for(Table t : tables){
                SlothTable slothTable = ((SlothTable)t);
                List<Value> row = Lists.newArrayList();
                for(Map.Entry<String, DataType> entry : columnAndDataType.entrySet()) {
                    String colName = entry.getKey();
                    DataType colType = entry.getValue();
                    Value v;
                    switch (colName.toUpperCase()){
                        case "TABLE_SCHEMA":
                            v = obj2Value(schema.getSchemaName(), colType);
                            break;
                        case "TABLE_NAME":
                            v = obj2Value(slothTable.getTableName(), colType);
                            break;
                        case "TABLE_TYPE":
                            v = obj2Value(slothTable.getJdbcTableType(), colType);
                            break;
                        default:
                            v = obj2Value(null ,colType);
                            break;

                    }
                    row.add(v);
                }
                rows.add(new SlothRow(row));
            }
        }

        return rows.iterator();

    }

    public Iterator<SlothRow> queryForColumns(Collection<SlothSchema> schemas){
        Map<String, DataType> columnAndDataType = slothTableEngine.getColumnAndDataType();

        List<SlothRow> rows = new ArrayList<>();
        for(SlothSchema schema : schemas){
            Collection<Table> tables = schema.getAllTable();
            for(Table t : tables){
                SlothTable slothTable = ((SlothTable)t);
                List<SlothColumn> cols = slothTable.getColumns();
                Long colPosition = 1L;
                for(SlothColumn col : cols){
                    List<Value> row = Lists.newArrayList();
                    for(Map.Entry<String, DataType> entry : columnAndDataType.entrySet()){
                        String colName = entry.getKey();
                        DataType colType = entry.getValue();
                        Value v;
                        switch (colName.toUpperCase()){
                            case "TABLE_SCHEMA":
                                v = obj2Value(schema.getSchemaName(), colType);
                                break;
                            case "TABLE_NAME":
                                v = obj2Value(slothTable.getTableName(), colType);
                                break;
                            case "COLUMN_NAME":
                                v = obj2Value(col.getColumnName(), colType);
                                break;
                            case "COLUMN_TYPE":
                            case "DATA_TYPE":
                                v = obj2Value(col.getColumnType().getColumnType(), colType);
                                break;
                            case "ORDINAL_POSITION":
                                v = obj2Value(colPosition++, colType);
                                break;
                            default:
                                v = obj2Value(null ,colType);
                                break;

                        }
                        row.add(v);
                    }
                    rows.add(new SlothRow(row));
                }
            }
        }

       return rows.iterator();

    }

    private Value obj2Value(Object data, DataType dataType){
        if(data == null){
            return new Value(null, dataType);
        }

        Value val = null;
        if (dataType == STRING) {
            val = new Value(data.toString(), dataType);
        } else if (dataType == LONG){
            val = new Value(Long.valueOf(data.toString()), dataType);
        } else if(dataType == DATE){
            //TODO-cyz not support now;
            return new Value(null, dataType);
        }
        return val;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean readOnly() {
        return true;
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean shouldFlush() {
       return false;
    }
}

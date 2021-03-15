package com.yuqi.schema.information;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yuqi.LifeCycle;
import com.yuqi.engine.SlothRow;
import com.yuqi.engine.data.type.DataType;
import com.yuqi.engine.data.value.Value;
import com.yuqi.sql.SlothTable;
import com.yuqi.sql.util.TypeConversionUtils;
import com.yuqi.storage.lucene.*;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author yuqi
 * @mail yuqi4733@gmail.com
 * @description your description
 * @time 13/8/20 17:47
 **/
public class InformationTableEngine extends SlothTableEngine {

    public static final Logger LOGGER = LoggerFactory.getLogger(InformationTableEngine.class);

    /**
     * Sloth Table instance
     */
    private InformationTable slothTable;

    /**
     * Column name and type, maybe this should change according to variables
     */
    private Map<String, DataType> columnAndDataType = Maps.newLinkedHashMap();


    /**
     * All column name
     */
    private List<String> columnNames = Lists.newArrayList();

    private InformationStorageEngine storageEngine;


    public SlothTable getSlothTable() {
        return slothTable;
    }

    public List<StorageEngine> getStorageEngines() {
        return ImmutableList.of(storageEngine);
    }

    public InformationTableEngine(InformationTable slothTable) {
        this.slothTable = slothTable;
        init();
    }

    public Map<String, DataType> getColumnAndDataType() {
        return columnAndDataType;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void insert(List<List<Value>> values) {
        throw new UnsupportedOperationException();
    }

    public Iterator<SlothRow> search(QueryContext queryContext) {
        List<Iterator<SlothRow>> searchIts = Lists.newArrayList();
        try {
            searchIts.add(storageEngine.query(queryContext));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new SlothMergeIterator<>(searchIts);
    }


    @Override
    public void init() {
        resolveType();
        this.storageEngine = new InformationStorageEngine(this);
//        loadData();
    }

    @Override
    public void close() {
        storageEngine.close();
    }


    public void removeData() {
       throw new UnsupportedOperationException();
    }

    private void resolveType() {
        slothTable.getColumns().forEach(column -> {
            final SqlTypeName sqlTypeName = column.getColumnType().getColumnType();
            columnAndDataType.put(column.getColumnName(),
                    TypeConversionUtils.getBySqlTypeName(sqlTypeName));
            columnNames.add(column.getColumnName());
        });
    }
}

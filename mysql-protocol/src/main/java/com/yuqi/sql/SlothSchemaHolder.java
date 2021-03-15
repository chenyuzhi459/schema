package com.yuqi.sql;

import com.google.common.collect.Maps;
import com.yuqi.LifeCycle;
import com.yuqi.protocol.connection.mysql.SchemaMeta;
import com.yuqi.protocol.connection.mysql.TableMeta;
import com.yuqi.schema.information.InformationSchema;
import com.yuqi.schema.information.InformationTable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author yuqi
 * @mail yuqi4733@gmail.com
 * @description your description
 * @time 10/7/20 20:19
 **/
public class SlothSchemaHolder implements LifeCycle {
    public final String INFORMATION_SCHEMA_NAME = "information_schema";

    private Map<String, SlothSchema> schemaMap;

    public static final SlothSchemaHolder INSTANCE = new SlothSchemaHolder();

    public SlothSchemaHolder() {
        this.schemaMap = Maps.newHashMap();
    }

    @Override
    public void init() {
        Set<String> schemes = SchemaMeta.INSTANCE.allSchema();
        for (String schema : schemes) {
            SlothSchema slothSchema = registerSchema(schema);
            //register table
            List<SlothTable> slothTables = TableMeta.INSTANCE.getAllTableInDb(slothSchema);
            for (SlothTable slothTable : slothTables) {
                slothSchema.restoreFromDb(slothTable);
            }
        }
        registerInformationSchema();
    }

    @Override
    public void close() {

    }

    private void registerInformationSchema(){
        InformationSchema informationSchema = new InformationSchema(INFORMATION_SCHEMA_NAME);
        schemaMap.put(INFORMATION_SCHEMA_NAME, informationSchema);
        CalciteSchema schema =
                ParserFactory.getCatalogReader().getRootSchema().add(INFORMATION_SCHEMA_NAME, informationSchema);
        Map<String, Table> tables = informationSchema.getInnerTables();
        for(Map.Entry<String, Table> entry : tables.entrySet()){
            SlothTable slothTable = (SlothTable) entry.getValue();
            schema.add(entry.getKey(), slothTable);
            slothTable.initTableEngine();

        }
        informationSchema.setSchema(schema);
    }

    public SlothSchema registerSchema(String schemaName) {

        //add db first
        if (SchemaMeta.INSTANCE.schemaIsOk()) {
            SchemaMeta.INSTANCE.addSchema(schemaName);
        }

        //then add in schema
        final SlothSchema slothSchema = new SlothSchema(schemaName);
        schemaMap.put(schemaName, slothSchema);
        CalciteSchema schema =
                ParserFactory.getCatalogReader().getRootSchema().add(schemaName, slothSchema);
        slothSchema.setSchema(schema);
        return slothSchema;
    }

    public boolean removeSchema(String schemaName) {
        SlothSchema schema = SlothSchemaHolder.INSTANCE.getSlothSchema(schemaName);
        schema.dropTableInSchema();

        schemaMap.remove(schemaName);
        //remove data in schema
        ParserFactory.getCatalogReader().getRootSchema()
                .removeSubSchema(schemaName);

        //remove data in db;
        SchemaMeta.INSTANCE.dropSchema(schemaName);
        return true;
    }

    public List<String> getAllSchemas() {
        return new ArrayList<>(schemaMap.keySet());
    }

    public boolean contains(String db) {
        return schemaMap.containsKey(db);
    }

    public SlothSchema getSlothSchema(String dbName) {
        return schemaMap.get(dbName);
    }

    public Collection<SlothSchema> getSchemaMap() {
        return schemaMap.values();
    }
}

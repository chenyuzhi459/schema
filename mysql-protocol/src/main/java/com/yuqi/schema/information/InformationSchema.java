package com.yuqi.schema.information;

import com.google.common.collect.ImmutableMap;
import com.yuqi.sql.SlothSchema;
import com.yuqi.sql.SlothTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class InformationSchema  extends SlothSchema {

    // Now we just mock tables, table_privileges, referential_constraints, key_column_usage and routines table
    // Because in MySQL ODBC, these tables are used.
    // TODO(zhaochun): Review some commercial BI to check if we need support where clause in show statement
    // like 'show table where_clause'. If we decide to support it, we must mock these related table here.
    public static Map<String, Table> TABLE_MAP =
            ImmutableMap
                    .<String, Table>builder()
                    .put("tables", new InformationTable(
//                            //SystemIdGenerator.getNextId(),
                            "tables",
//                            //TableType.SCHEMA
                            ImmutableMap.<String, SqlTypeName>builder()
                                    .put("TABLE_CATALOG", SqlTypeName.VARCHAR)
                                    .put("TABLE_SCHEMA", SqlTypeName.VARCHAR)
                                    .put("TABLE_NAME", SqlTypeName.VARCHAR)
                                    .put("TABLE_TYPE", SqlTypeName.VARCHAR)
                                    .put("ENGINE", SqlTypeName.VARCHAR)
                                    .put("VERSION", SqlTypeName.BIGINT)
                                    .put("ROW_FORMAT", SqlTypeName.VARCHAR)
                                    .put("TABLE_ROWS", SqlTypeName.BIGINT)
                                    .put("AVG_ROW_LENGTH", SqlTypeName.BIGINT)
                                    .put("DATA_LENGTH", SqlTypeName.BIGINT)
                                    .put("MAX_DATA_LENGTH", SqlTypeName.BIGINT)
                                    .put("INDEX_LENGTH", SqlTypeName.BIGINT)
                                    .put("DATA_FREE", SqlTypeName.BIGINT)
                                    .put("AUTO_INCREMENT", SqlTypeName.BIGINT)
                                    .put("CREATE_TIME", SqlTypeName.DATE)
                                    .put("UPDATE_TIME", SqlTypeName.DATE)
                                    .put("CHECK_TIME", SqlTypeName.DATE)
                                    .put("TABLE_COLLATION", SqlTypeName.VARCHAR)
                                    .put("CHECKSUM", SqlTypeName.BIGINT)
                                    .put("CREATE_OPTIONS", SqlTypeName.VARCHAR)
                                    .put("TABLE_COMMENT", SqlTypeName.VARCHAR)
                                    .build()))
                    .put("table_privileges", new InformationTable(
//                            //SystemIdGenerator.getNextId(),
                            "table_privileges",
//                            //TableType.SCHEMA
                            ImmutableMap.<String, SqlTypeName>builder()
                                    .put("GRANTEE", SqlTypeName.VARCHAR)
                                    .put("TABLE_CATALOG", SqlTypeName.VARCHAR)
                                    .put("TABLE_SCHEMA", SqlTypeName.VARCHAR)
                                    .put("TABLE_NAME", SqlTypeName.VARCHAR)
                                    .put("PRIVILEGE_TYPE", SqlTypeName.VARCHAR)
                                    .put("IS_GRANTABLE", SqlTypeName.VARCHAR)
                                    .build()))
                    .put("schema_privileges", new InformationTable(
                            //SystemIdGenerator.getNextId(),
                            "schema_privileges",
                            //TableType.SCHEMA
                            ImmutableMap.<String, SqlTypeName>builder()
                                    .put("GRANTEE", SqlTypeName.VARCHAR)
                                    .put("TABLE_CATALOG", SqlTypeName.VARCHAR)
                                    .put("TABLE_SCHEMA", SqlTypeName.VARCHAR)
                                    .put("PRIVILEGE_TYPE", SqlTypeName.VARCHAR)
                                    .put("IS_GRANTABLE", SqlTypeName.VARCHAR)
                                    .build()))
                    .put("user_privileges", new InformationTable(
                            //SystemIdGenerator.getNextId(),
                            "user_privileges",
                            //TableType.SCHEMA
                            ImmutableMap.<String, SqlTypeName>builder()
                                    .put("GRANTEE", SqlTypeName.VARCHAR)
                                    .put("TABLE_CATALOG", SqlTypeName.VARCHAR)
                                    .put("PRIVILEGE_TYPE", SqlTypeName.VARCHAR)
                                    .put("IS_GRANTABLE", SqlTypeName.VARCHAR)
                                    .build()))
                    .put("referential_constraints", new InformationTable(
                            //SystemIdGenerator.getNextId(),
                            "referential_constraints",
                            //TableType.SCHEMA
                            ImmutableMap.<String, SqlTypeName>builder()
                                    .put("CONSTRAINT_CATALOG", SqlTypeName.VARCHAR)
                                    .put("CONSTRAINT_SCHEMA", SqlTypeName.VARCHAR)
                                    .put("CONSTRAINT_NAME", SqlTypeName.VARCHAR)
                                    .put("UNIQUE_CONSTRAINT_CATALOG", SqlTypeName.VARCHAR)
                                    .put("UNIQUE_CONSTRAINT_SCHEMA", SqlTypeName.VARCHAR)
                                    .put("UNIQUE_CONSTRAINT_NAME", SqlTypeName.VARCHAR)
                                    .put("MATCH_OPTION", SqlTypeName.VARCHAR)
                                    .put("UPDATE_RULE", SqlTypeName.VARCHAR)
                                    .put("DELETE_RULE", SqlTypeName.VARCHAR)
                                    .put("TABLE_NAME", SqlTypeName.VARCHAR)
                                    .put("REFERENCED_TABLE_NAME", SqlTypeName.VARCHAR)
                                    .build()))
                    .put("key_column_usage", new InformationTable(
                            //SystemIdGenerator.getNextId(),
                            "key_column_usage",
                            //TableType.SCHEMA
                            ImmutableMap.<String, SqlTypeName>builder()
                                    .put("CONSTRAINT_CATALOG", SqlTypeName.VARCHAR)
                                    .put("CONSTRAINT_SCHEMA", SqlTypeName.VARCHAR)
                                    .put("CONSTRAINT_NAME", SqlTypeName.VARCHAR)
                                    .put("TABLE_CATALOG", SqlTypeName.VARCHAR)
                                    .put("TABLE_SCHEMA", SqlTypeName.VARCHAR)
                                    .put("TABLE_NAME", SqlTypeName.VARCHAR)
                                    .put("COLUMN_NAME", SqlTypeName.VARCHAR)
                                    .put("ORDINAL_POSITION", SqlTypeName.BIGINT)
                                    .put("POSITION_IN_UNIQUE_CONSTRAINT",
                                            SqlTypeName.BIGINT)
                                    .put("REFERENCED_TABLE_SCHEMA", SqlTypeName.VARCHAR)
                                    .put("REFERENCED_TABLE_NAME", SqlTypeName.VARCHAR)
                                    .put("REFERENCED_COLUMN_NAME", SqlTypeName.VARCHAR)
                                    .build()))
                    .put("routines", new InformationTable(
                            //SystemIdGenerator.getNextId(),
                            "routines",
                            //TableType.SCHEMA
                            ImmutableMap.<String, SqlTypeName>builder()
                                    .put("SPECIFIC_NAME", SqlTypeName.VARCHAR)
                                    .put("ROUTINE_CATALOG", SqlTypeName.VARCHAR)
                                    .put("ROUTINE_SCHEMA", SqlTypeName.VARCHAR)
                                    .put("ROUTINE_NAME", SqlTypeName.VARCHAR)
                                    .put("ROUTINE_TYPE", SqlTypeName.VARCHAR)
                                    .put("DTD_IDENTIFIER", SqlTypeName.VARCHAR)
                                    .put("ROUTINE_BODY", SqlTypeName.VARCHAR)
                                    .put("ROUTINE_DEFINITION", SqlTypeName.VARCHAR)
                                    .put("EXTERNAL_NAME", SqlTypeName.VARCHAR)
                                    .put("EXTERNAL_LANGUAGE", SqlTypeName.VARCHAR)
                                    .put("PARAMETER_STYLE", SqlTypeName.VARCHAR)
                                    .put("IS_DETERMINISTIC", SqlTypeName.VARCHAR)
                                    .put("SQL_DATA_ACCESS", SqlTypeName.VARCHAR)
                                    .put("SQL_PATH", SqlTypeName.VARCHAR)
                                    .put("SECURITY_TYPE", SqlTypeName.VARCHAR)
                                    .put("CREATED", SqlTypeName.DATE)
                                    .put("LAST_ALTERED", SqlTypeName.DATE)
                                    .put("SQL_MODE", SqlTypeName.VARCHAR)
                                    .put("ROUTINE_COMMENT", SqlTypeName.VARCHAR)
                                    .put("DEFINER", SqlTypeName.VARCHAR)
                                    .put("CHARACTER_SET_CLIENT", SqlTypeName.VARCHAR)
                                    .put("COLLATION_CONNECTION", SqlTypeName.VARCHAR)
                                    .put("DATABASE_COLLATION", SqlTypeName.VARCHAR)
                                    .build()))
                    .put("schemata", new InformationTable(
                            //SystemIdGenerator.getNextId(),
                            "schemata",
                            //TableType.SCHEMA
                            ImmutableMap.<String, SqlTypeName>builder()
                                    .put("CATALOG_NAME", SqlTypeName.VARCHAR)
                                    .put("SCHEMA_NAME", SqlTypeName.VARCHAR)
                                    .put("DEFAULT_CHARACTER_SET_NAME", SqlTypeName.VARCHAR)
                                    .put("DEFAULT_COLLATION_NAME", SqlTypeName.VARCHAR)
                                    .put("SQL_PATH", SqlTypeName.VARCHAR)
                                    .build()))
                    .put("session_variables", new InformationTable(
                            //SystemIdGenerator.getNextId(),
                            "session_variables",
                            //TableType.SCHEMA
                            ImmutableMap.<String, SqlTypeName>builder()
                                    .put("VARIABLE_NAME", SqlTypeName.VARCHAR)
                                    .put("VARIABLE_VALUE", SqlTypeName.VARCHAR)
                                    .build()))
                    .put("global_variables", new InformationTable(
                            //SystemIdGenerator.getNextId(),
                            "global_variables",
                            //TableType.SCHEMA
                            ImmutableMap.<String, SqlTypeName>builder()
                                    .put("VARIABLE_NAME", SqlTypeName.VARCHAR)
                                    .put("VARIABLE_VALUE", SqlTypeName.VARCHAR)
                                    .build()))
                    .put("columns", new InformationTable(
                            //SystemIdGenerator.getNextId(),
                            "columns",
                            //TableType.SCHEMA
                            ImmutableMap.<String, SqlTypeName>builder()
                                    .put("TABLE_CATALOG", SqlTypeName.VARCHAR)
                                    .put("TABLE_SCHEMA", SqlTypeName.VARCHAR)
                                    .put("TABLE_NAME", SqlTypeName.VARCHAR)
                                    .put("COLUMN_NAME", SqlTypeName.VARCHAR)
                                    .put("ORDINAL_POSITION", SqlTypeName.BIGINT)
                                    .put("COLUMN_DEFAULT", SqlTypeName.VARCHAR)
                                    .put("IS_NULLABLE", SqlTypeName.VARCHAR)
                                    .put("DATA_TYPE", SqlTypeName.VARCHAR)
                                    .put("CHARACTER_MAXIMUM_LENGTH",
                                            SqlTypeName.BIGINT)
                                    .put("CHARACTER_OCTET_LENGTH",
                                            SqlTypeName.BIGINT)
                                    .put("NUMERIC_PRECISION", SqlTypeName.BIGINT)
                                    .put("NUMERIC_SCALE", SqlTypeName.BIGINT)
                                    .put("DATETIME_PRECISION", SqlTypeName.BIGINT)
                                    .put("CHARACTER_SET_NAME", SqlTypeName.VARCHAR)
                                    .put("COLLATION_NAME", SqlTypeName.VARCHAR)
                                    .put("COLUMN_TYPE", SqlTypeName.VARCHAR)
                                    .put("COLUMN_KEY", SqlTypeName.VARCHAR)
                                    .put("EXTRA", SqlTypeName.VARCHAR)
                                    .put("PRIVILEGES", SqlTypeName.VARCHAR)
                                    .put("COLUMN_COMMENT", SqlTypeName.VARCHAR)
                                    .put("COLUMN_SIZE", SqlTypeName.BIGINT)
                                    .put("DECIMAL_DIGITS", SqlTypeName.BIGINT)
                                    .put("GENERATION_EXPRESSION", SqlTypeName.VARCHAR)
                                    .put("SRS_ID", SqlTypeName.BIGINT)
                                    .build()))
                    .put("character_sets", new InformationTable(
                            //SystemIdGenerator.getNextId(),
                            "character_sets",
                            //TableType.SCHEMA
                            ImmutableMap.<String, SqlTypeName>builder()
                                    .put("CHARACTER_SET_NAME", SqlTypeName.VARCHAR)
                                    .put("DEFAULT_COLLATE_NAME", SqlTypeName.VARCHAR)
                                    .put("DESCRIPTION", SqlTypeName.VARCHAR)
                                    .put("MAXLEN", SqlTypeName.BIGINT)
                                    .build()))
                    .put("collations", new InformationTable(
                            //SystemIdGenerator.getNextId(),
                            "collations",
                            //TableType.SCHEMA
                            ImmutableMap.<String, SqlTypeName>builder()
                                    .put("COLLATION_NAME", SqlTypeName.VARCHAR)
                                    .put("CHARACTER_SET_NAME", SqlTypeName.VARCHAR)
                                    .put("ID", SqlTypeName.BIGINT)
                                    .put("IS_DEFAULT", SqlTypeName.VARCHAR)
                                    .put("IS_COMPILED", SqlTypeName.VARCHAR)
                                    .put("SORTLEN", SqlTypeName.BIGINT)
                                    .build()))
                    .put("table_constraints", new InformationTable(
                            //SystemIdGenerator.getNextId(),
                            "table_constraints",
                            //TableType.SCHEMA
                            ImmutableMap.<String, SqlTypeName>builder()
                                    .put("CONSTRAINT_CATALOG", SqlTypeName.VARCHAR)
                                    .put("CONSTRAINT_SCHEMA", SqlTypeName.VARCHAR)
                                    .put("CONSTRAINT_NAME", SqlTypeName.VARCHAR)
                                    .put("TABLE_SCHEMA", SqlTypeName.VARCHAR)
                                    .put("TABLE_NAME", SqlTypeName.VARCHAR)
                                    .put("CONSTRAINT_TYPE", SqlTypeName.VARCHAR)
                                    .build()))
                    .put("engines",
                            new InformationTable(
                                    //SystemIdGenerator.getNextId(),
                                    "engines",
                                    //TableType.SCHEMA
                                    ImmutableMap.<String, SqlTypeName>builder()
                                            .put("ENGINE", SqlTypeName.VARCHAR)
                                            .put("SUPPORT", SqlTypeName.VARCHAR)
                                            .put("COMMENT", SqlTypeName.VARCHAR)
                                            .put("TRANSACTIONS", SqlTypeName.VARCHAR)
                                            .put("XA", SqlTypeName.VARCHAR)
                                            .put("SAVEPOINTS", SqlTypeName.VARCHAR)
                                            .build()))
                    .put("views",
                            new InformationTable(
                                    //SystemIdGenerator.getNextId(),
                                    "views",
                                    //TableType.SCHEMA
                                    ImmutableMap.<String, SqlTypeName>builder()
                                            .put("TABLE_CATALOG", SqlTypeName.VARCHAR)
                                            .put("TABLE_SCHEMA", SqlTypeName.VARCHAR)
                                            .put("TABLE_NAME", SqlTypeName.VARCHAR)
                                            .put("VIEW_DEFINITION", SqlTypeName.VARCHAR)
                                            .put("CHECK_OPTION", SqlTypeName.VARCHAR)
                                            .put("IS_UPDATABLE", SqlTypeName.VARCHAR)
                                            .put("DEFINER", SqlTypeName.VARCHAR)
                                            .put("SECURITY_TYPE", SqlTypeName.VARCHAR)
                                            .put("CHARACTER_SET_CLIENT", SqlTypeName.VARCHAR)
                                            .put("COLLATION_CONNECTION", SqlTypeName.VARCHAR)
                                            .build()))
                    .put("statistics",
                            new InformationTable(
                                    //SystemIdGenerator.getNextId(),
                                    "statistics",
                                    //TableType.SCHEMA
                                    ImmutableMap.<String, SqlTypeName>builder()
                                            .put("TABLE_CATALOG", SqlTypeName.VARCHAR)
                                            .put("TABLE_SCHEMA", SqlTypeName.VARCHAR)
                                            .put("TABLE_NAME", SqlTypeName.VARCHAR)
                                            .put("NON_UNIQUE", SqlTypeName.BIGINT)
                                            .put("INDEX_SCHEMA", SqlTypeName.VARCHAR)
                                            .put("INDEX_NAME", SqlTypeName.VARCHAR)
                                            .put("SEQ_IN_INDEX", SqlTypeName.BIGINT)
                                            .put("COLUMN_NAME", SqlTypeName.VARCHAR)
                                            .put("COLLATION", SqlTypeName.VARCHAR)
                                            .put("CARDINALITY", SqlTypeName.BIGINT)
                                            .put("SUB_PART", SqlTypeName.BIGINT)
                                            .put("PACKED", SqlTypeName.VARCHAR)
                                            .put("NULLABLE", SqlTypeName.VARCHAR)
                                            .put("INDEX_TYPE", SqlTypeName.VARCHAR)
                                            .put("COMMENT", SqlTypeName.VARCHAR)
                                            .build()))
                    .build();

    public InformationSchema(String schemaName) {
        super(schemaName);
    }
    //  statistics is table provides information about table indexes in mysql: 5.7
    // views column is from show create table views in mysql: 5.5.6


    @Override
    protected Map<String, Table> getTableMap() {
        return TABLE_MAP;
    }


    public boolean dropTable(String tableName) {
       throw new UnsupportedOperationException();
    }

    public boolean addTable(String tableName, SlothTable slothTable) {
        throw new UnsupportedOperationException();
    }

    public void restoreFromDb(SlothTable slothTable) {
        throw new UnsupportedOperationException();
    }

    public void dropTableInSchema() {
        throw new UnsupportedOperationException();
    }

    public Collection<Table> getAllTable() {
        return TABLE_MAP.values();
    }

    public Map<String, Table> getInnerTables() {
        return TABLE_MAP;
    }

    public boolean containsTable(String tableName) {
        return TABLE_MAP.containsKey(tableName);
    }

    public List<String> getTables() {
        return new ArrayList<>(TABLE_MAP.keySet());
    }
}

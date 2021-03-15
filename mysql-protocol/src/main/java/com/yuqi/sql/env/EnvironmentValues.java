package com.yuqi.sql.env;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author yuqi
 * @mail yuqi4733@gmail.com
 * @description your description
 * @time 6/9/20 15:52
 **/
public class EnvironmentValues {
    //TODO-cyz 区分Global和Session级别的配置, 支持select @@global.xx的查询
    public static final Map<String, Object> GLOBAL_ENVIRONMENT = Maps.newHashMap();

    public static final String EXEC_MEM_LIMIT = "exec_mem_limit";
    public static final String QUERY_TIMEOUT = "query_timeout";
    public static final String IS_REPORT_SUCCESS = "is_report_success";
    public static final String SQL_MODE = "sql_mode";
    public static final String RESOURCE_VARIABLE = "resource_group";
    public static final String AUTO_COMMIT = "autocommit";
    public static final String TX_ISOLATION = "tx_isolation";
    public static final String CHARACTER_SET_CLIENT = "character_set_client";
    public static final String CHARACTER_SET_CONNNECTION = "character_set_connection";
    public static final String CHARACTER_SET_RESULTS = "character_set_results";
    public static final String CHARACTER_SET_SERVER = "character_set_server";
    public static final String COLLATION_CONNECTION = "collation_connection";
    public static final String COLLATION_DATABASE = "collation_database";
    public static final String COLLATION_SERVER = "collation_server";
    public static final String LICENSE = "license";
    public static final String LOWER_CASE_TABLE_NAMES = "lower_case_table_names";
    public static final String SQL_AUTO_IS_NULL = "SQL_AUTO_IS_NULL";
    public static final String SQL_SELECT_LIMIT = "sql_select_limit";
    public static final String MAX_ALLOWED_PACKET = "max_allowed_packet";
    public static final String AUTO_INCREMENT_INCREMENT = "auto_increment_increment";
    public static final String QUERY_CACHE_TYPE = "query_cache_type";
    public static final String INTERACTIVE_TIMTOUT = "interactive_timeout";
    public static final String WAIT_TIMEOUT = "wait_timeout";
    public static final String NET_WRITE_TIMEOUT = "net_write_timeout";
    public static final String NET_READ_TIMEOUT = "net_read_timeout";
    public static final String TIME_ZONE = "time_zone";
    public static final String SQL_SAFE_UPDATES = "sql_safe_updates";
    public static final String NET_BUFFER_LENGTH = "net_buffer_length";
    public static final String CODEGEN_LEVEL = "codegen_level";
    // mem limit can't smaller than bufferpool's default page size
    public static final int MIN_EXEC_MEM_LIMIT = 2097152;
    public static final String BATCH_SIZE = "batch_size";
    public static final String DISABLE_STREAMING_PREAGGREGATIONS = "disable_streaming_preaggregations";
    public static final String DISABLE_COLOCATE_JOIN = "disable_colocate_join";
    public static final String ENABLE_BUCKET_SHUFFLE_JOIN = "enable_bucket_shuffle_join";
    public static final String PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM = "parallel_fragment_exec_instance_num";
    public static final String ENABLE_INSERT_STRICT = "enable_insert_strict";
    public static final String ENABLE_SPILLING = "enable_spilling";
    public static final String PREFER_JOIN_METHOD = "prefer_join_method";

    public static final String ENABLE_ODBC_TRANSCATION = "enable_odbc_transcation";
    public static final String ENABLE_SQL_CACHE = "enable_sql_cache";
    public static final String ENABLE_PARTITION_CACHE = "enable_partition_cache";

    public static final int MIN_EXEC_INSTANCE_NUM = 1;
    public static final int MAX_EXEC_INSTANCE_NUM = 32;
    // if set to true, some of stmt will be forwarded to master FE to get result
    public static final String FORWARD_TO_MASTER = "forward_to_master";
    // user can set instance num after exchange, no need to be equal to nums of before exchange
    public static final String PARALLEL_EXCHANGE_INSTANCE_NUM = "parallel_exchange_instance_num";
    public static final String SHOW_HIDDEN_COLUMNS = "show_hidden_columns";
    /*
     * configure the mem limit of load process on BE.
     * Previously users used exec_mem_limit to set memory limits.
     * To maintain compatibility, the default value of load_mem_limit is 0,
     * which means that the load memory limit is still using exec_mem_limit.
     * Users can set a value greater than zero to explicitly specify the load memory limit.
     * This variable is mainly for INSERT operation, because INSERT operation has both query and load part.
     * Using only the exec_mem_limit variable does not make a good distinction of memory limit between the two parts.
     */
    public static final String LOAD_MEM_LIMIT = "load_mem_limit";
    public static final String USE_V2_ROLLUP = "use_v2_rollup";
    public static final String TEST_MATERIALIZED_VIEW = "test_materialized_view";
    public static final String REWRITE_COUNT_DISTINCT_TO_BITMAP_HLL = "rewrite_count_distinct_to_bitmap_hll";
    public static final String EVENT_SCHEDULER = "event_scheduler";
    public static final String STORAGE_ENGINE = "storage_engine";
    public static final String DIV_PRECISION_INCREMENT = "div_precision_increment";

    // see comment of `doris_max_scan_key_num` and `max_pushdown_conditions_per_column` in BE config
    public static final String MAX_SCAN_KEY_NUM = "max_scan_key_num";
    public static final String MAX_PUSHDOWN_CONDITIONS_PER_COLUMN = "max_pushdown_conditions_per_column";

    // when true, the partition column must be set to NOT NULL.
    public static final String ALLOW_PARTITION_COLUMN_NULLABLE = "allow_partition_column_nullable";

    // max ms to wait transaction publish finish when exec insert stmt.
    public static final String INSERT_VISIBLE_TIMEOUT_MS = "insert_visible_timeout_ms";

    public static final String DELETE_WITHOUT_PARTITION = "delete_without_partition";


    public static final long DEFAULT_INSERT_VISIBLE_TIMEOUT_MS = 10_000;
    public static final long MIN_INSERT_VISIBLE_TIMEOUT_MS = 1000; // If user set a very small value, use this value instead.

    public static final String INIT_CONNECT = "init_connect";
    public static final String QUERY_CACHE_SIZE = "query_cache_size";
    public static final String SYSTEM_TIME_ZONE = "system_time_zone";
    public static final String TRANSACTION_ISOLATION = "transaction_isolation";
    public static final String VERSION_COMMENT = "version_comment";


    public static String versionComment = "druid";

    public static String transactionIsolation = "repeatable-read";

    // A string to be executed by the server for each client that connects
    public static String systemTimeZone = "UTC";

    // The amount of memory allocated for caching query results
    private static volatile long queryCacheSize = 1048576;

    // A string to be executed by the server for each client that connects
    private static volatile String initConnect = "";

    // 0: table names are stored as specified and comparisons are case sensitive.
    // 1: table names are stored in lowercase on disk and comparisons are not case sensitive.
    // 2: table names are stored as given but compared in lowercase.
    public static int lowerCaseTableNames = 0;

    public static String license = "Apache License, Version 2.0";

    // only
    public static int netBufferLength = 16384;

    public static long insertVisibleTimeoutMs = DEFAULT_INSERT_VISIBLE_TIMEOUT_MS;

    // max memory used on every backend.
    public static long maxExecMemByte = 2147483648L;

    public static boolean enableSpilling = false;

    // query timeout in second.
    public static int queryTimeoutS = 300;

    // if true, need report to coordinator when plan fragment execute successfully.
    public static boolean isReportSucc = false;

    // Set sqlMode to empty string
    public static long sqlMode = 0L;

    public static String resourceGroup = "normal";

    // this is used to make mysql client happy
    public static boolean autoCommit = true;

    // this is used to make c3p0 library happy
    public static String txIsolation = "REPEATABLE-READ";

    // this is used to make c3p0 library happy
    public static String charsetClient = "utf8";
    public static String charsetConnection = "utf8";
    public static String charsetResults = "utf8";
    public static String charsetServer = "utf8";
    public static String collationConnection = "utf8_general_ci";
    public static String collationDatabase = "utf8_general_ci";

    public static String collationServer = "utf8_general_ci";

    // this is used to make c3p0 library happy
    public static boolean sqlAutoIsNull = false;

    public static long sqlSelectLimit = 9223372036854775807L;

    // this is used to make c3p0 library happy
    public static int maxAllowedPacket = 1048576;

    public static int autoIncrementIncrement = 1;

    // this is used to make c3p0 library happy
    public static int queryCacheType = 0;

    // The number of seconds the server waits for activity on an interactive connection before closing it
    public static int interactiveTimeout = 3600;

    // The number of seconds the server waits for activity on a noninteractive connection before closing it.
    public static int waitTimeout = 28800;

    // The number of seconds to wait for a block to be written to a connection before aborting the write
    public static int netWriteTimeout = 60;

    // The number of seconds to wait for a block to be written to a connection before aborting the write
    public static int netReadTimeout = 60;

    // The current time zone
    public static String timeZone = "UTC";

    public static int exchangeInstanceParallel = -1;

    public static int sqlSafeUpdates = 0;

    // if true, need report to coordinator when plan fragment execute successfully.
    public static int codegenLevel = 0;

    public static int batchSize = 1024;

    public static boolean disableStreamPreaggregations = false;

    public static boolean disableColocateJoin = false;

    public static boolean enableBucketShuffleJoin = true;

    public static String preferJoinMethod = "broadcast";

    /*
     * the parallel exec instance num for one Fragment in one BE
     * 1 means disable this feature
     */
    public static int parallelExecInstanceNum = 1;

    public static boolean enableInsertStrict = false;

    public static boolean enableOdbcTransaction = false;

    public static boolean enableSqlCache = false;

    public static boolean enablePartitionCache = false;

    public static boolean forwardToMaster = false;

    public static long loadMemLimit = 0L;

    public static boolean useV2Rollup = false;

    // TODO(ml): remove it after test
    public static boolean testMaterializedView = false;

    public static boolean rewriteCountDistinct = true;

    // compatible with some mysql client connect, say DataGrip of JetBrains
    public static String eventScheduler = "OFF";
    public static String storageEngine = "olap";
    public static int divPrecisionIncrement = 4;

    // -1 means unset, BE will use its config value
    public static int maxScanKeyNum = -1;
    public static int maxPushdownConditionsPerColumn = -1;
    public static boolean showHiddenColumns = false;

    public static boolean allowPartitionColumnNullable = true;

    public static boolean deleteWithoutPartition = false;

    static {
        GLOBAL_ENVIRONMENT.put(VERSION_COMMENT, versionComment);
        GLOBAL_ENVIRONMENT.put(AUTO_INCREMENT_INCREMENT, autoIncrementIncrement);
        GLOBAL_ENVIRONMENT.put(CHARACTER_SET_CLIENT, charsetClient);
        GLOBAL_ENVIRONMENT.put(CHARACTER_SET_CONNNECTION, charsetConnection);
        GLOBAL_ENVIRONMENT.put(CHARACTER_SET_RESULTS, charsetResults);
        GLOBAL_ENVIRONMENT.put(CHARACTER_SET_SERVER, charsetServer);
        GLOBAL_ENVIRONMENT.put(COLLATION_SERVER, collationServer);
        GLOBAL_ENVIRONMENT.put(INIT_CONNECT, initConnect);
        GLOBAL_ENVIRONMENT.put(INTERACTIVE_TIMTOUT, interactiveTimeout);
        GLOBAL_ENVIRONMENT.put(LICENSE, license);
        GLOBAL_ENVIRONMENT.put(LOWER_CASE_TABLE_NAMES, lowerCaseTableNames);
        GLOBAL_ENVIRONMENT.put(MAX_ALLOWED_PACKET, maxAllowedPacket);
        GLOBAL_ENVIRONMENT.put(NET_WRITE_TIMEOUT, netWriteTimeout);
        GLOBAL_ENVIRONMENT.put(QUERY_CACHE_SIZE, queryCacheSize);
        GLOBAL_ENVIRONMENT.put(QUERY_CACHE_TYPE, queryCacheType);
        GLOBAL_ENVIRONMENT.put(SQL_MODE, sqlMode);
        GLOBAL_ENVIRONMENT.put(SYSTEM_TIME_ZONE, systemTimeZone);
        GLOBAL_ENVIRONMENT.put(TIME_ZONE, timeZone);
        GLOBAL_ENVIRONMENT.put(TRANSACTION_ISOLATION, transactionIsolation);
        GLOBAL_ENVIRONMENT.put(TX_ISOLATION, transactionIsolation);


        GLOBAL_ENVIRONMENT.put(WAIT_TIMEOUT, waitTimeout);
        GLOBAL_ENVIRONMENT.put(NET_BUFFER_LENGTH, netBufferLength);

    }
}

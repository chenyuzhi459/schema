package com.yuqi.protocol.enums;

/**
 * @author yuqi
 * @mail yuqi4733@gmail.com
 * @description your description
 * @time 13/7/20 23:41
 **/
public enum ShowEnum {

    /**
     * show tables;
     */
    SHOW_TABLES(0, "tables"),


    /**
     * show databases;
     */
    SHOW_DBS(1, "databases"),

    /**
     * show create table
     */
    SHOW_CREATE(2, "create"),

    /**
     * show variables
     */
    SHOW_VARIABLES(3, "variables"),

    /**
     * show columns
     */
    SHOW_COLUMNS(4, "columns"),

    /**
     * show table status;
     */
    SHOW_TABLES_STATUS(5, "table status"),

    /**
     * show engines;
     */
    SHOW_ENGINES(6, "ENGINES"),

    /**
     * show charset;
     */
    SHOW_CHARSET(7, "CHARSET"),

    /**
     * show collation;
     */
    SHOW_COLLATION(8, "COLLATION"),

    /**
     * show warnings;
     */
    SHOW_WARNINGS(9, "WARNINGS");


    private final int index;
    private final String startKeyWord;

    ShowEnum(int index, String startKeyWord) {
        this.index = index;
        this.startKeyWord = startKeyWord;
    }
}

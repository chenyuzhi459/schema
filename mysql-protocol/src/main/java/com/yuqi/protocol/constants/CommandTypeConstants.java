package com.yuqi.protocol.constants;

/**
 * @author yuqi
 * @mail yuqi4733@gmail.com
 * @description your description
 * @time 4/7/20 20:38
 **/
public class CommandTypeConstants {
    public static final byte COM_QUIT = 0x01;

    public static final byte COM_USE_DB = 0x02;

    public static final byte COM_QUERY = 0x03;

    public static final byte COM_FIELD_LIST = 0x04;

    public static final byte COM_CREATE_DB = 0x05;

    public static final byte COM_DROP_DB = 0x06;

    public static final byte COM_PING = 0x0e;  //14
}

package org.apache.kylin.storage.hbase.util;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;

import java.io.IOException;

/**
 * Created by Lenovo on 2017/4/10.
 */
public class Test {

    public static void main(String[] args) throws IOException {

        String s = "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x08\\x00\\x1C\\x00\\x04\\x04\\x41";
        byte[] startKey = BytesUtil.fromReadableText(s);//可读的16进制内容,输入参数是16进制的数据
        System.out.println(startKey.length);
        for(byte b:startKey){
            System.out.println(b);
        }
        String x = Bytes.toStringBinary(startKey);
        System.out.println(x);


    }

}

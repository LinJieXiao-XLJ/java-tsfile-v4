package org.apache.iotdb;

import org.apache.iotdb.table.TableTsFileV4_NoSameDevice_NoSameTimeStamp;
import org.apache.iotdb.table.TableTsFileV4_NoSameDevice_SameTimeStamp;
import org.apache.iotdb.table.TableTsFileV4_SameDevice_NoSameTimeStamp;
import org.apache.iotdb.table.TableTsFileV4_SameDevice_SameTimeStamp;
import org.apache.iotdb.tree.TreeTsFileV4_NoSameDevice_NoSameTimeStamp;
import org.apache.iotdb.tree.TreeTsFileV4_NoSameDevice_SameTimeStamp;
import org.apache.iotdb.tree.TreeTsFileV4_SameDevice_NoSameTimeStamp;
import org.apache.iotdb.tree.TreeTsFileV4_SameDevice_SameTimeStamp;
import org.apache.iotdb.utils.ReadConfig;

public class Main {

    // 实例化配置
    private static final ReadConfig config = ReadConfig.getInstance();

    public static void main(String[] args) {
        // 判断模式
        if (config.getConfigValue("MODE").equals("tree")) { // 树模型
            TreeTsFileV4_NoSameDevice_NoSameTimeStamp t1 = new TreeTsFileV4_NoSameDevice_NoSameTimeStamp();
            t1.testWrite();
            t1.testReader();

//            TreeTsFileV4_NoSameDevice_SameTimeStamp t2 = new TreeTsFileV4_NoSameDevice_SameTimeStamp();
//            t2.testWrite();
//            t2.testReader();
//
//            TreeTsFileV4_SameDevice_SameTimeStamp t3 = new TreeTsFileV4_SameDevice_SameTimeStamp();
//            t3.testWrite();
//            t3.testReader();
//
//            TreeTsFileV4_SameDevice_NoSameTimeStamp t4 = new TreeTsFileV4_SameDevice_NoSameTimeStamp();
//            t4.testWrite();
//            t4.testReader();
        } else if (config.getConfigValue("MODE").equals("table")) { // 表模型
            for (int times = 1; times <= Integer.parseInt(config.getConfigValue("LOOP")); times++) {
                TableTsFileV4_NoSameDevice_NoSameTimeStamp t1 = new TableTsFileV4_NoSameDevice_NoSameTimeStamp();
                t1.testWrite(times);
                t1.testReader();

                TableTsFileV4_NoSameDevice_SameTimeStamp t2 = new TableTsFileV4_NoSameDevice_SameTimeStamp();
                t2.testWrite(times);
                t2.testReader();

                TableTsFileV4_SameDevice_SameTimeStamp t3 = new TableTsFileV4_SameDevice_SameTimeStamp();
                t3.testWrite(times);
                t3.testReader();

                TableTsFileV4_SameDevice_NoSameTimeStamp t4 = new TableTsFileV4_SameDevice_NoSameTimeStamp();
                t4.testWrite(times);
                t4.testReader();
            }
        } else {
            System.out.println("Unknown mode " + config.getConfigValue("MODE"));
        }

    }
}
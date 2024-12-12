package org.apache.iotdb.table;

import org.apache.iotdb.utils.ReadConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.ResultSetMetadata;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.*;

/**
 * 表模型V4版本：相同device, 不同时间分区
 */
public class TableTsFileV4_SameDevice_NoSameTimeStamp {
    // 实例化配置
    private final ReadConfig config = ReadConfig.getInstance();
    // 次数
    private int currentTimes = 1;
    // 生成路径
    private String path = config.getConfigValue("POSITION") + "TableTsFileV4_SameDevice_NoSameTimeStamp" + currentTimes + ".tsfile";
    private String path_rs = config.getConfigValue("POSITION") + "TableTsFileV4_SameDevice_NoSameTimeStamp" + currentTimes + ".tsfile.resource";
    // 目标文件
    private File f = FSFactoryProducer.getFSFactory().getFile(path);
    private File f_rs = FSFactoryProducer.getFSFactory().getFile(path_rs);
    // 表名前缀
    private String tableName = config.getConfigValue("TABLE_NAME");
    // 标识列名前缀
    private String idName = config.getConfigValue("ID_NAME");
    // 物理量列名前缀
    private String measurementName = config.getConfigValue("MEASUREMENT_NAME_TABLE");
    // 标识列数量
    private int idNum = Integer.parseInt(config.getConfigValue("ID_COLUMN_NUM"));
    // 物理量列倍数
    private int mNum = Integer.parseInt(config.getConfigValue("MEASUREMENT_COLUMN_NUM"));

    // 用于存放列名
    private List<String> columnNameList = new ArrayList<>();
    // 用于存放列的数据类型
    private List<TSDataType> dataTypesList = new ArrayList<>();
    // 用于存放列的类别
    private List<Tablet.ColumnCategory> columnCategories = new ArrayList<>();
    // 用于存放列的类型
    private List<ColumnSchema> columnSchemaList = new ArrayList<>();

    /**
     * 生成tsfile文件
     */
    public void testWrite(int times) {
        // 设置当前次数
        this.currentTimes = times;
        // 根据当前次数进行命名
        setCurrentName(currentTimes);
        // 根据配置判断是否需要为表添加后缀
        if (config.getConfigValue("IS_UNIQUE_TABLE_NAME").equals("true")) {
            tableName = tableName + "_samedevice_nosametimestamp" + times;
        } else {
            tableName = tableName + times;
        }
        try {
            // 判断文件是否存在
            if (f.exists()) {
                Files.delete(f.toPath());
            }
            if (f_rs.exists()) {
                Files.delete(f_rs.toPath());
            }
            // 写入数据
            write_tsfile();
        } catch (IOException | WriteProcessException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * 写入数据
     */
    private void write_tsfile() throws WriteProcessException, IOException {
        for (int i = 0; i < idNum; i++) {
            columnSchemaList.add(new ColumnSchema(idName + "_" + i, TSDataType.STRING, Tablet.ColumnCategory.ID));
            columnCategories.add(Tablet.ColumnCategory.ID);
        }
        for (int i = 0; i < idNum + mNum; i++) {
            columnSchemaList.add(new ColumnSchema(measurementName + "_" + "boolean" + "_" + i, TSDataType.BOOLEAN, Tablet.ColumnCategory.MEASUREMENT));
            columnSchemaList.add(new ColumnSchema(measurementName + "_" + "int32" + "_" + i, TSDataType.INT32, Tablet.ColumnCategory.MEASUREMENT));
            columnSchemaList.add(new ColumnSchema(measurementName + "_" + "int64" + "_" + i, TSDataType.INT64, Tablet.ColumnCategory.MEASUREMENT));
            columnSchemaList.add(new ColumnSchema(measurementName + "_" + "float" + "_" + i, TSDataType.FLOAT, Tablet.ColumnCategory.MEASUREMENT));
            columnSchemaList.add(new ColumnSchema(measurementName + "_" + "double" + "_" + i, TSDataType.DOUBLE, Tablet.ColumnCategory.MEASUREMENT));
            columnSchemaList.add(new ColumnSchema(measurementName + "_" + "text" + "_" + i, TSDataType.TEXT, Tablet.ColumnCategory.MEASUREMENT));
            columnSchemaList.add(new ColumnSchema(measurementName + "_" + "string" + "_" + i, TSDataType.STRING, Tablet.ColumnCategory.MEASUREMENT));
            columnSchemaList.add(new ColumnSchema(measurementName + "_" + "blob" + "_" + i, TSDataType.BLOB, Tablet.ColumnCategory.MEASUREMENT));
            columnSchemaList.add(new ColumnSchema(measurementName + "_" + "timestamp" + "_" + i, TSDataType.TIMESTAMP, Tablet.ColumnCategory.MEASUREMENT));
            columnSchemaList.add(new ColumnSchema(measurementName + "_" + "date" + "_" + i, TSDataType.DATE, Tablet.ColumnCategory.MEASUREMENT));
            columnCategories.add(Tablet.ColumnCategory.MEASUREMENT);
            columnCategories.add(Tablet.ColumnCategory.MEASUREMENT);
            columnCategories.add(Tablet.ColumnCategory.MEASUREMENT);
            columnCategories.add(Tablet.ColumnCategory.MEASUREMENT);
            columnCategories.add(Tablet.ColumnCategory.MEASUREMENT);
            columnCategories.add(Tablet.ColumnCategory.MEASUREMENT);
            columnCategories.add(Tablet.ColumnCategory.MEASUREMENT);
            columnCategories.add(Tablet.ColumnCategory.MEASUREMENT);
            columnCategories.add(Tablet.ColumnCategory.MEASUREMENT);
            columnCategories.add(Tablet.ColumnCategory.MEASUREMENT);
        }
        TableSchema schema = new TableSchema(tableName, columnSchemaList);

        try(ITsFileWriter writer = new TsFileWriterBuilder().file(f).tableSchema(schema).build()) {
            for (int i = 0; i < idNum; i++) {
                columnNameList.add(idName + "_" + i);
                dataTypesList.add(TSDataType.STRING);
            }
            for (int i = 0; i < mNum; i++) {
                columnNameList.add(measurementName + "_" + "boolean" + "_"  + i);
                columnNameList.add(measurementName + "_" + "int32" + "_" + i);
                columnNameList.add(measurementName + "_" + "int64" + "_" + i);
                columnNameList.add(measurementName + "_" + "float" + "_" + i);
                columnNameList.add(measurementName + "_" + "double" + "_" + i);
                columnNameList.add(measurementName + "_" + "text" + "_" + i);
                columnNameList.add(measurementName + "_" + "string" + "_" + i);
                columnNameList.add(measurementName + "_" + "blob" + "_" + i);
                columnNameList.add(measurementName + "_" + "timestamp" + "_" + i);
                columnNameList.add(measurementName + "_" + "date" + "_" + i);
                dataTypesList.add(TSDataType.BOOLEAN);
                dataTypesList.add(TSDataType.INT32);
                dataTypesList.add(TSDataType.INT64);
                dataTypesList.add(TSDataType.FLOAT);
                dataTypesList.add(TSDataType.DOUBLE);
                dataTypesList.add(TSDataType.TEXT);
                dataTypesList.add(TSDataType.STRING);
                dataTypesList.add(TSDataType.BLOB);
                dataTypesList.add(TSDataType.TIMESTAMP);
                dataTypesList.add(TSDataType.DATE);
            }
            Tablet tablet = new Tablet(columnNameList, dataTypesList, Integer.parseInt(config.getConfigValue("ROW_NUMBER")));
            tablet.setRowSize(Integer.parseInt(config.getConfigValue("ROW_NUMBER")));
            long time;
            if (config.getConfigValue("NEGATIVE_TIMESTAMP").equals("true")) {
                time = -Long.parseLong(config.getConfigValue("CROSS_PARTITION_TIMESTAMP"));
            } else {
                time = Long.parseLong(config.getConfigValue("CROSS_PARTITION_TIMESTAMP"));
            }
            long partition = Long.parseLong(config.getConfigValue("CROSS_PARTITION_SIZE"));
            for (int row = 0; row < tablet.getRowSize(); row++) {
                tablet.addTimestamp(row, time += partition);
                for (int i = 0; i < idNum; i++) {
                    tablet.addValue(row, columnNameList.get(i), getString(64));
                }
                for (int i = idNum; i < idNum + mNum; i+=10) {
                    if (config.getConfigValue("IS_CONTAIN_NULL_VALUES").equals("false")) {
                        tablet.addValue(row, columnNameList.get(i), getRandom().nextBoolean());
                        tablet.addValue(row, i + 1, getRandom().nextInt(-2147483647, 2147483647));
                        tablet.addValue(row, columnNameList.get(i + 2), getRandom().nextLong(-9223372036854775807L, 9223372036854775807L));
                        tablet.addValue(row, i + 3, (float) getRandom().nextDouble(-2147483647, 2147483647));
                        tablet.addValue(row, columnNameList.get(i + 4), getRandom().nextDouble(-2147483647, 2147483647));
                        tablet.addValue(row, i + 5, getString(1000));
                        tablet.addValue(row, columnNameList.get(i + 6), getString(100));
                        tablet.addValue(row, i + 7, getString(100).getBytes(StandardCharsets.UTF_8));
                        tablet.addValue(row, columnNameList.get(i + 8), getRandom().nextLong(-9223372036854775807L, 9223372036854775807L));
                        tablet.addValue(row, i + 9, LocalDate.ofEpochDay(getRandom().nextInt(-100000, 100000)));
                    } else {
                        if (row % 2 == 0) {
                            tablet.addValue(row, columnNameList.get(i), getRandom().nextBoolean());
                            tablet.addValue(row, i + 1, getRandom().nextInt(-2147483647, 2147483647));
                            tablet.addValue(row, columnNameList.get(i + 2), getRandom().nextLong(-9223372036854775807L, 9223372036854775807L));
                            tablet.addValue(row, i + 3, (float) getRandom().nextDouble(-2147483647, 2147483647));
                            tablet.addValue(row, columnNameList.get(i + 4), getRandom().nextDouble(-2147483647, 2147483647));
                        } else {
                            tablet.addValue(row, i + 5, getString(1000));
                            tablet.addValue(row, columnNameList.get(i + 6), getString(100));
                            tablet.addValue(row, i + 7, getString(100).getBytes(StandardCharsets.UTF_8));
                            tablet.addValue(row, columnNameList.get(i + 8), getRandom().nextLong(-9223372036854775807L, 9223372036854775807L));
                            tablet.addValue(row, i + 9, LocalDate.ofEpochDay(getRandom().nextInt(-100000, 100000)));
                        }

                    }
                }
            }
            writer.write(tablet);
        }
    }

    /**
     * 设置当前次数的命名
     */
    private void setCurrentName(int currentTimes) {
        path = config.getConfigValue("POSITION") + "TableTsFileV4_SameDevice_NoSameTimeStamp" + currentTimes + ".tsfile";
        f = FSFactoryProducer.getFSFactory().getFile(path);
        path_rs = config.getConfigValue("POSITION") + "TableTsFileV4_SameDevice_NoSameTimeStamp" + currentTimes + ".tsfile.resource";
        f_rs = FSFactoryProducer.getFSFactory().getFile(path_rs);
        idName = idName + currentTimes;
        measurementName = measurementName + currentTimes;
    }

    /**
     * 用于生成随机数值
     */
    private SplittableRandom getRandom() {
        return new SplittableRandom();
    }

    /**
     * 用于生成随机字符串
     */
    private String getString(int length) {
        final String CHAR_SET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                "abcdefghijklmnopqrstuvwxyz" +
                "0123456789"
                + "!@#$%^&*()-_=+[]{}|;:,.<>?/~`" +
                "去微软推哦爬山的风格结合两者相差不能";
        Random random = new Random();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int randomIndex = random.nextInt(CHAR_SET.length());
            sb.append(CHAR_SET.charAt(randomIndex));
        }
        return sb.toString();
    }

    /**
     * 读取数据
     */
    public void testReader() {
        // 查询表
        try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build()) {
            ResultSet resultSet = reader.query(tableName, columnNameList, -864000000L, 864000000L);
            ResultSetMetadata metadata = resultSet.getMetadata();
            StringJoiner sj = new StringJoiner(" ");
            for (int column = 1; column <= 12; column++) {
                sj.add(metadata.getColumnName(column) + "(" + metadata.getColumnType(column) + ") ");
            }
            System.out.println(sj);
            while (resultSet.next()) {
                Long timeField = resultSet.getLong("Time");
                Integer field1 = resultSet.isNull(columnNameList.get(idNum + 1)) ? null : resultSet.getInt(columnNameList.get(idNum + 1));
                Long field2 = resultSet.isNull(columnNameList.get(idNum + 8)) ? null : resultSet.getLong(columnNameList.get(idNum + 8));
                sj = new StringJoiner(" ");
                System.out.println(
                        sj.add(timeField + " ")
                                .add(field1 + " ")
                                .add(field2 + " ")
                );
            }
            resultSet.close();
        } catch (IOException | ReadProcessException | NoTableException | NoMeasurementException e) {
            throw new RuntimeException(e);
        }
    }

}
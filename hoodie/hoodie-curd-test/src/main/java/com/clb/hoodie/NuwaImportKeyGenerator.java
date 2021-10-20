package com.clb.hoodie;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.KeyGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Xiahu
 * @create 2020/9/21
 */
public class NuwaImportKeyGenerator extends KeyGenerator {
    private static final Logger log = LoggerFactory.getLogger(NuwaImportKeyGenerator.class);
    private static final String DEFAULT_PARTITION_PATH = "default";
    private static final Integer DEFAULT_HASH_CODE = 1000000;


    protected final String recordKeyField;
    protected final String partitionPathField;
    protected final boolean hiveStylePartitioning;

    protected final String partitiontype;
    protected final String partitionStyle;
    protected String dataFormat;

    protected Integer hashCode = -1;
    protected SimpleDateFormat partitionStyleSdf = null;


    protected SimpleDateFormat dateSdf;

    //hospital_no,source_code,lastupdatetime
    private List<String> partitionColumnList;
    //C,C,yyyy-MM-dd
    private List<String> dataFormatList;
    //C,C,M
    private List<String> partitionStylesList;
    //C,C,DATE
    private List<String> partitionTypeList;


    private List<String> partitionRecode;


    public NuwaImportKeyGenerator(TypedProperties props) {
        super(props);
        this.recordKeyField = props.getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY());
        this.partitionPathField = props.getString(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY());
        this.hiveStylePartitioning = props.getBoolean(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(), Boolean.parseBoolean(DataSourceWriteOptions.DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL()));

        this.partitiontype = props.getString(NuwaConstant.NUWA_IMPORT_PARTITION_TYPE);
        if (!props.getString(NuwaConstant.NUWA_IMPORT_PARTITION_TYPE).equalsIgnoreCase(NuwaConstant.MULTIPLE)) {
            this.dateSdf = new SimpleDateFormat(props.getString(NuwaConstant.NUWA_IMPORT_DATE_FORMAT));
        } else {
            this.dataFormat = props.getString(NuwaConstant.NUWA_IMPORT_DATE_FORMAT);
        }
        this.partitionStyle = props.getString(NuwaConstant.NUWA_IMPORT_PARTITION_STYLE);

    }

    @Override
    public HoodieKey getKey(GenericRecord record) {
        buildParam();

        if (recordKeyField == null || partitionPathField == null) {
            throw new HoodieKeyException("Unable to find field names for record key or partition path in cfg");
        }

        String recordKey = DataSourceUtils.getNestedFieldValAsString(record, recordKeyField, true);
        String partitionPath = DataSourceUtils.getNestedFieldValAsString(record, partitionPathField, true);
        //多分区逻辑规则
        if (this.partitiontype.equalsIgnoreCase(NuwaConstant.MULTIPLE)) {
            partitionRecode = getValueFromRecoed(record, partitionColumnList);
        }


        if (recordKey == null || recordKey.isEmpty()) {
            throw new HoodieKeyException("recordKey value: \"" + recordKey + "\" for field: \"" + recordKeyField + "\" cannot be null or empty.");
        }

        switch (partitiontype) {
            case NuwaConstant.HASH:
                partitionPath = String.valueOf(recordKey.hashCode() % DEFAULT_HASH_CODE);
                break;
            case NuwaConstant.DATE:
                try {
                    partitionPath = partitionStyleSdf.format(dateSdf.parse(partitionPath));
                } catch (ParseException e) {
                    log.error(String.format("Parse Date Error \n%s", e.getMessage()));
                }
                break;
            case NuwaConstant.MULTIPLE:
                partitionPath = "";
                if (partitionRecode.size() == dataFormatList.size() && dataFormatList.size() == dataFormatList.size() && dataFormatList.size() == partitionTypeList.size()) {
                    for (int i = 0; i < partitionRecode.size(); i++) {
                        String tmpPartitionPath = "";
                        String tmpPartitionColumn = partitionRecode.get(i);
                        String tmpPartitionType = partitionTypeList.get(i);
                        String tmpDataFormat = dataFormatList.get(i);
                        String tmpPartitionStyle = partitionStylesList.get(i);
                        if (tmpPartitionType.equalsIgnoreCase(NuwaConstant.CONSTANT) && tmpPartitionStyle.equalsIgnoreCase(NuwaConstant.CONSTANT)) {
                            tmpPartitionPath = tmpPartitionColumn;
                        } else if (tmpPartitionType.equalsIgnoreCase(NuwaConstant.HASH)) {
                            tmpPartitionPath = String.valueOf(recordKey.hashCode() % DEFAULT_HASH_CODE);
                        } else if (tmpPartitionType.equalsIgnoreCase(NuwaConstant.DATE)) {
                            try {
                                Date parse = new SimpleDateFormat(tmpDataFormat).parse(tmpPartitionColumn);
                                switch (tmpPartitionStyle) {
                                    case "Y":
                                        partitionStyleSdf = new SimpleDateFormat("yyyy");
                                        break;
                                    case "M":
                                        partitionStyleSdf = new SimpleDateFormat("yyyy-MM");
                                        break;
                                    case "D":
                                        partitionStyleSdf = new SimpleDateFormat("yyyy-MM-dd");
                                        break;
                                    default:
                                        log.error(String.format("%s format is not exist,please checkout database", this.partitionStyle));
                                        break;
                                }
                                tmpPartitionPath = partitionStyleSdf.format(parse);
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                        }
                        if (i < partitionRecode.size() - 1) {
                            partitionPath = partitionPath + tmpPartitionPath + "/";
                        } else {
                            partitionPath = partitionPath + tmpPartitionPath;
                        }
                    }
                }

                break;
            default:
                log.error(String.format("%s format is not exist,please checkout database", this.partitiontype));
                //partitionPath = DataSourceUtils.getNestedFieldValAsString(record, partitionPathField, true);
                break;
        }

        if (partitionPath == null || partitionPath.isEmpty()) {
            partitionPath = DEFAULT_PARTITION_PATH;
        }
        if (hiveStylePartitioning) {
            partitionPath = partitionPathField + "=" + partitionPath;
        }

        return new HoodieKey(recordKey, partitionPath);
    }

    private void buildParam() {
        try {
            if (this.partitiontype.equalsIgnoreCase(NuwaConstant.HASH)) {
                hashCode = Integer.parseInt(partitionStyle);
            } else if (this.partitiontype.equalsIgnoreCase(NuwaConstant.DATE)) {
                switch (partitionStyle) {
                    case "Y":
                        partitionStyleSdf = new SimpleDateFormat("yyyy");
                        break;
                    case "M":
                        partitionStyleSdf = new SimpleDateFormat("yyyy-MM");
                        break;
                    case "D":
                        partitionStyleSdf = new SimpleDateFormat("yyyy-MM-dd");
                        break;
                    default:
                        log.error(String.format("%s format is not exist,please checkout database", this.partitionStyle));
                        break;
                }
            } else if (this.partitiontype.equalsIgnoreCase(NuwaConstant.MULTIPLE)) {
                this.partitionColumnList = spiltColumn(partitionPathField);
                this.dataFormatList = spiltColumn(dataFormat);
                this.partitionStylesList = spiltColumn(partitionStyle);
                partitionTypeList = new ArrayList<>();
                for (String type : dataFormatList) {
                    if (type.equalsIgnoreCase(NuwaConstant.CONSTANT)) {
                        partitionTypeList.add(NuwaConstant.CONSTANT);
                    } else if (type.equalsIgnoreCase(NuwaConstant.DATE_FORMAT_YEAR) || type.equalsIgnoreCase(NuwaConstant.DATE_FORMAT_MONTH) || type.equalsIgnoreCase(NuwaConstant.DATE_FORMAT_Day)) {
                        partitionTypeList.add(NuwaConstant.DATE);
                    } else if (type.equalsIgnoreCase(NuwaConstant.HASH)) {
                        partitionTypeList.add(NuwaConstant.HASH);
                    } else {
                        log.error(String.format("%s format is not exist,please checkout database", type));
                    }
                }
            } else {
                log.error(String.format("%s format is not exist,please checkout database", this.partitiontype));
            }
        } catch (Exception e) {
            log.error(String.format("Build PartitionType Error , PartitionType : %s , PartitionStyle : %s  \n%s", partitiontype, partitionStyle, e.getMessage()));
            throw new RuntimeException(e.getMessage());
        }
    }


    private static List<String> spiltColumn(String line) {
        List<String> result = new LinkedList<>();
        String[] split = line.split(",");
        for (String str : split) {
            result.add(str);
        }
        return result;
    }

    private static List<String> getValueFromRecoed(GenericRecord record, List<String> list) {
        List<String> result = null;
        if (list.size() > 0) {
            result = new LinkedList<>();
            for (String column : list) {
                result.add(DataSourceUtils.getNestedFieldValAsString(record, column, true));
            }
        }
        return result;
    }
}

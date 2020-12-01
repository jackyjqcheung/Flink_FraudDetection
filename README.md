
##### 官方示例：
https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/try-flink/datastream_api.html#%E5%AE%8C%E6%95%B4%E7%9A%84%E7%A8%8B%E5%BA%8F



##### 项目构建
```shell script
mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-datastream-java \
    -DarchetypeVersion=1.11.2 \
    -DgroupId=frauddetection \
    -DartifactId=frauddetection \
    -Dversion=0.1 \
    -Dpackage=spendreport \
    -DinteractiveMode=false
```


##### Flink 状态
KeyedProcessFunction和ProcessFunction并无直接关系
KeyedProcessFunction用于处理KeyedStream的数据集合，相比ProcessFunction类，KeyedProcessFunction拥有更多特性，
状态处理和定时器功能都是KeyedProcessFunction才有的：







##### 参考文章
https://www.cnblogs.com/bolingcavalry/p/14014561.html

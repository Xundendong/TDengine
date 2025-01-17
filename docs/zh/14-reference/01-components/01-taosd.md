---
title: "taosd 参考手册"
sidebar_label: "taosd"
toc_max_heading_level: 4
---

taosd 是 TDengine 数据库引擎的核心服务，其配置文件默认是 `/etc/taos/taos.cfg` 但也可以指定不同路径的配置文件。本节详细介绍 taosd 的命令行参数及配置文件中的配置参数。

## 命令行参数

taosd 命令行参数如下
- -a `<json file>`：指定一个 JSON 文件，其中包含服务启动时的各项配置参数，其格式形如 `{"fqdn":"td1"}`，关于配置参数的细节请参考下一节
- -c `<directory>`：指定配置文件所在目录
- -s：打印 SDB 信息
- -C: 打印配置信息
- -e: 指定环境变量，其格式形如 `-e 'TAOS_FQDN=td1'`
- -k: 获取机器码
- -dm: 启用内存调度
- -V: 打印版本信息

## 配置参数

:::note
配置文件参数修改后，需要重启*taosd*服务，或客户端应用才能生效。

:::

### 连接相关
|参数名称|支持版本|动态修改|参数含义|
|-----------------------|-------------------------|-------------------------|------------|
|firstEp                |                         |不支持动态修改             |taosd 启动时，主动连接的集群中首个 dnode 的 end point，默认值 localhost:6030|
|secondEp               |                         |不支持动态修改             |taosd 启动时，如果 firstEp 连接不上，尝试连接集群中第二个 dnode 的 endpoint，无默认值|
|fqdn                   |                         |不支持动态修改             |taosd 监听的服务地址，默认为所在服务器上配置的第一个 hostname|
|serverPort             |                         |不支持动态修改             |taosd 监听的端口，默认值 6030|
|compressMsgSize        |                         |支持动态修改 重启生效       |是否对 RPC 消息进行压缩；-1：所有消息都不压缩；0：所有消息都压缩；N (N>0)：只有大于 N 个字节的消息才压缩；默认值 -1|
|shellActivityTimer     |                         |支持动态修改 立即生效       |客户端向 mnode 发送心跳的时长，单位为秒，取值范围 1-120，默认值 3 |
|numOfRpcSessions       |                         |支持动态修改 重启生效       |RPC 支持的最大连接数，取值范围 100-100000，默认值 30000|
|numOfRpcThreads        |                         |支持动态修改 重启生效       |RPC 收发数据线程数目，取值范围1-50,默认值为 CPU 核数的一半|
|numOfTaskQueueThreads  |                         |支持动态修改 重启生效       |客户端处理 RPC 消息的线程数取值, 范围4-16,默认值为 CPU 核数的一半|
|rpcQueueMemoryAllowed  |                         |支持动态修改 立即生效       |dnode允许的已经收到的RPC消息占用的内存最大值，单位 bytes，取值范围 104857600-INT64_MAX，默认值为服务器内存的 1/10 |
|resolveFQDNRetryTime   | 3.x 之后取消             |不支持动态修改             |FQDN 解析失败时的重试次数|
|timeToGetAvailableConn | 3.3.4.x之后取消          |支持动态修改 重启生效       |获得可用连接的最长等待时间，取值范围 10-50000000，单位为毫秒，默认值 500000|
|maxShellConns          | 3.x 后取消             |支持动态修改 重启生效 |允许创建的最大链接数|
|maxRetryWaitTime       |                         |支持动态修改 重启生效 |重连最大超时时间, 从重试时候开始计算, 取值范围0-86400000, 单位为毫秒, 默认值10000|
|shareConnLimit         |3.3.4.0 新增             |支持动态修改 重启生效  |一个链接可以共享的请求的数目，取值范围 1-512，默认值 10|
|readTimeout            |3.3.4.0 新增             |支持动态修改 重启生效  |单个请求最小超时时间，取值范围 64-604800，单位为秒，默认值 900|

### 监控相关
|参数名称|支持版本|动态修改|参数含义|
|-----------------------|----------|-------------------------|-|
|monitor                |          |支持动态修改 立即生效       |是否收集监控数据并上报，0：关闭；1:打开；默认值 0|
|monitorFqdn            |          |支持动态修改 重启生效  |taosKeeper 服务所在服务器的 FQDN，默认值 无|
|monitorPort            |          |支持动态修改 重启生效  |taosKeeper 服务所监听的端口号，默认值 6043|
|monitorInterval        |          |支持动态修改 立即生效       |监控数据库记录系统参数（CPU/内存）的时间间隔，单位是秒，取值范围 1-200000 ，默认值 30|
|monitorMaxLogs         |          |支持动态修改 立即生效       |缓存的待上报日志条数|
|monitorComp            |          |支持动态修改 重启生效  |是否采用压缩方式上报监控日志时|
|monitorLogProtocol     |          |支持动态修改 立即生效       |是否打印监控日志|
|monitorForceV2         |          |支持动态修改 立即生效       |是否使用 V2 版本协议上报|
|telemetryReporting     |          |支持动态修改 立即生效       |是否上传 telemetry，0：不上传，1：上传，默认值 1|
|telemetryServer        |          |不支持动态修改             |telemetry 服务器地址|
|telemetryPort          |          |不支持动态修改             |telemetry 服务器端口编号|
|telemetryInterval      |          |支持动态修改 立即生效       |telemetry 上传时间间隔，单位为秒，默认 86400|
|crashReporting         |          |支持动态修改 立即生效       |是否上传 crash 信息；0：不上传，1：上传；默认值  1|

### 查询相关
|参数名称|支持版本|动态修改|参数含义|
|------------------------|----------|-------------------------|-|
|countAlwaysReturnValue  |          |支持动态修改 立即生效       |count/hyperloglog 函数在输入数据为空或者 NULL 的情况下是否返回值；0：返回空行，1：返回；默认值 1；该参数设置为 1 时，如果查询中含有 INTERVAL 子句或者该查询使用了 TSMA 时，且相应的组或窗口内数据为空或者 NULL，对应的组或窗口将不返回查询结果；注意此参数客户端和服务端值应保持一致|
|tagFilterCache          |          |不支持动态修改             |是否缓存标签过滤结果|
|queryBufferSize         |          |支持动态修改 重启生效  |暂不生效|
|queryRspPolicy          |          |支持动态修改 立即生效       |查询响应策略|
|queryUseMemoryPool      |          |不支持动态修改  |查询是否使用内存池管理内存，默认值：1（打开）; 0: 关闭，1: 打开|
|minReservedMemorySize   |          |不支持动态修改  |最小预留的系统可用内存数量，除预留外的内存都可以被用于查询，单位：MB，默认预留大小为系统物理内存的 20%，取值范围 1024 - 1000000000|
|singleQueryMaxMemorySize|          |不支持动态修改  |单个查询在单个节点(dnode)上可以使用的内存上限，超过该上限将返回错误，单位：MB，默认值：0（无上限），取值范围 0 - 1000000000|
|filterScalarMode        |          |不支持动态修改             |强制使用标量过滤模式，0：关闭；1：开启，默认值 0|
|queryRsmaTolerance      |          |不支持动态修改             |内部参数，用于判定查询哪一级 rsma 数据时的容忍时间，单位为毫秒|
|pqSortMemThreshold      |          |不支持动态修改             |内部参数，排序使用的内存阈值|

### 区域相关
|参数名称|支持版本|动态修改|参数含义|
|-----------------|----------|-------------------------|-|
|timezone         |          |不支持动态修改             |时区；缺省从系统中动态获取当前的时区设置|
|locale           |          |不支持动态修改             |系统区位信息及编码格式，缺省从系统中获取|
|charset          |          |不支持动态修改             |字符集编码，缺省从系统中获取|

:::info
1. 为应对多时区的数据写入和查询问题，TDengine 采用 Unix 时间戳(Unix Timestamp)来记录和存储时间戳。Unix 时间戳的特点决定了任一时刻不论在任何时区，产生的时间戳均一致。需要注意的是，Unix 时间戳是在客户端完成转换和记录。为了确保客户端其他形式的时间转换为正确的 Unix 时间戳，需要设置正确的时区。

在 Linux/macOS 中，客户端会自动读取系统设置的时区信息。用户也可以采用多种方式在配置文件设置时区。例如：

```
timezone UTC-8
timezone GMT-8
timezone Asia/Shanghai
```

均是合法的设置东八区时区的格式。但需注意，Windows 下并不支持 `timezone Asia/Shanghai` 这样的写法，而必须写成 `timezone UTC-8`。

时区的设置对于查询和写入 SQL 语句中非 Unix 时间戳的内容（时间戳字符串、关键词 now 的解析）产生影响。例如：

```sql
SELECT count(*) FROM table_name WHERE TS<'2019-04-11 12:01:08';
```

在东八区，SQL 语句等效于

```sql
SELECT count(*) FROM table_name WHERE TS<1554955268000;
```

在 UTC 时区，SQL 语句等效于

```sql
SELECT count(*) FROM table_name WHERE TS<1554984068000;
```

为了避免使用字符串时间格式带来的不确定性，也可以直接使用 Unix 时间戳。此外，还可以在 SQL 语句中使用带有时区的时间戳字符串，例如：RFC3339 格式的时间戳字符串，2013-04-12T15:52:01.123+08:00 或者 ISO-8601 格式时间戳字符串 2013-04-12T15:52:01.123+0800。上述两个字符串转化为 Unix 时间戳不受系统所在时区的影响。


2. TDengine 为存储中文、日文、韩文等非 ASCII 编码的宽字符，提供一种专门的字段类型 nchar。写入 nchar 字段的数据将统一采用 UCS4-LE 格式进行编码并发送到服务器。需要注意的是，编码正确性是客户端来保证。因此，如果用户想要正常使用 nchar 字段来存储诸如中文、日文、韩文等非 ASCII 字符，需要正确设置客户端的编码格式。

客户端的输入的字符均采用操作系统当前默认的编码格式，在 Linux/macOS 系统上多为 UTF-8，部分中文系统编码则可能是 GB18030 或 GBK 等。在 docker 环境中默认的编码是 POSIX。在中文版 Windows 系统中，编码则是 CP936。客户端需要确保正确设置自己所使用的字符集，即客户端运行的操作系统当前编码字符集，才能保证 nchar 中的数据正确转换为 UCS4-LE 编码格式。

在 Linux/macOS 中 locale 的命名规则为：\<语言>_\<地区>.\<字符集编码> 如：zh_CN.UTF-8，zh 代表中文，CN 代表大陆地区，UTF-8 表示字符集。字符集编码为客户端正确解析本地字符串提供编码转换的说明。Linux/macOS 可以通过设置 locale 来确定系统的字符编码，由于 Windows 使用的 locale 中不是 POSIX 标准的 locale 格式，因此在 Windows 下需要采用另一个配置参数 charset 来指定字符编码。在 Linux/macOS 中也可以使用 charset 来指定字符编码。


3. 如果配置文件中不设置 charset，在 Linux/macOS 中，taos 在启动时候，自动读取系统当前的 locale 信息，并从 locale 信息中解析提取 charset 编码格式。如果自动读取 locale 信息失败，则尝试读取 charset 配置，如果读取 charset 配置也失败，则中断启动过程。

在 Linux/macOS 中，locale 信息包含了字符编码信息，因此正确设置了 Linux/macOS 的 locale 以后可以不用再单独设置 charset。例如：

```
locale zh_CN.UTF-8
```

在 Windows 系统中，无法从 locale 获取系统当前编码。如果无法从配置文件中读取字符串编码信息，taos 默认设置为字符编码为 CP936。其等效在配置文件中添加如下配置：

```
charset CP936
```

如果需要调整字符编码，请查阅当前操作系统使用的编码，并在配置文件中正确设置。

在 Linux/macOS 中，如果用户同时设置了 locale 和字符集编码 charset，并且 locale 和 charset 的不一致，后设置的值将覆盖前面设置的值。

```
locale zh_CN.UTF-8
charset GBK
```

则 charset 的有效值是 GBK。

```
charset GBK
locale zh_CN.UTF-8
```

charset 的有效值是 UTF-8。

:::

### 存储相关
|参数名称|支持版本|动态修改|参数含义|
|--------------------|----------|-------------------------|-|
|dataDir             |          |不支持动态修改             |数据文件目录，所有的数据文件都将写入该目录，默认值 /var/lib/taos|
|diskIDCheckEnabled  |          |不支持动态修改             |在 3.3.4.3 后，在重启 dnode 时增加了检查 dataDir 所在磁盘 id 是否发生改变，0:进行检查，1:不进行检查；默认值：1|
|tempDir             |          |不支持动态修改             |指定所有系统运行过程中的临时文件生成的目录，默认值 /tmp|
|minimalDataDirGB    |          |不支持动态修改             |dataDir 指定的时序数据存储目录所需要保留的最小空间，单位 GB，默认值 2|
|minimalTmpDirGB     |          |不支持动态修改             |tempDir 所指定的临时文件目录所需要保留的最小空间，单位 GB，默认值 1|
|minDiskFreeSize     |3.1.1.0 后|支持动态修改 立即生效       |当某块磁盘上的可用空间小于等于这个阈值时，该磁盘将不再被选择用于生成新的数据文件，单位为字节，取值范围 52428800-1073741824，默认值为 52428800；企业版参数|
|s3MigrateIntervalSec|3.3.4.3 后|支持动态修改 立即生效       |本地数据文件自动上传 S3 的触发周期，单位为秒。最小值：600；最大值：100000。默认值 3600；企业版参数|
|s3MigrateEnabled    |3.3.4.3 后|支持动态修改 立即生效       |是否自动进行 S3 迁移，默认值为 0，表示关闭自动 S3 迁移，可配置为 1；企业版参数|
|s3Accesskey         |3.3.4.3 后|支持动态修改 重启生效       |冒号分隔的用户 SecretId:SecretKey，例如 AKIDsQmwsfKxTo2A6nGVXZN0UlofKn6JRRSJ:lIdoy99ygEacU7iHfogaN2Xq0yumSm1E；企业版参数|
|s3Endpoint          |3.3.4.3 后|支持动态修改 重启生效       |用户所在地域的 COS 服务域名，支持 http 和 https，bucket 的区域需要与 endpoint 保持一致，否则无法访问；企业版参数|
|s3BucketName        |3.3.4.3 后|支持动态修改 重启生效       |存储桶名称，减号后面是用户注册 COS 服务的 AppId，其中 AppId 是 COS 特有，AWS 和阿里云都没有，配置时需要作为 bucket name 的一部分，使用减号分隔；参数值均为字符串类型，但不需要引号；例如 test0711-1309024725；企业版参数|
|s3PageCacheSize     |3.3.4.3 后|支持动态修改 重启生效       |S3 page cache 缓存页数目，取值范围 4-1048576，单位为页，默认值 4096；企业版参数|
|s3UploadDelaySec    |3.3.4.3 后|支持动态修改 立即生效       |data 文件持续多长时间不再变动后上传至 S3，取值范围 1-2592000 (30天），单位为秒，默认值 60；企业版参数|
|cacheLazyLoadThreshold|        |支持动态修改 立即生效       |内部参数，缓存的装载策略|

### 集群相关
|参数名称|支持版本|动态修改|参数含义|
|--------------------------|----------|-------------------------|-|
|supportVnodes             |          |支持动态修改 立即生效       |dnode 支持的最大 vnode 数目，取值范围 0-4096，默认值 CPU 核数的 2 倍 + 5|
|numOfCommitThreads        |          |支持动态修改 重启生效       |落盘线程的最大数量，取值范围 1-1024，默认值为 4|
|numOfCompactThreads        |         |支持动态修改 重启生效       |落盘线程的最大数量，取值范围 1-16，默认值为 2|
|numOfMnodeReadThreads     |          |支持动态修改 重启生效       |mnode 的 Read 线程数目，取值范围 0-1024，默认值为 CPU 核数的四分之一（不超过 4）|
|numOfVnodeQueryThreads    |          |支持动态修改 重启生效       |vnode 的 Query 线程数目，取值范围 0-1024，默认值为 CPU 核数的两倍（不超过 16）|
|numOfVnodeFetchThreads    |          |支持动态修改 重启生效       |vnode 的 Fetch 线程数目，取值范围 0-1024，默认值为 CPU 核数的四分之一（不超过 4）|
|numOfVnodeRsmaThreads     |          |支持动态修改 重启生效       |vnode 的 Rsma 线程数目，取值范围 0-1024，默认值为 CPU 核数的四分之一（不超过 4）|
|numOfQnodeQueryThreads    |          |支持动态修改 重启生效       |qnode 的 Query 线程数目，取值范围 0-1024，默认值为 CPU 核数的两倍（不超过 16）|
|numOfSnodeSharedThreads   |          |支持动态修改 重启生效       |snode 的共享线程数目，取值范围 0-1024，默认值为 CPU 核数的四分之一（不小于 2，不超过 4）|
|numOfSnodeUniqueThreads   |          |支持动态修改 重启生效       |snode 的独占线程数目，取值范围 0-1024，默认值为 CPU 核数的四分之一（不小于 2，不超过 4）|
|ratioOfVnodeStreamThreads |          |支持动态修改 重启生效       |流计算使用 vnode 线程的比例，取值范围 0.01-4，默认值 0.5|
|ttlUnit                   |          |不支持动态修改             |ttl 参数的单位，取值范围 1-31572500，单位为秒，默认值 86400|
|ttlPushInterval           |          |支持动态修改 立即生效       |ttl 检测超时频率，取值范围 1-100000，单位为秒，默认值 10|
|ttlChangeOnWrite          |          |支持动态修改 立即生效       |ttl 到期时间是否伴随表的修改操作改变；0：不改变，1：改变；默认值为 0|
|ttlBatchDropNum           |          |支持动态修改 立即生效       |ttl 一批删除子表的数目，最小值为 0，默认值 10000|
|retentionSpeedLimitMB     |          |支持动态修改 立即生效       |数据在不同级别硬盘上迁移时的速度限制，取值范围 0-1024，单位 MB，默认值 0，表示不限制|
|maxTsmaNum                |          |支持动态修改 立即生效       |集群内可创建的TSMA个数；取值范围 0-3；默认值 3|
|tmqMaxTopicNum            |          |支持动态修改 立即生效       |订阅最多可建立的 topic 数量；取值范围 1-10000；默认值为 20|
|tmqRowSize                |          |支持动态修改 立即生效       |订阅数据块的最大记录条数，取值范围 1-1000000，默认值 4096|
|audit                     |          |支持动态修改 立即生效       |审计功能开关；企业版参数|
|auditInterval             |          |支持动态修改 立即生效       |审计数据上报的时间间隔；企业版参数|
|auditCreateTable          |          |支持动态修改 立即生效       |是否针对创建子表开启申计功能；企业版参数|
|encryptAlgorithm          |          |不支持动态修改             |数据加密算法；企业版参数|
|encryptScope              |          |不支持动态修改             |加密范围；企业版参数|
|enableWhiteList           |          |支持动态修改 立即生效       |白名单功能开关；企业版参数|
|syncLogBufferMemoryAllowed|          |支持动态修改 立即生效       |一个 dnode 允许的 sync 日志缓存消息占用的内存最大值，单位 bytes，取值范围 104857600-INT64_MAX，默认值 服务器内存的 1/10，3.1.3.2/3.3.2.13 版本开始生效 |
|syncElectInterval         |          |不支持动态修改             |内部参数，用于同步模块调试|
|syncHeartbeatInterval     |          |不支持动态修改             |内部参数，用于同步模块调试|
|syncHeartbeatTimeout      |          |不支持动态修改             |内部参数，用于同步模块调试|
|syncSnapReplMaxWaitN      |          |支持动态修改 立即生效       |内部参数，用于同步模块调试|
|arbHeartBeatIntervalSec   |          |支持动态修改 立即生效       |内部参数，用于同步模块调试|
|arbCheckSyncIntervalSec   |          |支持动态修改 立即生效       |内部参数，用于同步模块调试|
|arbSetAssignedTimeoutSec  |          |支持动态修改 立即生效       |内部参数，用于同步模块调试|
|mndSdbWriteDelta          |          |支持动态修改 立即生效       |内部参数，用于 mnode 模块调试|
|mndLogRetention           |          |支持动态修改 立即生效       |内部参数，用于 mnode 模块调试|
|skipGrant                 |          |不支持动态修改             |内部参数，用于授权检查|
|trimVDbIntervalSec        |          |支持动态修改 立即生效       |内部参数，用于删除过期数据|
|ttlFlushThreshold         |          |支持动态修改 立即生效       |内部参数，ttl 定时器的频率|
|compactPullupInterval     |          |支持动态修改 立即生效       |内部参数，数据重整定时器的频率|
|walFsyncDataSizeLimit     |          |支持动态修改 立即生效       |内部参数，WAL 进行 FSYNC 的阈值|
|transPullupInterval       |          |支持动态修改 立即生效       |内部参数，mnode 执行事务的重试间隔|
|mqRebalanceInterval       |          |支持动态修改 立即生效       |内部参数，消费者再平衡的时间间隔|
|uptimeInterval            |          |支持动态修改 立即生效       |内部参数，用于记录系统启动时间|
|timeseriesThreshold       |          |支持动态修改 立即生效       |内部参数，用于统计用量|
|udf                       |          |支持动态修改 重启生效       |是否启动 UDF 服务；0：不启动，1：启动；默认值为 0 |
|udfdResFuncs              |          |支持动态修改 重启生效       |内部参数，用于 UDF 结果集设置|
|udfdLdLibPath             |          |支持动态修改 重启生效       |内部参数，表示 UDF 装载的库路径|


### 流计算参数
|参数名称|支持版本|动态修改|参数含义|
|-----------------------|-------------------------|----------|-|
|disableStream          |          |支持动态修改 重启生效       |流计算的启动开关|
|streamBufferSize       |          |支持动态修改 重启生效       |控制内存中窗口状态缓存的大小，默认值为 128MB|
|streamAggCnt           |          |不支持动态修改             |内部参数，并发进行聚合计算的数目|
|checkpointInterval     |          |支持动态修改 重启生效       |内部参数，checkponit 同步间隔|
|concurrentCheckpoint   |          |支持动态修改 立即生效       |内部参数，是否并发检查 checkpoint|
|maxStreamBackendCache  |          |支持动态修改 立即生效       |内部参数，流计算使用的最大缓存|
|streamSinkDataRate     |          |支持动态修改 重启生效       |内部参数，用于控制流计算结果的写入速度|

### 日志相关
|参数名称|支持版本|动态修改|参数含义|
|----------------|-------------------------|----------|-|
|logDir          |          |不支持动态修改             |日志文件目录，运行日志将写入该目录，默认值 /var/log/taos|
|minimalLogDirGB |          ｜不支持动态修改             |日志文件夹所在磁盘可用空间大小小于该值时，停止写日志，单位 GB，默认值 1|
|numOfLogLines   |          ｜支持动态修改 立即生效       |单个日志文件允许的最大行数，默认值 10,000,000|
|asyncLog        |          |支持动态修改 立即生效       |日志写入模式，0：同步，1：异步，默认值 1|
|logKeepDays     |          ｜支持动态修改 立即生效       |日志文件的最长保存时间，单位：天，默认值 0，小于等于0意味着只有两个日志文件相互切换保存日志，超过两个文件保存数量的日志会被删除；当设置为大于 0 的值时，当日志文件大小达到设置的上限时会被重命名为 taosdlog.yyy，其中 yyy 为日志文件最后修改的时间戳，并滚动产生新的日志文件|
|slowLogThreshold|3.3.3.0 后|支持动态修改 立即生效       |慢查询门限值，大于等于门限值认为是慢查询，单位秒，默认值 3 |
|slowLogMaxLen   |3.3.3.0 后|支持动态修改 立即生效       |慢查询日志最大长度，取值范围 1-16384，默认值 4096|
|slowLogScope    |3.3.3.0 后|支持动态修改 立即生效       |慢查询记录类型，取值范围 ALL/QUERY/INSERT/OTHERS/NONE，默认值 QUERY|
|slowLogExceptDb |3.3.3.0 后|支持动态修改 立即生效       |指定的数据库不上报慢查询，仅支持配置换一个数据库|
|debugFlag       |          |支持动态修改 立即生效       |运行日志开关，131（输出错误和警告日志），135（输出错误、警告和调试日志），143（输出错误、警告、调试和跟踪日志）；默认值 131 或 135 （取决于不同模块），该参数的设置会影响所有模块的开关，后设置的参数起效|
|tmrDebugFlag    |          |支持动态修改 立即生效       |定时器模块的日志开关，取值范围同上|
|uDebugFlag      |          |支持动态修改 立即生效       |共用功能模块的日志开关，取值范围同上|
|rpcDebugFlag    |          |支持动态修改 立即生效       |rpc 模块的日志开关，取值范围同上|
|qDebugFlag      |          |支持动态修改 立即生效       |query 模块的日志开关，取值范围同上|
|dDebugFlag      |          |支持动态修改 立即生效       |dnode 模块的日志开关，取值范围同上|
|vDebugFlag      |          |支持动态修改 立即生效       |vnode 模块的日志开关，取值范围同上|
|mDebugFlag      |          |支持动态修改 立即生效       |mnode 模块的日志开关，取值范围同上|
|azDebugFlag     |3.3.4.3 后|支持动态修改 立即生效       |S3 模块的日志开关，取值范围同上|
|sDebugFlag      |          |支持动态修改 立即生效       |sync 模块的日志开关，取值范围同上|
|tsdbDebugFlag   |          |支持动态修改 立即生效       |tsdb 模块的日志开关，取值范围同上|
|tqDebugFlag     |          |支持动态修改 立即生效       |tq 模块的日志开关，取值范围同上|
|fsDebugFlag     |          |支持动态修改 立即生效       |fs 模块的日志开关，取值范围同上|
|udfDebugFlag    |          |支持动态修改 立即生效       |udf 模块的日志开关，取值范围同上|
|smaDebugFlag    |          |支持动态修改 立即生效       |sma 模块的日志开关，取值范围同上|
|idxDebugFlag    |          |支持动态修改 立即生效       |index 模块的日志开关，取值范围同上|
|tdbDebugFlag    |          |支持动态修改 立即生效       |tdb 模块的日志开关，取值范围同上|
|metaDebugFlag   |          |支持动态修改 立即生效       |meta 模块的日志开关，取值范围同上|
|stDebugFlag     |          |支持动态修改 立即生效       |stream 模块的日志开关，取值范围同上|
|sndDebugFlag    |          |支持动态修改 立即生效       |snode 模块的日志开关，取值范围同上|

### 调试相关
|参数名称|支持版本|动态修改|参数含义|
|--------------------|-------------------------|----------|-|
|enableCoreFile      |          |支持动态修改 立即生效       |crash 时是否生成 core 文件，0：不生成，1：生成；默认值 1|
|configDir           |          |不支持动态修改             |配置文件所在目录|
|forceReadConfig     |          |不支持动态修改             |强制使用配置文件中的参数，0:使用持久化的配置参数，1:使用配置文件中的配置参数；默认值 0|
|scriptDir           |          |不支持动态修改             |内部测试工具的脚本目录|
|assert              |          |不支持动态修改             |断言控制开关，默认值 0|
|randErrorChance     |          |支持动态修改 立即生效       |内部参数，用于随机失败测试|
|randErrorDivisor    |          |支持动态修改 立即生效       |内部参数，用于随机失败测试|
|randErrorScope      |          |支持动态修改 立即生效       |内部参数，用于随机失败测试|
|safetyCheckLevel    |          |支持动态修改 立即生效       |内部参数，用于随机失败测试|
|experimental        |          |支持动态修改 立即生效       |内部参数，用于一些实验特性|
|simdEnable          |3.3.4.3 后|不支持动态修改             |内部参数，用于测试 SIMD 加速|
|AVX512Enable        |3.3.4.3 后|不支持动态修改             |内部参数，用于测试 AVX512 加速|
|rsyncPort           |          |不支持动态修改             |内部参数，用于调试流计算|
|snodeAddress        |          |支持动态修改 重启生效       |内部参数，用于调试流计算|
|checkpointBackupDir |          |支持动态修改 重启生效       |内部参数，用于恢复 snode 数据|
|enableAuditDelete   |          |不支持动态修改             |内部参数，用于测试审计功能|
|slowLogThresholdTest|          |不支持动态修改             |内部参数，用于测试慢日志|
|bypassFlag          |3.3.4.5 后|支持动态修改 立即生效       |内部参数，用于短路测试，0：正常写入，1：写入消息在 taos 客户端发送 RPC 消息前返回，2：写入消息在 taosd 服务端收到 RPC 消息后返回，4：写入消息在 taosd 服务端写入内存缓存前返回，8：写入消息在 taosd 服务端数据落盘前返回；默认值 0|

### 压缩参数
|参数名称|支持版本|动态修改|参数含义|
|------------|----------|-------------------------|-|
|fPrecision  |          |支持动态修改 立即生效       |设置 float 类型浮点数压缩精度 ，取值范围 0.1 ~ 0.00000001  ，默认值  0.00000001  , 小于此值的浮点数尾数部分将被截断|
|dPrecision  |          |支持动态修改 立即生效       |设置 double 类型浮点数压缩精度 , 取值范围 0.1 ~ 0.0000000000000001 ， 默认值 0.0000000000000001 ， 小于此值的浮点数尾数部分将被截取|
|lossyColumn |3.3.0.0 前|不支持动态修改             |对 float 和/或 double 类型启用 TSZ 有损压缩；取值范围 float/double/none；默认值 none，表示关闭无损压缩|
|ifAdtFse    |          |支持动态修改 重启生效       |在启用 TSZ 有损压缩时，使用 FSE 算法替换 HUFFMAN 算法，FSE 算法压缩速度更快，但解压稍慢，追求压缩速度可选用此算法；0：关闭，1：打开；默认值为 0|
|maxRange    |          |支持动态修改 重启生效       |内部参数，用于有损压缩设置|
|curRange    |          |支持动态修改 重启生效       |内部参数，用于有损压缩设置|
|compressor  |          |支持动态修改 重启生效       |内部参数，用于有损压缩设置|

**补充说明**
1. 在 3.4.0.0 之后，所有配置参数都将被持久化到本地存储，重启数据库服务后，将默认使用持久化的配置参数列表；如果您希望继续使用 config 文件中配置的参数，需设置 forceReadConfig 为 1。
2. 在 3.2.0.0 ~ 3.3.0.0（不包含）版本生效，启用该参数后不能回退到升级前的版本
3. TSZ 压缩算法是通过数据预测技术完成的压缩，所以更适合有规律变化的数据
4. TSZ 压缩时间会更长一些，如果您的服务器 CPU 空闲多，存储空间小的情况下适合选用
5. 示例：对 float 和 double 类型都启用有损压缩
```shell
lossyColumns     float|double
```
6. 配置需重启服务生效，重启如果在 taosd 日志中看到以下内容，表明配置已生效：
```sql
   02/22 10:49:27.607990 00002933 UTL  lossyColumns     float|double
```


## taosd 监控指标

taosd 会将监控指标上报给 taosKeeper，这些监控指标会被 taosKeeper 写入监控数据库，默认是 `log` 库，可以在 taoskeeper 配置文件中修改。以下是这些监控指标的详细介绍。

### taosd\_cluster\_basic 表

`taosd_cluster_basic` 表记录集群基础信息。

| field                | type      | is\_tag | comment                         |
| :------------------- | :-------- | :------ | :------------------------------ |
| ts                   | TIMESTAMP |         | timestamp                       |
| first\_ep            | VARCHAR   |         | 集群 first ep                   |
| first\_ep\_dnode\_id | INT       |         | 集群 first ep 的 dnode id       |
| cluster_version      | VARCHAR   |         | tdengine version。例如：3.0.4.0 |
| cluster\_id          | VARCHAR   | TAG     | cluster id                      |

### taosd\_cluster\_info 表

`taosd_cluster_info` 表记录集群信息。

| field                    | type      | is\_tag | comment                                          |
| :----------------------- | :-------- | :------ | :----------------------------------------------- |
| \_ts                     | TIMESTAMP |         | timestamp                                        |
| cluster_uptime           | DOUBLE    |         | 当前 master 节点的uptime。单位：秒               |
| dbs\_total               | DOUBLE    |         | database 总数                                    |
| tbs\_total               | DOUBLE    |         | 当前集群 table 总数                              |
| stbs\_total              | DOUBLE    |         | 当前集群 stable 总数                             |
| dnodes\_total            | DOUBLE    |         | 当前集群 dnode 总数                              |
| dnodes\_alive            | DOUBLE    |         | 当前集群 dnode 存活总数                          |
| mnodes\_total            | DOUBLE    |         | 当前集群 mnode 总数                              |
| mnodes\_alive            | DOUBLE    |         | 当前集群 mnode 存活总数                          |
| vgroups\_total           | DOUBLE    |         | 当前集群 vgroup 总数                             |
| vgroups\_alive           | DOUBLE    |         | 当前集群 vgroup 存活总数                         |
| vnodes\_total            | DOUBLE    |         | 当前集群 vnode 总数                              |
| vnodes\_alive            | DOUBLE    |         | 当前集群 vnode 存活总数                          |
| connections\_total       | DOUBLE    |         | 当前集群连接总数                                 |
| topics\_total            | DOUBLE    |         | 当前集群 topic 总数                              |
| streams\_total           | DOUBLE    |         | 当前集群 stream 总数                             |
| grants_expire\_time      | DOUBLE    |         | 认证过期时间，企业版有效，社区版为 DOUBLE 最大值 |
| grants_timeseries\_used  | DOUBLE    |         | 已用测点数                                       |
| grants_timeseries\_total | DOUBLE    |         | 总测点数，开源版本为 DOUBLE 最大值               |
| cluster\_id              | VARCHAR   | TAG     | cluster id                                       |

### taosd\_vgroups\_info 表

`taosd_vgroups_info` 表记录虚拟节点组信息。

| field          | type      | is\_tag | comment                                        |
| :------------- | :-------- | :------ | :--------------------------------------------- |
| \_ts           | TIMESTAMP |         | timestamp                                      |
| tables\_num    | DOUBLE    |         | vgroup 中 table 数量                           |
| status         | DOUBLE    |         | vgroup 状态, 取值范围 unsynced = 0, ready = 1 |
| vgroup\_id     | VARCHAR   | TAG     | vgroup id                                      |
| database\_name | VARCHAR   | TAG     | vgroup 所属的 database 名字                    |
| cluster\_id    | VARCHAR   | TAG     | cluster id                                     |

### taosd\_dnodes\_info 表

`taosd_dnodes_info` 记录 dnode 信息。

| field             | type      | is\_tag | comment                                                                                           |
| :---------------- | :-------- | :------ | :------------------------------------------------------------------------------------------------ |
| \_ts              | TIMESTAMP |         | timestamp                                                                                         |
| uptime            | DOUBLE    |         | dnode uptime，单位：秒                                                                            |
| cpu\_engine       | DOUBLE    |         | taosd cpu 使用率，从 `/proc/<taosd_pid>/stat` 读取                                                |
| cpu\_system       | DOUBLE    |         | 服务器 cpu 使用率，从 `/proc/stat` 读取                                                           |
| cpu\_cores        | DOUBLE    |         | 服务器 cpu 核数                                                                                   |
| mem\_engine       | DOUBLE    |         | taosd 内存使用率，从 `/proc/<taosd_pid>/status` 读取                                              |
| mem\_free         | DOUBLE    |         | 服务器可用内存，单位 KB                                                                           |
| mem\_total        | DOUBLE    |         | 服务器内存总量，单位 KB                                                                           |
| disk\_used        | DOUBLE    |         | data dir 挂载的磁盘使用量，单位 bytes                                                             |
| disk\_total       | DOUBLE    |         | data dir 挂载的磁盘总容量，单位 bytes                                                             |
| system\_net\_in   | DOUBLE    |         | 网络吞吐率，从 `/proc/net/dev` 中读取的 received bytes。单位 byte/s                               |
| system\_net\_out  | DOUBLE    |         | 网络吞吐率，从 `/proc/net/dev` 中读取的 transmit bytes。单位 byte/s                               |
| io\_read          | DOUBLE    |         | io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 rchar 与上次数值计算之后，计算得到速度。单位 byte/s |
| io\_write         | DOUBLE    |         | io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 wchar 与上次数值计算之后，计算得到速度。单位 byte/s |
| io\_read\_disk    | DOUBLE    |         | 磁盘 io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 read_bytes。单位 byte/s                        |
| io\_write\_disk   | DOUBLE    |         | 磁盘 io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 write_bytes。单位 byte/s                       |
| vnodes\_num       | DOUBLE    |         | dnode 上 vnodes 数量                                                                              |
| masters           | DOUBLE    |         | dnode 上 master node 数量                                                                         |
| has\_mnode        | DOUBLE    |         | dnode 是否包含 mnode，取值范围 包含=1,不包含=0                                                   |
| has\_qnode        | DOUBLE    |         | dnode 是否包含 qnode，取值范围 包含=1,不包含=0                                                   |
| has\_snode        | DOUBLE    |         | dnode 是否包含 snode，取值范围 包含=1,不包含=0                                                   |
| has\_bnode        | DOUBLE    |         | dnode 是否包含 bnode，取值范围 包含=1,不包含=0                                                   |
| error\_log\_count | DOUBLE    |         | error 总数                                                                                        |
| info\_log\_count  | DOUBLE    |         | info 总数                                                                                         |
| debug\_log\_count | DOUBLE    |         | debug 总数                                                                                        |
| trace\_log\_count | DOUBLE    |         | trace 总数                                                                                        |
| dnode\_id         | VARCHAR   | TAG     | dnode id                                                                                          |
| dnode\_ep         | VARCHAR   | TAG     | dnode endpoint                                                                                    |
| cluster\_id       | VARCHAR   | TAG     | cluster id                                                                                        |

### taosd\_dnodes\_status 表

`taosd_dnodes_status` 表记录 dnode 状态信息。

| field       | type      | is\_tag | comment                                  |
| :---------- | :-------- | :------ | :--------------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                                |
| status      | DOUBLE    |         | dnode 状态,取值范围 ready=1，offline =0 |
| dnode\_id   | VARCHAR   | TAG     | dnode id                                 |
| dnode\_ep   | VARCHAR   | TAG     | dnode endpoint                           |
| cluster\_id | VARCHAR   | TAG     | cluster id                               |

### taosd\_dnodes\_log\_dir 表

`taosd_dnodes_log_dir` 表记录 log 目录信息。

| field       | type      | is\_tag | comment                             |
| :---------- | :-------- | :------ | :---------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                           |
| avail       | DOUBLE    |         | log 目录可用空间。单位 byte         |
| used        | DOUBLE    |         | log 目录已使用空间。单位 byte       |
| total       | DOUBLE    |         | log 目录空间。单位 byte             |
| name        | VARCHAR   | TAG     | log 目录名，一般为 `/var/log/taos/` |
| dnode\_id   | VARCHAR   | TAG     | dnode id                            |
| dnode\_ep   | VARCHAR   | TAG     | dnode endpoint                      |
| cluster\_id | VARCHAR   | TAG     | cluster id                          |

### taosd\_dnodes\_data\_dir 表

`taosd_dnodes_data_dir` 表记录 data 目录信息。

| field       | type      | is\_tag | comment                           |
| :---------- | :-------- | :------ | :-------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                         |
| avail       | DOUBLE    |         | data 目录可用空间。单位 byte      |
| used        | DOUBLE    |         | data 目录已使用空间。单位 byte    |
| total       | DOUBLE    |         | data 目录空间。单位 byte          |
| level       | VARCHAR   | TAG     | 0、1、2 多级存储级别              |
| name        | VARCHAR   | TAG     | data 目录，一般为 `/var/lib/taos` |
| dnode\_id   | VARCHAR   | TAG     | dnode id                          |
| dnode\_ep   | VARCHAR   | TAG     | dnode endpoint                    |
| cluster\_id | VARCHAR   | TAG     | cluster id                        |

### taosd\_mnodes\_info 表

`taosd_mnodes_info` 表记录 mnode 角色信息。

| field       | type      | is\_tag | comment                                                                                                  |
| :---------- | :-------- | :------ | :------------------------------------------------------------------------------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                                                                                                |
| role        | DOUBLE    |         | mnode 角色， 取值范围 offline = 0,follower = 100,candidate = 101,leader = 102,error = 103,learner = 104 |
| mnode\_id   | VARCHAR   | TAG     | master node id                                                                                           |
| mnode\_ep   | VARCHAR   | TAG     | master node endpoint                                                                                     |
| cluster\_id | VARCHAR   | TAG     | cluster id                                                                                               |

### taosd\_vnodes\_role 表

`taosd_vnodes_role` 表记录虚拟节点角色信息。

| field          | type      | is\_tag | comment                                                                                                 |
| :------------- | :-------- | :------ | :------------------------------------------------------------------------------------------------------ |
| \_ts           | TIMESTAMP |         | timestamp                                                                                               |
| vnode\_role    | DOUBLE    |         | vnode 角色，取值范围 offline = 0,follower = 100,candidate = 101,leader = 102,error = 103,learner = 104 |
| vgroup\_id     | VARCHAR   | TAG     | dnode id                                                                                                |
| dnode\_id      | VARCHAR   | TAG     | dnode id                                                                                                |
| database\_name | VARCHAR   | TAG     | vgroup 所属的 database 名字                                                                             |
| cluster\_id    | VARCHAR   | TAG     | cluster id                                                                                              |

### taosd\_sql\_req 表

`taosd_sql_req` 记录服务端 sql 请求信息。

| field       | type      | is\_tag | comment                                  |
| :---------- | :-------- | :------ | :--------------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                                |
| count       | DOUBLE    |         | sql 数量                                 |
| result      | VARCHAR   | TAG     | sql的执行结果，取值范围 Success, Failed |
| username    | VARCHAR   | TAG     | 执行sql的user name                       |
| sql\_type   | VARCHAR   | TAG     | sql类型，取值范围 inserted_rows         |
| dnode\_id   | VARCHAR   | TAG     | dnode id                                 |
| dnode\_ep   | VARCHAR   | TAG     | dnode endpoint                           |
| vgroup\_id  | VARCHAR   | TAG     | dnode id                                 |
| cluster\_id | VARCHAR   | TAG     | cluster id                               |

### taos\_sql\_req 表

`taos_sql_req` 记录客户端 sql 请求信息。

| field       | type      | is\_tag | comment                                   |
| :---------- | :-------- | :------ | :---------------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                                 |
| count       | DOUBLE    |         | sql 数量                                  |
| result      | VARCHAR   | TAG     | sql的执行结果，取值范围 Success, Failed  |
| username    | VARCHAR   | TAG     | 执行sql的user name                        |
| sql\_type   | VARCHAR   | TAG     | sql类型，取值范围 select, insert，delete |
| cluster\_id | VARCHAR   | TAG     | cluster id                                |

### taos\_slow\_sql 表

`taos_slow_sql` 记录客户端慢查询信息。

| field       | type      | is\_tag | comment                                               |
| :---------- | :-------- | :------ | :---------------------------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                                             |
| count       | DOUBLE    |         | sql 数量                                              |
| result      | VARCHAR   | TAG     | sql的执行结果，取值范围 Success, Failed              |
| username    | VARCHAR   | TAG     | 执行sql的user name                                    |
| duration    | VARCHAR   | TAG     | sql执行耗时，取值范围 3-10s,10-100s,100-1000s,1000s- |
| cluster\_id | VARCHAR   | TAG     | cluster id                                            |

## 日志相关

TDengine 通过日志文件记录系统运行状态，帮助用户监控系统运行情况，排查问题，这里主要介绍 taosc 和 taosd 两个系统日志的相关说明。

TDengine 的日志文件主要包括普通日志和慢日志两种类型。

1. 普通日志行为说明
    1. 同一台机器上可以起多个客户端进程，所以客户端日志命名方式为 taoslogX.Y，其中 X 为序号，为空或者 0 到 9，Y 为后缀 0 或者 1。
    2. 同一台机器上只能有一个服务端进程。所以服务端日志命名方式为 taosdlog.Y，其中 Y 为后缀， 0 或者 1。

       序号和后缀确定规则如下（假设日志路径为 /var/log/taos/）：
        1. 确定序号：使用 10 个序号作为日志命名方式，/var/log/taos/taoslog0.Y  -  /var/log/taos/taoslog9.Y，依次检测每个序号是否使用，找到第一个没使用的序号作为该进程的日志文件使用的序号。 如果 10 个序号都被进程使用，不使用序号，即 /var/log/taos/taoslog.Y，进程都往相同的文件里写（序号为空）。
        2. 确定后缀：0 或者 1。比如确定序号为 3，备选的日志文件名就为 /var/log/taos/taoslog3.0 /var/log/taos/taoslog3.1。如果两个文件都不存在用后缀 0，一个存在一个不存在，用存在的后缀。两个都存在，用修改时间最近的那个后缀。
    3. 如果日志文件超过配置的条数 numOfLogLines，会切换后缀名，继续写日志，比如/var/log/taos/taoslog3.0 写够了，切换到 /var/log/taos/taoslog3.1 继续写日志。/var/log/taos/taoslog3.0  会添加时间戳后缀重命名并压缩存储（异步线程操作）。
    4. 通过配置 logKeepDays 控制日志文件保存几天，几天之外的日志会被删除。比如配置为 1，则一天之前的日志会在新日志压缩存储时检测删除。不是自然天。

系统除了记录普通日志以外，对于执行时间超过配置时间的 SQL 语句，会被记录到慢日志中。慢日志文件主要用于分析系统性能，排查性能问题。

2. 慢日志行为说明
    1. 慢日志一方面会记录到本地慢日志文件中，另一方面会通过 taosAdapter 发送到 taosKeeper 进行结构化存储（需打开 monitorr 开关）。
    2. 慢日志文件存储规则为：
        1. 慢日志文件一天一个，如果当天没有慢日志，没有当天的文件。
        2. 文件名为 taosSlowLog.yyyy-mm-dd（taosSlowLog.2024-08-02），日志存储路径通过 logDir 配置。
        3. 多个客户端的日志存储在相应日志路径下的同一个 taosSlowLog.yyyy.mm.dd 文件里。
        4. 慢日志文件不自动删除，不压缩。
        5. 使用和普通日志文件相同的三个参数 logDir,  minimalLogDirGB,  asyncLog。另外两个参数 numOfLogLines，logKeepDays 不适用于慢日志。


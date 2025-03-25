---
sidebar_label: 体验 TDgpt
title: 体验 TDgpt
description: 通过公共数据库体验 TDgpt。
---

TDgpt 是与 TDengine 主进程 taosd 适配的外置式时序数据分析智能体，能够将时序数据分析服务无缝集成在 TDengine 的查询执行流程中。 TDgpt 是一个无状态的平台，其内置了经典的统计分析模型库 Statsmodel，内嵌了 torch/Keras 等机器/深度学习框架库，而且在云服务上 TDgpt 内置支持了涛思自研时序数据基础大模型（TDtsfm） 和 TimeMoE 时序基础模型。

云服务上的公共数据库“时序数据预测分析数据集”预先准备好了三个数据集，用于体验时序数据基础模型强大的预测能力和泛化能力。两个时序模型均为在该数据集上进行训练。内置的数据集包括：1）不同时间段的电力需求数据 electricity_demands；2）cpc 和 cpm 两个数据集是来源于 NAB 的公开数据集。

TDgpt 当前只集成了时序基础模型的预测能力，使用 SQL 语句可以轻松调用时序基础模型的预测能力，通过以下 SQL 语句即可调用涛思时序基础模型预测能力。

``` SQL
select forecast(val, 'algo=tdtsfm_1') from forecast.electricity_demand_sub;
```

用户可以将其中的参数 algo 的值替换为 “timemoe-fc” 即可调用 TimeMoE 时序基础模型进行预测。另外，用户可以执行“show anodes full;”去查询目前系统配置好的 forecast 数据库的模型和算法列表，然后相应替换 algo 参数值就可以使用这些模型和算法来进行电力时序数据预测。

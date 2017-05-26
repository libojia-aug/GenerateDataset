# GenerateDataset
用于教育科研目的，生成指定分布数据集的分布式工具
## config
/config/GenerateDatasetConfig.properties

###count

生成数据条数
###slices（保留）

分片数
###discontinuity.point

时间戳分割点
###distribution

时间戳分布比率
当：discontinuity.point=1475535256000,1475635256000,1475835256000

distribution=10,90

代表：

1475535256000至1475635256000占10%

1475635256000至1475835256000占90%

###accuracy（保留）
生成数精度

###output.path
输出数据路径

###test.output.path
输出测试数据路径

###output.file（保留）
输出数据文件名

###input.source_address_iplbs.file (保留)
IPlbs信息基础库地址

###input.source_address_iplbs_h.file
IPlbs信息高频库地址

###input.source_address_iplbs_l.file
IPlbs信息低频库地址

###input.source_address_iplbs_h.file.extract.factor（保留）

IPlbs信息生成高频集时的抽取因子，代表抽取基础库的百分之多少。
0.1代表10％。
###input.source_address_iplbs_l.file.extract.factor（保留）

IPlbs信息生成低频集时的抽取因子，代表抽取基础库的百分之多少。
0.1代表10％。
###input.source_address_iplbs_h.file.extract.count（保留）

IPlbs信息生成高频集时，从基础库中抽取的条数。
###input.source_address_iplbs_l.file.extract.count（保留）

IPlbs信息生成低频集时，从基础库中抽取的条数。
###source_address_iplbs.factor

高频集数据占生成数据的比例。
0.8代表80%
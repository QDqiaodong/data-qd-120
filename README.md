# data-qd-120 · 文本标注一致性分析

Pairwise categorical annotation agreement with missingness accounting。业务输入、计算和当前边界详见 [数据契约](docs/contract.md)。

## 安装与演示

Python >=3.11，运行和测试仅用标准库；先确认 `python3 --version`。
每仓建立独立环境，构建工具锁版本。首次安装无公共缓存时可能联网；业务运行不访问网络。

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
.venv/bin/python -m pip check
.venv/bin/label-agreement --source fixtures/input.json --output run/report.json
.venv/bin/python -m unittest discover -s tests -v
```

本地已有wheel时，安装构建工具可加 `--no-index --find-links <公共wheel缓存目录>`。
修改源码后重新安装。本仓不共享其他仓环境，不依赖Docker、外部账号或持续服务。

输入JSON最多8MB，拒绝重复键、非有限数值和未定义字段；源字节SHA256随结果保留。
输出是可独立读取的JSON；成功后原子替换，输入/计算失败退出2并保留旧报告。
输入和输出不得相同，输出符号链接拒绝。单操作者批处理，不承诺并发发布或断电恢复。
同输入重跑不累加。测试使用临时目录，运行产物在忽略的run/。

src/label_agreement/model.py 为业务模型，相关算法模块独立组织；common.py 提供受限JSON及报告发布，
cli.py 衔接数据链路。fixtures 为明确自制样例，代码与样例采用MIT许可。

## SQLite标注修订账本（v2）

在不改变默认CLI的前提下，cli.py 提供三个账本子命令（仅标准库，含sqlite3）。名录在初始化时
固定，批次以事务方式只追加导入，历史导出按截止批次重放有效票并复用原分析逻辑；协议细节见
[数据契约](docs/contract.md)。

```sh
# 名录文件仅含 items/annotators/labels；初始化后无票、无批次
.venv/bin/label-agreement ledger-init --db run/ledger.db --roster roster.json
# 批次含 batch_id 与有序 events；set 带合法 label，delete 禁止 label，revision 为正整数
.venv/bin/label-agreement ledger-import --db run/ledger.db --batch batch-001.json
# 同 batch_id 同内容幂等，不同内容（含事件顺序）拒绝；失败整批回滚，不占批序
# 导出最新状态，或用 --cutoff-batch-id 复现某截止批次，未来修订不会混入
.venv/bin/label-agreement ledger-export --db run/ledger.db --output run/history-001.json \
  --cutoff-batch-id batch-001
```

导出报告为 schema_version=2，带截止批次、提交顺序及每批源字节SHA256；原子替换发布，
失败保留旧报告，数据库可在新进程中重开继续导入或导出。

## 离线标注补采任务编排（v3）

`plan-tasks --source <源JSON> --strategy <策略JSON> --output <报告>` 读取v1格式源与补采策略，
只给缺口安排未标过且未禁配的人员，已有票计入目标、超额票不撤销，配额只统计新任务。可行性由
带上下界的二分可行流精确判定（不用贪心），并在可行方案中最小化人员新任务数的峰值；输入顺序
变化不改变任务单。无解时发布`status="infeasible"`状态报告（退出0、不含部分任务），附需求
缺口与可按[数据契约](docs/contract.md)重建网络核对的最小割证书；协议错误退出2并保留旧报告。

```sh
# 策略含 strategy_version=1、每item目标、每人新任务上下限、禁配 item/annotator 对
.venv/bin/label-agreement plan-tasks --source fixtures/input.json \
  --strategy fixtures/strategy.sample.json --output run/plan.json
```

成功报告为 schema_version=3，含有序任务单、人员负载、最优峰值及两份输入的原字节SHA256。

## 语料分片发布（v4 build-corpus）

`build-corpus --source <v1源JSON> --corpus <语料JSON> --seed <整数> --output-root <目录>` 把
标注源与`[{item,text,group}]`语料合成确定性`train/validation/test` JSONL。语料恰覆盖名录；
只纳入至少两票且有严格多数者，其余逐项记排除原因（全名录与排除项都进manifest）。分片以
“group相同或NFKC并折叠空白后的text相同”的传递闭包为整分量，按seed与分量最小item的SHA256
稳定映射80/10/10桶；编码与桶界随manifest写明。

版本按两份输入原字节与策略内容寻址，完整暂存验真后原子切换`CURRENT`；已有版本验真才复用，
损坏拒绝。重启清理遗留暂存；并发发布不会暴露半成品、误删活跃暂存或回滚他人发布，失败保留
上次完整版本。协议细节见 [数据契约](docs/contract.md)。

```sh
.venv/bin/label-agreement build-corpus --source fixtures/input.json \
  --corpus fixtures/corpus.sample.json --seed 5 --output-root run/corpus
```

```
run/corpus/
├── CURRENT                       # 当前版本id
└── versions/<id>/{train,validation,test}.jsonl + manifest.json
```

## 业务与样例结果

8项标注；A/B共同3项，Po=2/3、Pe=4/9、Kappa=2/5。I2平票、I4无票单列。
agreement.py计算精确两两指标，model.py构建覆盖与观察票多数；不把一致性当正确率。

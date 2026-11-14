# Annotation agreement v1

为文本数据集整理人员检查分类标注一致性与覆盖缺口。输入包含显式items、annotators、labels名录和annotations。
名录非空且唯一：items<=10000、annotators<=50、labels<=50，标识非空、无首尾空白/控制字符、<=128字符。
每行annotation恰有item/annotator/label，全部引用须存在；同一item/annotator只允许一个决定，重复即拒绝。
annotations<=20000。缺失用没有行表达，不是null标签，也不自动补negative。

每对标注者仅在两人都标过的item交集上计算混淆矩阵（行=left，列=right）、Po、Pe、Cohen Kappa。
Po=相同标签数/n，Pe=sum(行边际*列边际)/n²，Kappa=(Po-Pe)/(1-Pe)。
没有共同item则三项null；Pe=1时Kappa不可定义，输出constant_identical_marginals，不能强行给1。
Po/Pe/Kappa为精确分数文本。labels未出现的类别仍保留0格。不同pair使用各自交集，不能直接当同一总体比较。
不把多个pair Kappa简单平均，也不输出Fleiss Kappa、显著性或置信区间。

逐item列出投票计数、参与/缺失标注者。已观察票中某标签严格超过半数才有majority，
否则no_strict_majority且consensus=null；无票为unlabeled。一个人的票也可形成观察票多数，
这不表示达到质量门槛或完整覆盖。覆盖率分母是完整item名录。
样例8项决定，I2平票、I4无票；A/B共同3项，Po=2/3、Pe=4/9、Kappa=2/5。
A/C、B/C均共同2项且Kappa=1。没有真值，不把一致性说成正确率。

当前仅单标签名义分类，不支持跨度/序列标注、ordinal权重、多轮版本、裁决写回或在线标注界面。
原创合成标签不代表真实用户观点或自动分类模型能力。

# Annotation revision ledger v2（SQLite标注修订账本）

在v1分析逻辑之上增加只追加的修订账本，支持连续更正与按历史批次复现。仅使用Python标准库
（含sqlite3）。v1命令行（`--source/--output`）行为保持不变；账本能力通过子命令提供。

## 初始化与名录固定

`ledger-init --db <数据库> --roster <名录文件>` 接收仅含`items/annotators/labels`的JSON，
标识规则与v1完全一致（非空、无首尾空白/控制字符、<=128字符；items<=10000、annotators<=50、
labels<=50，各自非空且唯一）。初始化后账本内无任何票、无批次，名录写入独立表后不可原地改写；
数据库路径为符号链接或已存在时拒绝初始化。

## 批次与事件协议

`ledger-import --db <数据库> --batch <批次文件>` 导入一个批次。批次文件恰有：

- `batch_id`：非空标识，同v1标识规则。
- `events`：非空有序数组（<=100000），事件顺序是协议的一部分，参与规范化哈希与幂等比较。

每个事件恰有`item/annotator/revision/op`，`set`事件还必须带合法`label`；
`delete`事件禁止带`label`字段。`revision`为正整数。`item/annotator`必须引用固定名录，
`set`的`label`必须在label名录内，否则整批拒绝。

每个`(item,annotator)`键维护一个已收修订上界：事件revision必须严格大于该键此前已接收的
最大revision，包括同批次内更早事件、跨批次历史，以及撤回事件本身的revision。因此同批次内可
连续修订（revision递增），而重放旧revision一律拒绝。`delete`一张当前不存在的票同样记账，
并保留该键的版本上界；它不产生有效票，但会挡住更小或相等的旧revision。

## 事务、批序与幂等

每个批次在单个SQLite事务（`BEGIN IMMEDIATE`）内完成：写入批次行、全部事件行并更新各键版本
上界，然后提交。校验或写入失败时回滚，不留下任何事件、不占用批次序号（AUTOINCREMENT在回滚
后复用，批序无空洞），数据库恢复到导入前状态。

同`batch_id`重复提交时，按事件有序内容的规范化SHA256比较：内容完全相同则幂等返回，不重复
写入、不新增批序；内容不同（含事件顺序不同、字段不同）则拒绝。批次同时记录批次源文件的原始
字节SHA256。

## 历史导出与复现

`ledger-export --db <数据库> --output <报告> [--cutoff-batch-id <批次>]` 按提交顺序重放
截止批次（含）之前的事件，截止批次之后的未来修订绝不混入；不给cutoff时取最新已提交批次，
无批次时拒绝导出。重放把`set`记为当前票、`delete`移除当前票（含撤回不存在票的空操作），
由截止时点的有效票构造v1输入并调用原有`analyze`分析逻辑，覆盖率、两两Kappa、多数判定等
口径与v1完全一致。

报告（schema_version=2）含：截止批次id与序号、按提交顺序排列的批次清单（每批含序号、
batch_id、批次源字节SHA256、提交时间），以及v1完整`result`。报告经临时文件原子替换发布，
导出失败保留旧报告；输出路径为符号链接或与数据库相同均拒绝。导出以只读模式打开数据库，
可在新进程中重开已有数据库继续导入或导出。

## 不可变性与边界

名录固定、事件只追加，不提供任何原地改写或删除历史的入口；历史报告与当前报告可因后续更正而
分歧，这是预期行为。账本JSON同样走受限解析：<=8MB、拒绝重复JSON键与非有限数值。
账本不承诺多进程并发写入或断电恢复，适用单操作者批处理边界。

# 离线标注补采任务编排 v3（plan-tasks）

在不改变v1默认CLI与账本命令的前提下新增`plan-tasks`子命令，读取现有格式源JSON（v1：
items/annotators/labels/annotations）与策略JSON，仅为缺口安排**新任务**。仅使用Python标准库；
两份输入都走`common.read`严格解析（<=8MB、拒绝重复JSON键与非有限数值），报告经`common.save`
临时文件原子替换发布，失败保留旧报告。

## 策略协议（strategy_version=1）

策略JSON恰有四个顶层字段：

- `strategy_version`：整数，当前固定为1；其他值整文件拒绝。
- `targets`：非空有序数组，长度<=10000，每个名录item恰好一行。每行恰有`item`（合法标识且
  引用源名录）与`target`（整数，0..10^9），重复item行即拒绝。
- `annotators`：非空有序数组，长度<=50，每个名录annotator恰好一行。每行恰有`annotator`、
  `min_new_tasks`、`max_new_tasks`，三个整数均在0..10^9；`min_new_tasks>max_new_tasks`为
  矛盾边界，拒绝；重复annotator行拒绝。
- `forbiddens`：数组（<=100000），每行恰有`item`与`annotator`，二者均须引用源名录；
  重复禁配对拒绝。

标识规则与v1完全一致（非空、无首尾空白/控制字符、<=128字符）。源JSON自身仍按v1严格校验：
重复item/annotator决定、未知引用、未知label均退出2。策略中任何未知item/annotator引用、
targets或annotators未完整覆盖名录、非整数或越界值，同样退出2且不写报告。

## 缺口、配额与配对规则

- 源中每个有效`(item,annotator)`决定计为一张已有票；item的已有票数为持票annotator集合大小。
- item缺口`demand=max(0,target-已有票数)`。已有票超过目标的item不撤销任何票，缺口为0。
- 可配人员：该item未持票且不在禁配对中的annotator。已持票者绝不重复安排，禁配对绝不安排，
  每个`(item,annotator)`对在新任务单中最多出现一次。
- annotator上下限只统计本轮新任务数，与已有票数无关。任何可行方案必须同时满足全部item缺口、
  全部annotator上下限；求解器不得用贪心失败代替无解判定。
- 目标：在所有可行方案中最小化单个annotator承担新任务数的最大值（最优峰值）。

## 可行流网络与最优峰值（不枚举分配）

可行性由带上下界的二分匹配流精确判定，不枚举任何完整分配方案。对候选峰值`p`（每个
annotator有效新任务上限取`min(max_new_tasks,p)`）构造网络：

- 节点：源S、汇T、每个annotator一个节点、每个item一个节点，以及变换用超源SS、超汇TT。
- 边S→annotator：下界`min_new_tasks`，容量`min(max_new_tasks,p)`。
- 边annotator→item：仅对可配对建边，下界0，容量1（天然保证每对至多一次）。
- 边item→T：下界与容量都等于该item的`demand`（缺口必须恰好满足）。
- 边T→S：容量取所有下界与缺口之和加1的充分大值，形成环流。

上下界变换：对每条边u→v（下界l、容量c），残量容量取c-l；维护节点平衡
`balance[v]+=l, balance[u]-=l`。变换后为`balance>0`的节点加SS→node边（容量balance），
为`balance<0`的节点加node→TT边（容量-balance）。对SS/TT跑Dinic最大流：当且仅当最大流等于
SS发出容量总和时，候选峰值可行。逻辑边的实际流量为`l+(c-l-残量容量)`，
annotator→item上流量1即一条新任务。

峰值二分区间为`[max(min_new_tasks), max(max_new_tasks)]`：区间上界先做一次完整可行性判定；
不可行则整个实例无解，发布无解状态报告。可行时二分得到最小可行峰值，并在该峰值重建网络抽取
任务单。因此调用次数为O(log A)次最大流，A为annotator数（<=50）；网络规模
O(items+annotators+可配对)，支持千项规模。

## 无解状态报告与最小割证书

无解时正常发布报告（CLI退出0），`status="infeasible"`，不含任何部分任务（无tasks字段）。
报告含：

- `demand_summary`：总缺口、annotator下限之和、上限之和、尝试的峰值（即上界）。
- `demand_gaps`：可配人数小于缺口的item清单，逐项给出目标、已有票、缺口、排序后的可配
  annotator名单与短缺量，便于人工定位局部容量不足。
- `mincut_certificate`：可按上文规则独立重建网络核对的割证书：
  - `required_flow`/`achieved_flow`：SS要求的总流量与实际最大流，后者严格小于前者。
  - `reachable_nodes`：在SS残量网络（仅走残量容量>0的边）上可达的全部节点，规范节点名为
    `S`、`T`、`SS`、`TT`、`annotator:<id>`、`item:<id>`，按字典序排列。
  - `cut_edges`：从可达侧跨到不可达侧的协议边，每条带kind、from、to、下界、容量、实际流量。
  - `auxiliary_cut_edges`：同样跨割的SS/TT/T→S辅助边及其流量。
  - 核对方式：重建同构网络并复算最大流后，对SS做残量BFS，所得割集的跨边容量总和等于
    achieved_flow与required_flow之差；任何跨割边均满足from可达、to不可达。

## 成功报告（schema_version=3）

`status="planned"`，含两份输入的**原字节**SHA256（`source_sha256`、`strategy_sha256`）、
`optimal_peak`、`totals`（demand与new_tasks）、`tasks`有序任务单（按item再annotator字典序，
每行恰有item/annotator）、`annotator_loads`（每人已有票数、新任务数、上下限，按annotator
排序）、`item_coverage`（每项目标、已有票、缺口、实际安排，按item排序）。

确定性：所有集合遍历先排序，解只取决于输入内容；输入数组顺序（items/annotators/
annotations/targets/annotators策略行/forbiddens的排列）变化不改变任务单与峰值。
报告为可独立读取的JSON（sort_keys、allow_nan=False）。输出路径不得与任一输入相同，
符号链接拒绝；同输入重跑产生字节相同的报告。

## 边界与不做的事

不撤销或改写已有票，不在线下发任务、不回写账本，不输出枚举的全部可行分配，不做加权或优先级
目标（仅最小峰值），不支持名录外人员。单操作者批处理边界，不承诺并发发布或断电恢复。



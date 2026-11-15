# Annotation agreement v1

为文本数据集整理人员检查分类标注一致性与覆盖缺口。输入包含显式items、annotators、labels名录和annotations。
名录非空且唯一：items<=1,000,000、annotators<=50、labels<=50，标识非空、无首尾空白/控制字符、<=128字符。
每行annotation恰有item/annotator/label，全部引用须存在；同一item/annotator只允许一个决定，重复即拒绝。
annotations<=20,000,000。缺失用没有行表达，不是null标签，也不自动补negative。
默认v1合法输入上限为2 GiB（2^31字节，恰等2 GiB接受，超出即拒绝）；顶层字段与各数组可任意排序，
annotations允许先于items/annotators/labels出现，引用与重复决定在校验阶段统一判定。

## 流式解析、临时SQLite与内存边界（v1默认CLI）

默认CLI（`--source/--output`）以流式管线处理v1源，独立CLI进程全程RSS<=128 MiB（含stdout）：

- 解析：`tokens.py`提供缓冲式严格JSON分词器，按固定块（64 KiB）读取，单字符串/数字令牌有界，
  永不整文件入内存。语法与`common.read`的标准库严格解码一致：拒绝`NaN/Infinity`、非有限数值、
  UTF-8 BOM、文档根值之后的任何尾随字节（含空白）；重复JSON键在分层解析器的每个对象上拒绝。
  源SHA-256随读随算，按**原始字节**计算，与历史口径完全一致。
- 暂存：名录与票写入输出目录下的私有临时SQLite（`PRAGMA cache_size`约2 MiB、临时排序落盘、
  可处置日志在内存），不把整名录、整票表或完整报告放进Python对象。annotators/labels至多50个
  可驻留小集合；items走SQLite主键去重，票先入无约束暂存表，文档解析完成后再用SQL统一校验
  未知item/annotator/label引用与重复`(item,annotator)`决定，因此“票先于名录”同样可校验。
- 统计：覆盖率、逐item投票与两两混淆矩阵全部由SQL聚合/有序游标产出；任意时刻驻留状态以50人、
  50标签为界。Po/Pe/Kappa仍用`Fraction`精确分数文本，labels未出现的类别保留0格；
  Pe=1时Kappa输出`constant_identical_marginals`，无共同item时三项null，口径不变。
- 报告：结构化流式写入器（`streaming.JsonWriter`）直接把报告写入私有暂存文件，输出字节与
  `model.analyze`+`common.save`逐字节相同（sort_keys、indent=2、结尾一个LF），完整报告不入内存。
- 发布：成功后`os.replace`原子替换；输入/输出同路径、输出符号链接仍拒绝。任何解析、校验、
  暂存或输出写失败都保留旧报告，并清理临时SQLite与暂存文件。stdout只打印一行简短JSON摘要，
  不回显报告正文。

库入口`model.analyze(data)`（接收已解析dict）与其他子命令（账本、plan-tasks、build-corpus）
的API与行为保持不变；8 MiB的`common.read`仍服务于这些既有路径。v1默认CLI改为流式管线，
但报告schema（schema_version=1）、字段、排序、分数与哈希口径全部不变。

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

## 资源测量方法

资源测试（`tests/test_scale.py`，需`RUN_SCALE_TESTS=1`）构造恰2 GiB、含至少200万票的v1源，
在Linux上以**独立CLI进程**运行并测量：

- 峰值RSS：`subprocess`启动真实CLI并等待退出后，读取
  `resource.getrusage(RUSAGE_CHILDREN).ru_maxrss`（Linux单位KiB）；每次测量用全新harness进程、
  仅一个子进程，因此读数即该子进程峰值，不是累计值，预算128 MiB。
- 不以退出码代替正确性：独立流式校验器（`tests/tools/streaming_validate.py`）在第二个独立进程中
  重新按原始字节计算SHA-256、用自己的SQLite重算全部计数与统计，并以令牌流方式通读完整报告
  （报告同样不整载入内存），逐项核对覆盖率、Po/Pe/Kappa、混淆零格、逐item投票、排序与源哈希；
  任一不符即非零退出。超2 GiB源必须以退出2拒绝且不产生输出、不留暂存。


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

# 语料分片构建 v4（build-corpus）

在不改变v1默认CLI、账本与`plan-tasks`的前提下新增`build-corpus`子命令：把v1源JSON与语料JSON
合成按item排序的`train/validation/test` JSONL，并以内容寻址的版本目录原子发布。仅使用Python
标准库；源JSON仍走`common.read`（<=8MB、严格解析），语料JSON走同样的严格规则（拒绝重复键、
非有限数值、非UTF-8），上限放宽到64MB（文本体量天然大于v1源）。

```sh
.venv/bin/label-agreement build-corpus --source fixtures/input.json \
  --corpus fixtures/corpus.sample.json --seed 5 --output-root run/corpus
```

`--seed`是命令行文本参数：可选负号加十进制数字，禁止前导零（`0`除外）与前缀`+`，数字位数
<=4000，避免把无界输入带进哈希材料。库入口`build_corpus(source, corpus, seed:int, root)`
只接受真正的`int`。

## 语料协议

语料JSON是数组，每行恰有`item`、`text`、`group`：

- `item`：非空标识，规则与v1一致（<=128字符、无首尾空白/控制字符），在数组内唯一，且必须
  引用源名录；数组必须恰好覆盖源名录全部items（不多、不少、不重复）。
- `text`、`group`：非空字符串；`text`必须能按UTF-8编码。仅由空白组成的`text`不算空串，
  仍被接受，但规范化后为空串并参与分量合并（这是规范化定义的直接结果）。

任何一行不合规、缺失/多余item、未知item、重复item或重复JSON键，整份语料拒绝，退出2，不写
任何发布产物。

## 纳入与排除

逐item读取v1分析结果中的观察票：

- 观察票数为0：排除，`no_votes`。
- 观察票数为1：排除，`fewer_than_two_votes`。
- 观察票数>=2但没有任何标签严格超过半数（含平票与1-1-1）：排除，`no_strict_majority`。
- 否则纳入，标签取该严格多数标签（`consensus`）。

“严格多数”沿用v1口径`count*2>n`。纳入不需标注者数量门槛、不做质量加权。名录中每个item都出现
在manifest的`items`表里：纳入项带`label`与`included=true`，排除项带`exclusion_reason`与
`included=false`，并另列于`exclusions`（含item、观察票数、原因、所属分量与分片）。

## 分量与80/10/10分片

关系“group相同（原字节精确相等）或规范化text相同”按并查集求**传递闭包**；每个连通分量整体
进入同一个分片，因此一个分量内的纳入项与排除项不会跨分片。

text规范化：先做Unicode NFKC，再按Unicode空白切分并以单个ASCII空格连接（折叠空白、去首尾
空白）。NFKC不折叠大小写。

分量以字典序最小item为键，稳定映射到分片。编码与桶界（同时写入manifest的`policy`）：

- 材料：UTF-8字节`b"build-corpus/1"`、NUL、seed的ASCII十进制（库内为标准十进制，负号保留）、
  NUL、最小item的UTF-8字节。NUL分隔使`seed=12,item="3"`与`seed=1,item="23"`不会碰撞。
- `v`：`SHA-256(材料)`前8字节按无符号大端整数，范围`[0, 2^64)`。
- `train`：`v*100 < 80*2^64`；`validation`：`80*2^64 <= v*100 < 90*2^64`；`test`：
  `v*100 >= 90*2^64`。
- 整数比较避免浮点边界误差；空分量文件合法（0行，`sha256`为空串哈希）。

分片归属只取决于seed与分量最小item，输入数组顺序变化不改变归属。

## 输出与manifest

每个版本目录含四个文件：`train.jsonl`、`validation.jsonl`、`test.jsonl`、`manifest.json`。

JSONL：UTF-8，每行恰有`item`/`text`/`label`，键按字典序、紧凑分隔符，行按item码点升序，
每行一个LF且文件以LF结尾。`text`保留语料原始字节内容，不做规范化改写。

`manifest.json`（`schema_version=4`、`command="build-corpus"`）按`sort_keys`、`indent=2`
写出一行结尾LF，包含：

- `version`：内容寻址id（见下）、`seed`、`source_sha256`、`corpus_sha256`（均为**原字节**SHA256）。
- `policy`：完整策略载荷（版本化规则、规范化定义、split编码与桶界、输出编码、版本规则），参与
  版本id计算，因此策略内容变化必然换版本。
- `counts`：名录总数、纳入/排除数、分量数、各分片行数。
- `files`：每个分片的`path`、`sha256`、`line_count`。
- `components`：按分量序号，含`minimum_item`、`split`、`items`、`included_items`、
  `excluded_items`。
- `items`：按item排序，每行含`vote_count`、`label`（排除为null）、`included`、
  `exclusion_reason`（纳入为null）、`component`、`component_minimum_item`、`split`。
- `exclusions`：全部排除项及原因。

## 版本、复用与原子发布

版本id = `SHA-256(canonical JSON {'policy','source_sha256','corpus_sha256'})`，规范JSON为
`sort_keys`、紧凑分隔符、UTF-8。同输入字节与同策略必须同id；源或语料任一字节、seed或策略
载荷变化都换id。

发布根目录布局：`versions/<id>/`、`staging/`（私有暂存）、`CURRENT`（内容为版本id加一个LF）、
`.build-corpus.lock`（`flock`排他锁）。流程：

1. 解析两份输入并计算全部字节（不触碰发布根）。
2. 在锁内清理遗留`stage-*`与`.CURRENT-*`临时文件。
3. 目标版本已存在：逐字节验真后复用，不重写文件。验真包括目录名=manifest.version=由内容
   重算的id、恰好四个文件、每文件SHA256/行数与行内容、排序/唯一性、与manifest纳入清单一致、
   计数一致；任一项失败即拒绝（不“修复”），退出2。
4. 目标不存在：写入`staging/stage-*`，写完fsync并验真，再`os.rename`到`versions/<id>`
   （同一文件系统内的原子改名），fsync目录，最后以临时文件+`os.replace`原子切换`CURRENT`。

锁覆盖从暂存清理到`CURRENT`切换的全过程，因此两个发布进程交错时：不会暴露半成品（`CURRENT`
只会指向已完整落盘且验真的版本）、不会误删别人的活跃暂存、不会把他人已完成的发布回滚为旧指针。
若在写文件、落manifest或切指针前失败/崩溃，`CURRENT`仍指向上次完整版本（或首次构建时不存在），
重启后清除暂存并在下一次成功构建时恢复；已落盘的完整版本可直接复用。

## 确定性、失败与边界

同输入字节、同seed、同策略重跑，版本目录字节与`CURRENT`一致；置换源/语料数组顺序不改变item
归属与分片文件内容，但会改变输入字节哈希与版本id（这是内容寻址的预期行为）。失败退出2，保留
上次完整发布；成功后不累加。输出根、`versions`、`staging`、`CURRENT`与锁文件均拒绝符号链接。

不做：不去重文本、不改写语料文本、不自动扩部分量、不做分层抽样比例外的重分配、不训练模型；
不承诺跨文件系统原子改名（`versions`与`staging`必须同一根目录内），不提供断点续传。




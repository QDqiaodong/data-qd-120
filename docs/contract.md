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


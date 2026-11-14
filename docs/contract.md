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

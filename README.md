# graph-influence-go

本地运行的图影响传播模拟与种子集合选取库。无任何外部服务依赖：所有蒙特卡洛模拟、
估计、选种与持久化都在进程内完成。

## 快速使用

```go
svc, _ := influence.NewService("./data")
defer svc.Close()

svc.AddNode("a")
svc.AddNode("b")
svc.AddEdge("a", "b", 0.4)

id, _ := svc.SubmitEstimation(influence.EstimateConfig{
    Model:      influence.ModelConfig{Model: influence.ModelIndependentCascade, UseEdgeWeights: true},
    Seeds:      []influence.NodeID{"a"},
    Rounds:     10000,
    BatchSize:  100,
    Seed:       42,
    Confidence: 0.95,
})
state, _ := svc.WaitTask(context.Background(), id)
fmt.Println(state.Estimate.Mean, state.Estimate.MarginOfError)
```

进程重启后，`NewService` 恢复图与任务记录；调用 `ResumePending()` 继续尚未完成
的任务。已完成、失败或已取消的任务不会被自动重启。

## 传播模型

### 独立级联（Independent Cascade）

- 每个节点被激活时，对每条出边尝试激活其邻居，且**只尝试一次**。
- 尝试成功的概率即该边的传播概率：
  - `UseEdgeWeights=true`：使用边权重（需在 `[0,1]`）；
  - `UseEdgeWeights=false`：所有边统一使用 `DefaultProbability`。
- 传播按波次进行，直到没有新节点被激活。结果包含初始种子。

### 线性阈值（Linear Threshold）

- 每个节点有一个阈值；当所有**已激活入邻居**的权重之和达到阈值时，节点在本轮激活。
- 阈值来源：
  - `Thresholds[node]` 显式指定时使用该固定阈值（需在 `[0,1]`）；
  - 未指定的节点在每轮模拟开始时独立抽取一个 `[0,1]` 均匀随机阈值。
- 同步迭代直到达到不动点。边权重为影响权重，节点所有权重之和是否归一化由调用方负责。

## 参数

| 参数 | 含义 |
| --- | --- |
| `Rounds` | 蒙特卡洛模拟轮数，必须为正。 |
| `BatchSize` | 每个持久化批次包含的轮数，必须为正。 |
| `Seed` | 主随机种子，`int64`。 |
| `Confidence` | 置信区间置信度，取开区间 `(0,1)`，如 `0.95`。 |
| `K`（选种） | 预算：要选取的种子数量。 |
| `Candidates` | 候选节点集合；为空使用全部节点。 |

估计结果 `EstimateResult` 给出样本均值 `Mean`、样本标准差 `StdDev`、正态近似的
误差边界 `MarginOfError` 以及 `LowerBound` / `UpperBound`。

## 确定性保证

相同图、相同配置（含 `Seed`）下，无论是否并发、无论进程重启多少次，结果**逐位一致**：

- 每个传播决策（一条边的激活尝试、一个节点的阈值）都由 `(主种子, 批次, 轮次,
  边端点/节点)` 通过 SHA-256 域分离派生独立密钥，再由 ChaCha8 产生随机数；
- 随机数不依赖节点被遍历到的先后，也不依赖其他节点是否激活；
- 种子集合与候选集合都会去重并排序，节点遍历顺序固定为排序后的邻接表；
- 批次在任务内全局编号，不同估计规格（模型 + 种子集合 + 阈值覆盖）永不共享随机流；
  相同规格的批次会被 CELF 选种复用且不重复计算。

## 种子选取

使用 CELF（Cost-Effective Lazy Forward）懒贪心：依据每次选择的**模拟边际增益**
`f(S∪{v}) − f(S)` 排序，边际增益完全由配置模型的蒙特卡洛估计给出，不使用度、
中心性等任何静态图指标。贪心理论保证对影响力最大化目标函数的 `(1−1/e)` 近似。
候选按 ID 排序并使用确定性的平局规则。

## 持久化与崩溃恢复

- 所有写入采用「临时文件 + fsync + 原子 rename」，崩溃只会看到完整文件或旧文件，
  不会出现半写入。
- 图保存在 `graphs/current.json`，任务保存在 `tasks/<id>.json`。
- 每完成一个模拟批次就原子落盘一次，记录 `Sum` 与 `SumSq` 等聚合量。
- 恢复时已完成的批次从磁盘加载并直接复用，**不会重复计算**；任务从下一个批次继续。
- 用户显式取消的任务在重启后保持 `cancelled`；进程关闭（模拟崩溃）则把任务保留为
  非终态，等待 `ResumePending()` 继续。取消不会写入部分估计/选种结果。

## 图更新语义

在线增删节点/边、修改权重会使图版本号单调递增并计算新的内容哈希。正在运行的任务
固定（pin）提交时的不可变快照，因此并发更新不会影响其结果；任务状态记录所基于的
`graph_version` 与 `graph_hash`，结果与图版本的对应关系可解释。新提交的任务使用
最新图；旧图结论不会被静默当作新图结论（不同图哈希）。

## 非法输入

错误均为可区分的哨兵错误（可用 `errors.Is` 判断），并尽量附带 `NodeError` /
`EdgeError` / `ValueError` 上下文：

- 空图、空种子集；
- 传播概率或阈值越界；
- 边引用不存在的端点；
- 自环、重复节点/边、删除不存在的节点/边；
- 轮数、批大小、置信度、选种数量非法；候选引用不存在节点；
- 选种数量超过候选数量。

## 资源边界

- 模拟只保留集合与邻接遍历状态，批次只保留聚合和，运行内存不随 `Rounds` 增长；
- 任务通过 `context` 在批次与 CELF 评估边界检查取消；
- `Close()` 取消所有运行中任务并等待协程退出。

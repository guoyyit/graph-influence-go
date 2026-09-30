# graph-influence-go

本仓库在本地实现带权有向图上的**影响传播模拟**、**影响力估计**与**预算约束种子选取**。
全部能力在进程内与本地文件目录中完成，不依赖任何外部服务。

## 模型语义

### 独立级联模型（Independent Cascade, IC）

- 每个节点只有「未激活 / 已激活」两种状态，初始只有种子集合激活。
- 一个在第 t 轮新激活的节点 u，对每条出边 u→v（v 尚未激活）获得**恰好一次**尝试机会，
  成功概率等于该边权重 `weight(u,v) ∈ [0,1]`。
- 成功则 v 在第 t+1 轮激活；失败的边不会重试。
- 没有新节点激活时传播终止。

### 线性阈值模型（Linear Threshold, LT）

- 传播开始前，每个节点 v 采样一个均匀随机阈值 `θ(v) ∈ [0,1)`；
  也可通过 `FixedThreshold` 为所有节点指定 `(0,1]` 内的固定阈值。
- 节点 v 的入边权重之和视为对它的「影响压力」。当
  `Σ_{u 已激活} weight(u,v) ≥ θ(v)` 时，v 激活。
- 激活逐轮扩散，直到没有节点再越过阈值。

> 两种模型都要求边权重在 `[0,1]`。对 LT，建议每个节点入边权重之和不超过 1。

## 参数

`ModelConfig`：

| 字段 | 含义 |
| --- | --- |
| `Model` | `independent_cascade` 或 `linear_threshold` |
| `Rounds` | 每个蒙特卡洛批次内独立模拟的次数，必须为正 |
| `Seed` | 随机流种子，int64 |
| `FixedThreshold` | LT 专用；为 0 时每节点均匀采样阈值 |

`EstimateRequest`：

- `Batches`：蒙特卡洛批次数（每批跑 `Rounds` 次模拟），必须为正；总样本数为 `Batches × Rounds`。
- `Confidence`：置信水平 `(0,1)`，默认 `0.95`。
- `Workers`：批次并发度；`<=0` 串行。
- `Store`：可选的批次缓存（见下）。
- `Cancel` / `Progress`：协作式取消与进度回调。

`SelectRequest` 额外包含 `Budget`（要选的种子数）、可选 `Candidate`（默认全部节点）。

## 确定性保证

在**相同图数据、相同种子集合、相同配置**下，结果逐位一致，且：

- 与进程重启无关；
- 与种子集合给出的顺序无关（内部规范化排序）；
- 与批次并发度、goroutine 调度无关；
- 与多个估计任务是否并发执行无关。

实现方式：每个 `(batchIndex, roundIndex)` 的随机流都由
`(Seed, 规范化种子集合, 模型, 固定阈值, 批次号, 轮号)` 经确定性哈希派生，
worker 之间不共享、不交错随机源；聚合顺序一律按节点 ID 规范化。

## 影响力估计

`Estimate` 返回期望受影响节点数的样本均值、标准差，以及基于
t 分布（大样本时退化为正态近似）的双侧误差边界 `MarginOfError`：

```go
res, err := influence.Estimate(influence.EstimateRequest{
    Graph:   g,
    Seeds:   []influence.NodeID{"a"},
    Config:  influence.ModelConfig{Model: influence.IndependentCascade, Rounds: 50, Seed: 42},
    Batches: 10,              // 500 次模拟
    Workers: 4,
})
// res.Mean, res.StdDev, res.Confidence, res.MarginOfError
```

## 种子选取

`Select` 采用**纯模拟驱动的贪心边际增益**：每轮对每个候选节点，以
「当前已选种子 + 该候选」跑一次影响力估计，选择边际增益最大的候选；
不使用度数、中心性等任何静态图指标。候选按 ID 排序，平局取字典序最小者，
因此并发下结果仍然确定。

```go
res, err := influence.Select(influence.SelectRequest{
    Graph:   g,
    Config:  influence.ModelConfig{Model: influence.LinearThreshold, Rounds: 30, Seed: 7},
    Batches: 5,
    Budget:  3,
})
// res.Seeds, res.Gains, res.ExpectedSpread
```

复杂度为 `O(Budget × 候选数 × Batches × Rounds)` 次模拟。已完成的
`(版本, 种子集合, 配置, 批次)` 会被缓存复用；候选规模较大时可通过
降低 `Rounds/Batches`、限制 `Candidate` 或使用更高并发来控制本地耗时。

## 持久化与崩溃恢复

`FileStore` 将数据写入本地目录，所有写操作都是「临时文件 + fsync + 原子 rename」，
崩溃只会留下上一个完整版本或新完整版本，不会出现半写文件。

```
<dir>/
├── graph.json        # 当前图快照（含版本号）
├── batches/          # 每个完成的蒙特卡洛批次一个 JSON
└── tasks/            # 每个异步任务一个 JSON（状态/进度/结果）
```

`Service` 负责异步任务：

1. `NewService(store, WithWorkers(n))` + `Start(ctx)` 启动 worker 并执行 `Resume`。
2. 提交任务后状态经历 `pending → running → completed/failed/canceled`，
   每完成一个批次就持久化进度。
3. **崩溃恢复**：进程重启后再次 `Start` 时，所有处于 `pending/running` 的任务
   会被重新置为 `pending` 并继续；`batches/` 中已完成的批次按键直接复用，
   **不会重复计算**。
4. **取消**：`CancelTask` 协作式停止长任务；任务标记为 `canceled`，
   不写任何结果载荷（已完成的批次仍可被其他任务复用），恢复时不会复活。
5. `Close` 优雅停机：执行中的任务回到 `pending`，下次启动继续。

## 图更新与缓存失效

图是**不可变快照**。`GraphBuilder` 的每次增删节点、增删边、改权重都会
克隆出一个新版本号，旧快照保持不变。已经提交的任务在其载荷中固定了
提交时的图快照，因此与后续更新天然隔离、结果可解释。

`BatchKey` 以 `GraphVersion` 为前缀：图更新后新任务使用新版本号，
**不可能命中旧版本的批次缓存**——这就是缓存失效规则。`Service.UpdateGraph`
持久化新快照；正在运行的旧任务不受影响，新提交的任务基于新图重新模拟。

## 非法输入与错误码

所有非法输入都通过 `*GraphError` 显式拒绝，可用 `influence.ErrorCodeOf(err)`
区分，绝不静默产生错误结论：

| 错误码 | 触发情形 |
| --- | --- |
| `empty_graph` | 空图上模拟/估计/选取 |
| `self_loop` | 边的起点等于终点 |
| `invalid_weight` | 权重不在 `[0,1]` |
| `invalid_threshold` | 阈值/置信度越界 |
| `invalid_rounds` / `invalid_batches` | 轮数或批次数非正 |
| `missing_endpoint` | 边引用了不存在的节点 |
| `unknown_node` | 种子/候选不存在、空种子集 |
| `duplicate_edge` | 重复节点或重复边 |
| `invalid_budget` | 预算非正或超过候选数 |
| `unknown_model` | 未知传播模型 |
| `task_not_found` / `task_canceled` / `task_not_running` | 任务生命周期错误 |

## 资源边界

- 批次并发由 `Workers` 限制，服务级任务并发由 `WithWorkers` 限制。
- 取消在批次边界协作生效，内存中不存在随时间无界增长的结构。
- 所有中间聚合只保留标量（和、平方和），不保留每次模拟的明细。

## 测试

```sh
go test ./... -race -count=1
```

覆盖：确定性（串行/并发/重启一致）、多任务并发、崩溃恢复、
图更新后的缓存失效、取消不留部分结果，以及全部非法输入分支。

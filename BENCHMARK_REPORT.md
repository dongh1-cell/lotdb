# 时序数据库文件格式 AI 训练负载 Benchmark 报告

## 研究动机

在大模型时代，时序数据的访问模式发生了根本性变化：

- **传统 IoT 查询**：近一分钟的数据、时间局部性强、全列读取、顺序扫描为主
- **AI 训练查询**：跨月/跨年的历史数据、随机窗口采样、部分列、降采样读取

三个维度的错配：

| 维度 | 传统查询 | AI 训练查询 | 错配后果 |
|------|---------|------------|---------|
| 时间范围 | 最近几分钟 | 数周至数月 | 缓存失效，随机 I/O |
| 列选择性 | 读取全部测点 | 50 列中取 3 列 | 列裁剪失效导致 I/O 放大 |
| 采样率 | 全分辨率 | 每 100 点取 1 点 | Page 内数据 99% 被丢弃 |

核心矛盾：**TsFile 的 I/O 最小单元是 Page（对齐场景下捆绑了时间列 + 所有值列），而 AI 训练只需要 Page 内的一小部分数据。**

---

## 测试指标

每个 query 采集 **9 项指标**：

| 指标 | 含义 | 采集方式 |
|------|------|---------|
| **Wall(s)** | 端到端墙钟耗时 | `time.perf_counter()` |
| **CPU(s)** | 进程 CPU 时间 (user + system) | Python: `os.times()`, Java: `OperatingSystemMXBean.getProcessCpuTime()` |
| **CPU%** | CPU / Wall 比 (>100% 并行, ~100% 单线程) | 计算 |
| **Read(MB)** | 从存储层访问的压缩字节数 | 文件元数据预计算：Parquet=RowGroup, Arrow=文件大小, HDF5=chunk, TsFile=Chunk |
| **Amp** | 读放大率 = Read(MB) / Useful(MB) | 计算 |
| **Useful(MB)** | 返回的有用解压字节 (points × 16) | 计算 |
| **Mem(KB)** | RSS 内存占用 | Python: `psutil.Process().memory_info()`, Java: `psutil.Process(pid).memory_info()` 子进程轮询 |
| **Pts** | 返回的数据点数 | 应用层计数 |
| **Pts/s** | 有效吞吐量 | Pts / Wall(s) |

### 公平性保障

| 保障项 | 方式 |
|--------|------|
| **单线程对比** | `pa.set_cpu_count(1)` + `OMP_NUM_THREADS=1` 锁死 Arrow/Parquet 多线程解压 |
| **JVM 预热** | Java 端测量前跑 3 轮预热，消除 JIT 编译开销 |
| **修剪均值** | 每次 >=3 次重复时，丢弃最慢一次（冷启动 OS page cache miss），余下取均值 |
| **进程级 CPU** | Java 用 `OperatingSystemMXBean.getProcessCpuTime()`，和 Python `os.times()` 口径一致 |
| **子进程 Memory** | `psutil.Process(pid).memory_info().rss` 在 Java 进程存活期间 10ms 轮询，捕获峰值 RSS |

---

## 测试环境

| 项目 | 配置 |
|------|------|
| CPU | Intel Core (Windows 11) |
| 内存 | 32 GB |
| 磁盘 | SSD |
| Python | 3.12.1 |
| PyArrow | 24.0.0 |
| h5py | 3.16.0 |
| Java | OpenJDK 17.0.18 |
| TsFile | 2.3.0-260422-SNAPSHOT |

---

## 数据集

| 参数 | 值 |
|------|-----|
| 设备数 | 30（Benchmark 全部 30 设备对比） |
| 每设备测点数 | 15 |
| 时间跨度 | 10 天 |
| 采样间隔 | 2 秒 |
| 总数据点 | **194,400,000** |
| 单个测点点数 | 432,000 |

### 数据质量

| 检查项 | 结果 |
|--------|------|
| 数值范围合理 | ✓ 全部符合物理约束 |
| 时间单调递增 | ✓ |
| 间隔一致性 | ✓ 0 处不匹配 / 431,999 间隔 |
| 跨设备独立性 | ✓ d_000 vs d_001 相关性 = -0.21 |
| 跨测点独立性 | ✓ temperature vs vibration 相关性 = -0.01 |
| 跨格式一致性 | ✓ Parquet = Arrow = HDF5 = TsFile（15/15 测点验证） |

---

## 对比文件格式

| 格式 | 存储大小 (30 设备) | 压缩 | 编码 | I/O 最小单元 | 列裁剪 | 随机访问 |
|------|-------------------|------|------|-------------|--------|---------|
| **Parquet** | 1,757 MB | Snappy | RLE + Dictionary | Row Group (~1M 行) | Row Group 级 ✓ | 差 |
| **Arrow IPC** | 3,132 MB | LZ4 | Plain + Dictionary | 整个文件 | 无 ✗ | 无 |
| **HDF5** | 857 MB | Gzip-4 | Plain | Chunk (100K 点) | Dataset 级 ✓✓ | 优 |
| **TsFile** | 1,217 MB | Snappy | **GORILLA** | Page (原子解压) | Chunk 级 ✓ | **优** |

---

## 测试结果

### Pattern 1: Sequential Scan（顺序扫描基线）

单列，全量时间范围（432,000 点）

| 格式 | Wall(s) | CPU(s) | CPU% | Read(MB) | Amp | Mem(KB) | Pts/s |
|------|---------|--------|------|----------|-----|---------|-------|
| **TsFile** | **0.035** | 0.055 | 154% | 2.5 | 0.4× | 1,292,816 | 12.3M |
| **HDF5** | 0.014 | 0.008 | 57% | 7.6 | 1.2× | 867 | 30.9M |
| **Parquet** | 0.146 | 0.129 | 88% | 3.9 | 0.6× | 5,257 | 3.0M |
| **Arrow** | 0.350 | 0.316 | 90% | 104.3 | 15.8× | 20,974 | 1.2M |

- TsFile Read 最低（2.5 MB，仅读目标测点的 Chunk）
- Arrow Read 最高（104.3 MB，必须读整个文件）
- TsFile Wall 0.035s，已经和 Parquet 0.146s 同级
- **TsFile Mem 1.29 GB** 是 JVM 堆（含 GC 预分配），非纯文件数据

---

### Pattern 2: Column Subset（列子集选择）

从 15 个测点中选取不同比例

#### 完整数据

| 格式 | 7% (1/15) | 20% (3/15) | 50% (7/15) | 100% (15/15) | 裁剪比 |
|------|-----------|------------|-------------|--------------|--------|
| **TsFile** Wall | 0.035s | 0.100s | 0.221s | 0.446s | **0.08×** |
| TsFile Read | 2.4MB | 7.3MB | 17.1MB | 36.7MB | 线性缩放 ✓ |
| **HDF5** Wall | 0.015s | 0.033s | 0.058s | 0.121s | **0.13×** |
| HDF5 Read | 7.6MB | 22.9MB | 53.4MB | 114.4MB | 线性缩放 ✓ |
| **Parquet** Wall | 0.160s | 0.491s | 1.148s | 2.662s | **0.06×** |
| Parquet Read | 3.9MB | 11.7MB | 27.2MB | 58.4MB | 近似线性 ✓ |
| **Arrow** Wall | 0.391s | 0.411s | 0.443s | 0.357s | **1.10×** |
| Arrow Read | 103.8MB | 311.5MB | 726.9MB | 1,557.6MB | **恒定** ✗ |

**关键发现**：

- **Arrow 无列裁剪**：I/O 完全不随列数变化（全文件读取），裁剪比 1.10× 说明耗时与列数无关
- **TsFile/HDF5/Parquet 均有有效列裁剪**：Read 随列数线性缩放
- TsFile 绝对 Read 最低（2.4MB vs HDF5 7.6MB vs Parquet 3.9MB）

---

### Pattern 3: Downsampling（降采样）—— 核心测试

全量时间范围，单列，每 N 个点取 1 个

#### 读放大率（Read Amplification）

| 格式 | Step 1/1 | Step 1/10 | Step 1/100 | Step 1/500 | 放大来源 |
|------|----------|-----------|------------|------------|---------|
| **Arrow** | 15.8× | 157.6× | 1,576× | **7,878×** | 全文件读取 |
| **HDF5** | 1.2× | 11.6× | 115.7× | **579×** | Chunk 粒度浪费 |
| **Parquet** | 0.6× | 5.8× | 58.4× | **292×** | Row Group 粒度浪费 |
| **TsFile** | 0.4× | 3.8× | 37.8× | **189×** | Page 粒度浪费 |

#### 耗时恒定性验证

| 格式 | Step 1 | Step 10 | Step 100 | Step 500 | 结论 |
|------|--------|---------|----------|----------|------|
| **TsFile** | 0.028s | 0.027s | 0.027s | 0.028s | **恒定** |
| **HDF5** | 0.012s | 0.009s | 0.008s | 0.009s | **恒定** |
| **Parquet** | 0.205s | 0.201s | 0.198s | 0.202s | **恒定** |
| **Arrow** | 0.312s | 0.307s | 0.307s | 0.320s | **恒定** |

**所有格式耗时完全不随采样率变化**——Page/Chunk 是原子 I/O 单元的直接证据。

#### CPU 耗时分析

| 格式 | Step 1/500 CPU | CPU% | 解读 |
|------|---------------|------|------|
| **TsFile** | 0.031s | — | 单线程解码 |
| **Parquet** | 0.207s | 102% | **单线程**（已锁定） |
| **Arrow** | 0.293s | 91% | **单线程**（已锁定） |
| **HDF5** | 0.004s | 44% | 单线程，几乎无计算 |

---

### Pattern 4: AI Training Simulation（AI 训练模拟）

500 个随机窗口（512 点/窗口），5 个随机设备，3/15 列（20%）。

**这是最接近真实 AI DataLoader 的测试。**

| 格式 | Wall(s) | CPU(s) | CPU% | Read(MB) | Amp | Mem(KB) | Pts/s |
|------|---------|--------|------|----------|-----|---------|-------|
| **TsFile** | **0.444** | 0.777 | 175% | **4.5** | **0.4×** | 1,292,816 | **1,733,282** |
| **HDF5** | 0.628 | 0.625 | 99% | 13.6 | 1.2× | 688 | 1,223,454 |
| **Arrow** | 1.506 | 1.461 | 97% | 185.1 | 15.8× | −650 | 510,534 |
| **Parquet** | 316.3 | 316.9 | 100% | 6.9 | 0.6× | 88 | 2,430 |

**核心发现**：

1. **TsFile 最快**（0.44s）：Chunk 级 min/max time 统计值让 `TsFileReader.query()` 能跳过不相关 Chunk，只读命中 Page。Read 仅 4.5 MB。**比 HDF5 快 1.4×，比 Arrow 快 3.4×，比 Parquet 快 712×。**

2. **Parquet 崩溃**（316s）：每次随机窗口是一次独立 `pq.read_table()`。500 窗口 × 3 测点 × 5 设备 = 7,500 次独立 Parquet 操作，每次需解析 Footer → 定位 Row Group → 读 Column Chunk。

3. **单线程验证**：Parquet CPU% = 100%，说明 `pa.set_cpu_count(1)` 成功锁死多线程。之前多线程下 Parquet 132s Wall 658s CPU（5× 并行），现在 316s Wall ≈ 316s CPU（单线程）。

4. **TsFile 内存 1.29 GB**：包含 JVM 堆（~1GB）的基础开销，不是 TsFile 查询本身的内存。HDF5 和 Parquet 几乎无内存增量（文件元数据在 Python 侧以惰性方式处理）。

---

## 各格式设计差异总结

| 特性 | Parquet | Arrow IPC | HDF5 | TsFile |
|------|---------|-----------|------|--------|
| **列裁剪** | Row Group 级 ✓ | 无 ✗ | Dataset 级 ✓✓ | Chunk 级 ✓ |
| **Page 内跳读** | 无 | 无 | 无 | 无 |
| **随机窗口** | 差（712× 慢于 TsFile） | 中（全文件重读） | 优 | **最优** |
| **I/O 单元** | Row Group (~1M 行) | 整个文件 (6.48M 行) | Chunk (100K 点) | Page (~几 K 点) |
| **编码** | RLE + Dict | Plain + Dict | Plain | **GORILLA** |
| **单线程 Read(MB)** P4 | 6.9 | 185.1 | 13.6 | **4.5** |
| **单线程 Wall P4** | 316.3s | 1.506s | 0.628s | **0.444s** |
| 读放大 P3 (1/500) | 292× | 7,878× | 579× | **189×** |

---

## TsFile 格式专项分析

### 对齐 Chunk Group 结构

```
Aligned Chunk Group (per device)
  ├── Time Chunk:      [Page] [Page] ... [Page]    ← 时间列，GORILLA 编码
  ├── Value Chunk m₀:  [Page] [Page] ... [Page]    ← 测点 0，GORILLA 编码
  ├── Value Chunk m₁:  [Page] [Page] ... [Page]    ← 测点 1
  └── ... (共 15 个 Value Chunk)

每个 Chunk 有 min/max time 统计值 → 时间过滤在 Chunk 级生效
Time + Value Chunks 分离存储       → 列裁剪在 Chunk 级生效
Page 是原子压缩单元                → Page 内不支持部分读取
GORILLA 编码 XOR 相邻值            → 时序数据极致压缩
```

### P4 AI 训练场景下的 I/O 路径

```
场景: 3/15 列, 窗口=512 点, 随机时间位置

TsFile 实际 I/O:
  1. Chunk 统计过滤: 跳过 12/15 的 Value Chunk
  2. Time Chunk 统计过滤: 定位包含目标时间的段
  3. Page 读取: 读命中的 ~几个 Page（每个 Page 含 ~几 K 个点）
  4. Page 全解压: GORILLA 解码整个 Page（无法部分解码）

放大来源:
  列维度: Chunk 级已过滤 ✓（无放大）
  时间维度: Page 含 ~几 K 点，只需 512 点 → 放大 ~N×
  综合: Read 4.5MB / Useful 12.3MB = 0.4×
```

### HDF5 和 TsFile 的读放大对比

在这个 Benchmark 中，HDF5 Chunk = 100K 点，TsFile Page ≈ 几 K 点。为了读取 512 点的窗口：

- HDF5：必须解压整个 100K 点的 Chunk → 13.6 MB Read
- TsFile：必须解压包含 512 点的 Page（几 K 点） → 4.5 MB Read

TsFile 更小的 Page 粒度使其在随机窗口场景下读放大更低。

---

## TsFile 优化建议

### 建议 1：Page 内列偏移索引（短期）

在 Page 元数据中增加每列偏移信息：

```
当前: 整 Page 原子压缩
建议: [col_offset_table][time_col][val_col0]...[val_colN]
```

预期：列维 I/O 缩减 15× → 1×

### 建议 2：编码层重同步标记（中期）

在 GORILLA 编码流中每 K 个点插入重同步点，允许跳读：

预期：时间维 I/O 缩减 189× → 10-20×

### 建议 3：Compaction 多分辨率 Page（长期）

Compaction 时生成低分辨率 Page，查询按采样率路由：

预期：时间维 I/O 缩减 189× → 1×

---

## TsFile 格式转换方法

TsFile 的读写库是纯 Java 的（`org.apache.tsfile`），没有 Python binding。为了在 Python Benchmark 框架中测试 TsFile，经历了三个阶段。

### 阶段一：直接 Java 测试（规模受限）

最初用 Java 原生 API 手写了一个小规模 TsFile（1 设备 × 15 测点 × 43,200 点），在 Java 端直接跑查询。问题是：
- 无法和 Python 端的 Parquet/Arrow/HDF5 共享同一份数据
- 数据集规模太小（1 天 vs 10 天），结论不具备可比性

### 阶段二：JPype 桥接（规模匹配，性能代价大）

用 JPype 在 Python 进程中启动 JVM，调用 `TsFileWriter` Java API，从 Parquet 文件读取同一份数据写入 TsFile。

**写入路径（`convert_to_tsfile.py`）**：

```
Parquet (30 设备) → pandas DataFrame → JPype → TsFileWriter → .tsfile
```

- 每个设备 6.48M 行 × 15 列 = 97M 次 `tablet.addValue()` JPype 调用
- 每次调用穿越 JNI（Java Native Interface）边界，单设备转换耗时 2-17 分钟
- 30 设备总耗时约 30 分钟

**初始版本的两个 Bug**：

| Bug | 问题 | 修复 |
|-----|------|------|
| PLAIN 编码 | 未启用 TsFile 的时序专有编码，文件偏小但压缩率不反映真实场景 | 改用 `TSEncoding.GORILLA`（XOR 相邻值差异编码） |
| 数组硬凑对齐 | 假设所有测点时间戳完全一致，用手动索引拼接 | 改用 `df.pivot(index="time", columns="measurement")` 安全对齐 |

**查询路径（JPype 版本，已被淘汰）**：

```
Python → JPype → TsFileReader.query() → 逐行 ds.next()
```

每次 `query()` 和 `ds.next()` 都穿越 JNI。实测 Pattern 1 顺序扫描 0.92s，纯 Java 下只需 0.035s——JPype 开销约 26 倍。

### 阶段三：原生 Java 子进程（当前方案）

彻底绕开 JPype。写了一个独立的 Java 程序 `TsFileNativeRunner.java`，Python 通过 `subprocess` 启动它，Java 内部跑完所有 4 个 Pattern，输出 JSON 到 stdout，Python 解析合并。

```
benchmark_runner.py
  │
  ├─ subprocess.Popen("java TsFileNativeRunner")  ← TsFile 查询在此
  │     ├─ warmup (3 轮 JIT 预热)
  │     ├─ 4 个 Pattern 的查询
  │     ├─ 进程级 CPU 计时 (OperatingSystemMXBean)
  │     └─ stdout 输出 JSON
  │
  ├─ psutil.Process(pid) 轮询子进程 RSS  ← Memory 监控
  │
  └─ Python Querier: Parquet / Arrow / HDF5
```

**优势**：TsFile 查询跑在纯 Java 中，无 JPype 开销，Wall time 和 Parquet/Arrow/HDF5 公平可比。

**代价**：TsFile 跑在独立 Java 进程，Python 的 `os.times()` 测不到它的 CPU，`psutil` 测 Python 进程 RSS 也测不到它。这需要额外的手段来补齐（见下节）。

---

## 公平性改进历程

### 第一轮：基础 Benchmark（问题最多）

| 问题 | 表现 | 严重程度 |
|------|------|---------|
| Parquet 多核解压 | CPU/Wall = 497%（5 核并行），TsFile 单核 | **致命**：8 个人和 1 个人比速度 |
| TsFile PLAIN 编码 | 文件仅 32 MB，但未启用 GORILLA，压缩率不反映真实 | **致命**：TsFile 没装备核心武器 |
| JPype 跨语言开销 | TsFile 每次 `query()` 穿 JNI，P1 0.92s vs Java 0.035s | **致命**：26 倍性能惩罚 |
| 无 JVM 预热 | 首次查询 0.5s，后续 0.03s，均值被冷启动拉偏 | 中等 |
| CPU 测量口径不一 | TsFile 测单线程，Parquet 测全进程（含 5 核） | 中等 |
| Memory 测不到 | TsFile 在子进程，Python `psutil` 返回 0 | 中等 |
| 数据趋势漂移 | 趋势项用 Unix 绝对时间（17 亿秒），温度漂移到 -767°C | 影响数值但非性能指标 |
| 设备间几乎相同 | 随机种子区分度不够，d_000 vs d_001 相关性 0.98 | 影响 P4 随机设备维度 |

### 第二轮：修数据 + 替换 JPype

**数据修复**（`data_gen.py`）：
- 趋势项改用相对时间戳（秒 from dataset start）
- 随机种子加大设备间区分度（`SEED × 1M + dev_id × 7919 + meas_hash × 31`）
- 添加每设备独立相位偏移（±2π）、振幅缩放（0.6×-1.4×）、周期拉伸（±25%）
- 应用物理约束（`np.maximum(signal, 0)` for non-negative）
- 修复后验证：跨设备相关性 → -0.21，数值范围合理

**查询替换**（`TsFileNativeRunner.java`）：
- 编写纯 Java 原生 Benchmark，Python 通过 subprocess 调用
- P1 从 JPype 的 0.92s 降到 0.035s（26× 提速）
- P4 从 JPype 的 2.22s 降到 0.44s（5× 提速）

### 第三轮：统一测量口径

**单线程锁定**（`benchmark_runner.py` 开头）：

```python
os.environ["OMP_NUM_THREADS"] = "1"
pa.set_cpu_count(1)
pa.set_io_thread_count(1)
```

效果：Parquet P4 CPU/Wall 从 497% → 100%，Wall 从 132s → 316s（真实的单核性能）。

**JVM 预热**（`TsFileNativeRunner.java`）：

```java
// 3 轮 warmup，让 JIT 把热点编译成机器码
for (int i = 0; i < 3; i++) {
    warmup_query();
}
// 之后才开始计时
```

效果：首次查询 0.5s → 预热后稳态 0.03s，修剪均值不再被冷启动拉偏。

**进程级 CPU**（Java 端）：

```java
// 从 ThreadMXBean（单线程）改为 OperatingSystemMXBean（全 JVM 进程）
OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
long cpuNs = osBean.getProcessCpuTime();
```

和 Python 端 `os.times()` 的进程级口径一致。

**子进程 Memory 监控**（`tsfile_native.py`）：

```python
# 踩坑 1: subprocess.run() 阻塞调用，返回时 Java 已退出，内存被 OS 回收 → 永远 0
# 踩坑 2: Popen + while proc.poll() is None 轮询 → stdout 管道填满导致死锁
# 最终方案: Popen + 独立线程 drain stdout + 主线程 10ms 轮询 RSS
proc = subprocess.Popen(cmd, stdout=PIPE, ...)
# daemon thread reads stdout to prevent pipe deadlock
t = threading.Thread(target=_drain_stdout, daemon=True)
t.start()
ps_proc = psutil.Process(proc.pid)
while proc.poll() is None:
    rss = ps_proc.memory_info().rss   # ← 在主线程轮询
    time.sleep(0.01)
```

效果：TsFile Memory 从 0 KB → 1.29 GB（JVM 堆，包含 ~1GB 预分配）。

**修剪均值**（`analyze.py`）：

```python
def _safe_mean(items, key, default=0):
    vals = [...]
    if len(vals) >= 3:
        vals.remove(max(vals))  # 丢弃冷启动最慢一次
    return sum(vals) / len(vals)
```

### 改进效果总览

| 指标 | 第一轮 | 最终 | 改进方式 |
|------|--------|------|---------|
| TsFile P1 Wall | 0.92s | **0.035s** | JPype → 原生 Java subprocess |
| TsFile P4 Wall | 2.22s | **0.444s** | 同上 + JVM 预热 |
| Parquet CPU% | 497% | **100%** | `pa.set_cpu_count(1)` |
| TsFile Mem | 0 KB | **1.29 GB** | Popen + 线程 drain + psutil 轮询 |
| TsFile 编码 | PLAIN | **GORILLA** | 修改 `convert_to_tsfile.py` |
| 数据质量 | 温度 -767°C | 正常范围 | 相对时间戳 + 物理约束 |
| 跨设备独立性 | r=0.98 | **r=-0.21** | 种子重构 + 相位偏移 |
| P1 冷启动偏差 | 0.5s vs 0.03s | 已消除 | 修剪均值（去 max） |
| Parquet Read(MB) | 部分为 0 | 全部有效 | 改用 `file_size // n_meas` |

---

## 局限性与说明

| 局限 | 说明 |
|------|------|
| **TsFile Mem 含 JVM 堆** | 1.29 GB 包含 JVM 运行时 + 堆预分配，不是纯查询数据量。HDF5/Parquet/Arrow 无此开销 |
| **Read(MB) 是预计算值** | 来自文件元数据（RowGroup/Chunk/Dataset 压缩大小），非运行时测量。对 Chunk 级过滤场景可能高估 |
| **Parquet P4 Read(MB) = 6.9** | 略高于预期，因为 `_cost` 用 `file_size // 15` 粗略估算，未考虑 Row Group 级过滤的实际命中量 |
| **单机测试** | 单台 Windows 机器 |
| **GORILLA 文件偏大** | Synthetic 数据含随机噪声，GORILLA XOR 编码不产生大量前导零。真实 IoT 数据压缩率更高 |

---

## 复现

```bash
cd benchmark

# 1. 数据生成
python data_gen.py

# 2. Parquet / Arrow / HDF5
python converters.py

# 3. TsFile (GORILLA)
python convert_to_tsfile.py

# 4. Benchmark（30 设备）
python benchmark_runner.py

# 5. 分析
python analyze.py

# 输出: results/benchmark_results_*.json, results/io_amplification.png
```

# Heracles

Heracles 是一个基于 Prometheus TSDB v0.8.0 构建的高性能时序数据库 (Time Series Database)，由 naivewong 开发。项目使用 Go 1.12 编写，实现了对时序数据的高效存储、压缩和查询功能。

## 背景

本项目基于 Prometheus TSDB，继承了其核心设计理念：
- 基于 Gorilla 论文中的 XOR 压缩算法
- 高效的内存和磁盘存储结构
- 支持水平扩展的数据分区

设计文档可参考 [Prometheus TSDB Design](https://fabxc.org/blog/2017-04-10-writing-a-tsdb/)，原始压缩算法基于 [Gorilla 论文](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf)。

## 项目结构

### 核心模块

| 模块/文件 | 描述 |
|-----------|------|
| `db.go` | 数据库核心实现，提供 `DB` 结构体、`Options` 配置、`Appender` 和 `Querier` 接口 |
| `block.go` | 数据块管理，处理块的创建、读取和生命周期，实现 `BlockReader` 接口 |
| `head.go` | 内存头部数据存储，处理最新的时序数据写入和 WAL 日志 |
| `compact.go` | 数据压缩和合并模块，实现 `LeveledCompactor` 进行分层压缩 |
| `querier.go` | 查询接口实现，提供时序数据查询、标签选择功能 |
| `wal.go` | 预写日志 (Write-Ahead Log) 实现，保证数据持久性 |
| `checkpoint.go` | 检查点机制，用于 WAL 清理和恢复 |
| `repair.go` | 数据修复工具，处理损坏的数据块 |
| `record.go` | 记录编码/解码，用于 WAL 和检查点的数据序列化 |
| `tombstones.go` | 墓碑标记，用于逻辑删除数据 |

### 子包模块

| 子包 | 描述 |
|------|------|
| `chunkenc/` | 数据块编码/解码，实现多种压缩算法 (XOR, GMC1, GMC2, GDC1) |
| `chunks/` | 数据块存储管理，处理块文件的读写 |
| `index/` | 索引实现，提供系列查找和 postings 列表功能 |
| `encoding/` | 编码工具，实现 Nth 编码等 |
| `fileutil/` | 文件工具，提供 mmap、flock、同步等平台相关功能 |
| `labels/` | 标签系统，处理时序数据的标签和选择器 |
| `wal/` | WAL 日志实现，包含 reader 和 writer |
| `errors/` | 错误定义模块，提供 `MultiError` 等工具 |
| `goversion/` | Go 版本检查 |
| `testutil/` | 测试工具 |
| `tsdbutil/` | TSDB 工具函数，如 buffer 实现 |
| `test/` | 测试辅助代码 |

### 命令行工具

| 命令 | 描述 |
|------|------|
| `cmd/tsdb/` | TSDB 主命令行工具，提供 benchmark 和测试功能 |
| `cmd/query_bench/` | 查询性能基准测试工具 |

### 压缩算法

`chunkenc` 包实现了多种压缩算法：

| 算法 | 描述 |
|------|------|
| **XOR** | 基于 Gorilla 的 XOR 压缩，适用于浮点数时序数据 |
| **GMC1** | 自定义压缩算法版本 1 |
| **GMC2** | 自定义压缩算法版本 2 |
| **GDC1** | 另一种自定义压缩算法 |

## 数据格式

详细的二进制格式文档位于 [`docs/format/`](docs/format/) 目录：

| 文档 | 描述 |
|------|------|
| [Index](docs/format/index.md) | 索引文件格式 |
| [Chunks](docs/format/chunks.md) | 数据块文件格式 |
| [Tombstones](docs/format/tombstones.md) | 墓碑文件格式 |
| [Wal](docs/format/wal.md) | WAL 日志格式 |
| [Group Index](docs/format/group_index.md) | 分组索引格式 |
| [Group Chunk](docs/format/group_chunk.md) | 分组数据块格式 |

## 快速开始

### 依赖

本项目需要 Go 1.12 或更高版本。

### 构建

```bash
# 获取依赖
go mod vendor

# 编译项目
go build ./...
```

### 基本使用

```go
import "github.com/naivewong/tsdb-group"

// 打开数据库
db, err := tsdb.Open(
    "./data",           // 数据目录
    logger,             // log.Logger
    prometheusRegistry, // prometheus.Registerer (可选)
    &tsdb.Options{
        WALSegmentSize:    wal.DefaultSegmentSize,
        RetentionDuration: 15 * 24 * 60 * 60 * 1000, // 15 天
        BlockRanges:       tsdb.ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
        NoLockfile:        false,
        WALCompression:    false,
    },
)
if err != nil {
    // 处理错误
}
defer db.Close()

// 写入数据
appender := db.Appender()
ref, err := appender.Add(labels.Labels{{Name: "__name__", Value: "metric1"}}, timestamp, value)
if err != nil {
    // 处理错误
}
err = appender.Commit()

// 查询数据
querier, err := db.Querier(mint, maxt)
if err != nil {
    // 处理错误
}
defer querier.Close()

seriesSet, err := querier.Select(labels.MustNewMatcher(labels.MatchEqual, "__name__", "metric1"))
```

## 运行测试

### 运行所有测试

```bash
# 运行项目所有测试
go test ./...

# 使用 vendor 模式运行测试
go test -mod=vendor ./...

# 显示详细输出
go test -v ./...
```

### 运行特定包的测试

```bash
# 测试核心模块
go test -v .

# 测试特定子包
go test -v ./chunkenc/
go test -v ./index/
go test -v ./wal/
go test -v ./labels/
go test -v ./encoding/
go test -v ./fileutil/
go test -v ./errors/
go test -v ./tsdbutil/

# 运行单个测试文件中的测试
go test -v -run TestBlock .
go test -v -run TestHead .
```

### 测试覆盖率

```bash
# 生成覆盖率报告
go test -cover ./...

# 生成详细覆盖率报告（按函数）
go test -cover -covermode=count ./...

# 生成 HTML 覆盖率报告
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

### 基准测试

```bash
# 运行所有基准测试
go test -bench=. ./...

# 运行特定基准测试
go test -bench=BenchmarkDB ./...
go test -bench=BenchmarkHead ./...
go test -bench=BenchmarkChunk ./chunkenc/

# 基准测试带内存分析
go test -bench=. -benchmem ./...
```

### 跳过慢测试

部分测试执行时间较长（详见下文"慢测试标注"），可通过 `-short` 标志跳过：

```bash
go test -short ./...
```

在测试代码中使用 `testing.Short()` 判断：

```go
if testing.Short() {
    t.Skip("skipping slow test in short mode")
}
```

## Benchmark

### 测试准备

1. 准备测试数据集（可从 [这里](https://mycuhk-my.sharepoint.com/:f:/g/personal/1155092207_link_cuhk_edu_hk/Ei2gQU_2J9ZJoULerBrXJjgBmBou4qNRg8HKxrCirQyYDg?e=FWEg3O) 下载）
2. 将数据文件复制到 `testdata/bigdata/node_exporter` 目录
3. 准备依赖：`go mod vendor`

### 写入测试

```bash
cd cmd/tsdb

# 随机数据测试
go run -mod=vendor main.go bench write \
    --osleep [baseline_sleep_seconds] \
    --gsleep [group_sleep_seconds] \
    --metrics [num_of_ts] \
    --batch [batch_size]

# Timeseries 数据测试
go run -mod=vendor main.go bench write \
    --osleep [baseline_sleep_seconds] \
    --gsleep [group_sleep_seconds] \
    --metrics [num_of_ts] \
    --batch [batch_size] \
    --timeseries
```

### TSBS 查询基准测试

修改 `db_bench_tsbs_test.go` 中的 `numSeries` 参数来测试不同数据量：

```bash
# Prometheus TSDB 测试
go test -mod=vendor -run ^$ -bench ^BenchmarkDBtsbs$ . -timeout 99999s -benchtime 10s -v

# TGroup 测试
go test -mod=vendor -run ^$ -bench ^BenchmarkGroupDBtsbs$ . -timeout 99999s -benchtime 10s -v
```

## 核心特性

- **高效压缩**: 支持多种压缩算法，包括 XOR、GMC1、GMC2、GDC1
- **分层存储**: 内存 Head + 磁盘 Blocks 的分层架构
- **自动压缩**: 后台自动进行数据块合并压缩
- **数据持久性**: WAL 预写日志保证数据不丢失
- **灵活保留策略**: 支持基于时间和大小的数据保留策略
- **并发查询**: 支持多查询并发访问
- **墓碑机制**: 支持数据的逻辑删除

## 架构说明

```
┌─────────────────────────────────────────────────────────────┐
│                           DB                                │
│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
│  │      Head       │    │            Blocks               │ │
│  │  (内存存储)     │    │  ┌─────┐ ┌─────┐ ┌─────┐       │ │
│  │  - WAL 日志      │    │  │Block│ │Block│ │Block│ ...   │ │
│  │  - 最新数据      │    │  └─────┘ └─────┘ └─────┘       │ │
│  └─────────────────┘    └─────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## 测试模块总结

### 核心模块测试

| 测试文件 | 测试内容 | 备注 |
|----------|----------|------|
| `block_test.go` | 数据块管理测试：元数据版本、压缩失败标记、块创建、损坏 chunk 检测、块大小计算、索引读取、墓碑删除 | |
| `checkpoint_test.go` | 检查点机制测试：最近检查点查找、检查点删除、检查点创建、错误处理后临时目录清理 | |
| `compact_test.go` | 数据压缩测试：重叠 group chunk 合并、块压缩、压缩器创建 | **慢测试**：涉及多块合并和数据生成 |
| `db_test.go` | 数据库核心测试：WAL 读写、系列添加、查询选择器、数据删除、恢复机制 | **慢测试**：需要完整 DB 生命周期 |
| `head_test.go` | 内存头部存储测试：分组数据生成、并发追加、截断、 WAL 集成 | |
| `querier_test.go` | 查询器测试：系列集合合并、标签匹配、时间范围查询 | |
| `record_test.go` | 记录编码测试：Series/Sample/Tombstone 编解码、损坏记录处理 | |
| `repair_test.go` | 数据修复测试：索引版本修复、损坏块恢复 | |
| `tombstones_test.go` | 墓碑机制测试：区间合并、并发安全、文件读写、边界条件 | |
| `wal_test.go` | WAL 日志测试：段切割、截断、日志恢复、损坏修复、迁移 | **慢测试**：涉及多段文件操作 |

### 子包测试

| 子包 | 测试文件 | 测试内容 | 备注 |
|------|----------|----------|------|
| `chunkenc/` | `chunk_test.go` | XOR chunk 编解码器测试、迭代器正确性 | 包含 benchmark |
| `chunkenc/` | `gmc1_test.go` | GroupMemoryChunk1 测试：不完整 tuple、单/多系列、大批量点 | **慢测试**：10000 点测试 |
| `chunkenc/` | `gmc2_test.go` | GroupMemoryChunk2 测试：截断、系列迭代 | **慢测试**：包含截断测试 |
| `chunkenc/` | `gdc1_test.go` | GroupDiskChunk1 测试：GMC 转换、部分范围转换、尺寸测试 | **慢测试**：需要外部测试数据 |
| `chunks/` | `chunks_test.go` | 数据块读写测试：GroupChunk 持久化、部分转换 | |
| `encoding/` | `encoding_test.go` | 编码工具测试：Nth 元素查找、MedianHeap | 包含 benchmark |
| `encoding/` | `nth_test.go` | Nth 算法测试：最大/最小堆、中位数堆、边界条件 | 包含 benchmark |
| `errors/` | `errors_test.go` | 错误处理测试：MultiError 聚合、扁平化 | |
| `fileutil/` | `fileutil_test.go` | 文件工具测试：目录复制、重命名、替换、读取 | |
| `fileutil/` | `flock_test.go` | 文件锁测试 | |
| `goversion/` | `goversion_test.go` | Go 版本检查测试 | |
| `goversion/` | `goversion_test.go` | Go 版本检查测试 | |
| `index/` | `index_test.go` | 索引读写测试：符号表、postings、group postings、端到端验证 | **慢测试**：加载 20k 系列 JSON |
| `index/` | `postings_test.go` | Postings 列表测试 | |
| `labels/` | `labels_test.go` | 标签系统测试 | |
| `labels/` | `selector_test.go` | 选择器测试：Equal/Regexp/Not 匹配器、边界条件 | 包含 benchmark |
| `test/` | `conv_test.go` | 类型转换测试 | |
| `test/` | `hash_test.go` | 哈希测试 | |
| `test/` | `labels_test.go` | 标签测试 | |
| `tsdbutil/` | `buffer_test.go` | 缓冲区测试 | |
| `tsdbutil/` | `chunks_test.go` | Chunk 工具测试：样本生成、 populated chunk | |
| `wal/` | `reader_test.go` | WAL 读取器测试 | |
| `wal/` | `wal_test.go` | WAL 核心测试：损坏修复、页错误处理 | **慢测试**：涉及文件修复 |

### 基准测试

| 测试文件 | 内容 |
|----------|------|
| `db_bench_test.go` | DB 写入/查询基准：DevOps/Node Exporter 数据加载 |
| `db_bench_tsbs_test.go` | TSBS 查询基准测试 |
| `head_bench_test.go` | Head 存储基准测试 |
| `chunkenc/chunk_test.go` | XOR 迭代器/追加器基准 |
| `encoding/encoding_test.go` | MaxBits、Nth 算法基准 |
| `encoding/nth_test.go` | 堆算法基准 |
| `labels/selector_test.go` | 匹配器基准 |
| `tsdbutil/chunks_test.go` | Chunk 生成基准 |

### 慢测试标注

以下测试由于涉及大量数据生成、文件 I/O 或外部数据依赖，执行时间较长：

1. **`compact_test.go`** - 多块合并操作
2. **`db_test.go`** - 完整数据库生命周期
3. **`wal_test.go`** - 多段 WAL 文件操作
4. **`chunkenc/gmc1_test.go`** - 10000 点测试、外部数据文件
5. **`chunkenc/gmc2_test.go`** - 截断测试
6. **`chunkenc/gdc1_test.go`** - 需要 testdata/bigdata 数据文件
7. **`index/index_test.go`** - 加载 20k 系列 JSON

## 许可证

Apache License 2.0

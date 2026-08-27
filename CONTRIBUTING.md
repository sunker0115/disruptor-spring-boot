# 贡献指南

感谢你对 `disruptor-spring-boot` 的关注。提交改动前，请先确认问题能够在当前 `main` 分支复现，并尽量将一次贡献限定在一个明确目标内。

## 开发环境

- JDK 21 或更高版本；
- Maven；
- Git。

在仓库根目录执行完整验证：

```bash
mvn --batch-mode --no-transfer-progress clean verify
```

## 代码与测试

- 保持 `disruptor-core` 不依赖 Spring；
- 直接复用 LMAX Disruptor 原生拓扑和处理器 API，不增加平行 DSL；
- 修复缺陷时优先增加复现测试，新逻辑必须同时包含测试；
- 异步测试使用有界等待，不能依赖无限休眠；
- 日志统一使用 SLF4J，不使用 `System.out` 或 `System.err`；
- 不在同一 PR 中夹带无关重构或格式化。

## 提交 Issue

提交前请先搜索已有 Issue。缺陷报告至少应包含：

- 使用的 JDK、Spring Boot、Disruptor 和项目版本；
- 最小复现代码或仓库；
- 预期行为与实际行为；
- 完整异常堆栈和必要日志，移除凭据及业务敏感数据。

安全漏洞不要通过公开 Issue 报告，请遵循[安全策略](SECURITY.md)。

## 提交 Pull Request

Pull Request 应说明问题、实现取舍和验证结果。提交前请确认：

- 全仓 `clean verify` 通过；
- 对外行为变化已同步到 README 或架构文档；
- 没有提交构建产物、IDE 元数据、凭据或私有配置；
- 改动保持向后兼容；如确实需要破坏性变更，应在 PR 中明确影响范围。

提交贡献即表示你有权提交相关内容，并同意该贡献按照项目的 [Apache License 2.0](LICENSE) 进行许可。

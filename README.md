# 断点可恢复的异步采集调度器（本地模拟站点 → SQLite）

纯 Python 3.12 实现，依赖仅 `httpx`（已安装）。不使用任何现成爬虫调度
引擎、Docker 或外部网站；模拟站点由 `crawler/site.py` 实现。

## 目录结构

- `crawler/db.py` — SQLite 任务库：稳定任务身份、持久化去重、租约领取、
  fencing token 防旧 worker 迟到提交、结果+后续任务+完成状态原子提交
- `crawler/site.py` — 模拟站点（分类→分页→详情，含重复链接与成环），
  以及仅供测试使用的可控同步点（`/__ctrl/...`：闸门、503 注入、慢响应、
  请求计数）
- `crawler/parser.py` — URL 规范化（去重身份）与 HTML 解析
- `crawler/worker.py` — asyncio worker：限并发、限内存队列、退避重试、
  三个崩溃注入点
- `crawler/scheduler.py` / `crawler/cli.py` — 多进程调度、完成判定、
  状态报告、命令行入口
- `tests/test_crawler.py` — 11 个自动化测试；`tests/expected_data.json`
  为预期答案，**仅供测试读取**，采集代码不引用它

## 关键设计

- **任务身份**：`sha1(规范化URL)`（去 fragment、查询参数排序）。一次采集
  中站点数据固定，URL 即稳定身份；`INSERT OR IGNORE` 持久化去重，重复
  链接与成环不会产生重复任务。
- **先存后续任务再算完成**：子任务 INSERT、父任务结果 INSERT、父任务
  状态置 done 在**同一个 `BEGIN IMMEDIATE` 事务**中提交；父任务完成前
  子任务必然已落库，重启不会漏分支。
- **租约 + fencing token**：领取任务时 `claim_seq+1` 并写入租约到期时间。
  worker 崩溃/超时后租约过期，任务可被重新领取；旧 worker 的迟到提交因
  `claim_seq` 不匹配在事务内被拒绝并整体回滚。
- **有限退避重试**：503 与超时按 `backoff_base * 2^attempts`（封顶
  `backoff_cap`）重试，超过 `max_retries` 置为 `failed` 并记录错误，
  绝不假装完成；4xx 直接判失败。
- **完成判定**：仅当任务表中不存在 `pending` 与 `leased` 行时才结束——
  内存队列暂时为空不会退出（仍有执行中或等待退避的任务）；报告会列出
  失败任务（含重试次数、错误）与"done 但缺结果行"的缺记录。

## Windows 运行说明（PowerShell）

```powershell
# 终端 1：启动模拟站点（也可由测试/脚本内嵌启动）
python -m crawler site --port 8765

# 终端 2：初始化数据库并播种首页任务
python -m crawler init --db crawl.db --base-url http://127.0.0.1:8765

# 启动 3 个 worker 进程采集到完成（退出码 0=完成，1=有失败/缺记录）
python -m crawler run --db crawl.db --base-url http://127.0.0.1:8765 --workers 3

# 随时查询任务状态、重试次数与完成依据（--verbose 列出每个任务）
python -m crawler status --db crawl.db --verbose
```

断点恢复：直接重复执行 `run` 即可，原数据库上继续，已完成任务不会
重新抓取。

崩溃注入（验证恢复用）：设置环境变量后启动 worker，命中即硬退出：

```powershell
$env:CRAWLER_CRASH = "pre_commit@books-0"   # response | pre_commit | post_commit
python -m crawler worker --db crawl.db --base-url http://127.0.0.1:8765 --worker-id w0
```

运行测试（标准库 unittest，无需安装 pytest）：

```powershell
python -m unittest discover -s tests -v
```

## 测试覆盖与实测结果

实测（本机 Windows + Python 3.12.8）：`Ran 11 tests in ~25s, OK`，
连续两轮均通过。

- `test_full_crawl_matches_expected` — 3 worker 全量采集：33 个任务、
  23 条详情记录与预期答案逐项一致；每个页面恰好抓取 1 次（重复链接/
  成环被持久化去重）。
- `test_no_double_claim_and_lease_expiry` — 并发抢任务：租约未过期时
  第二个 worker 抢不到；过期后可重领且 fencing token 递增。
- `test_stale_worker_commit_rejected` — 旧 worker 迟到的结果与重试
  提交均被拒绝，结果行唯一。
- `test_late_result_rejected_with_live_workers` — 用站点闸门制造真实
  场景：A 卡住→租约过期→B 重领并完成→A 迟到提交被拒；该页被抓 2 次
  但只落库 1 次，最终任务集与预期一致。
- `test_503_retry_then_success` — 详情页先 503 两次后成功：
  `attempts=2`、状态 done、页面共抓 3 次。
- `test_retry_exhaustion_marks_failed` — 503 持续超出重试上限：任务
  明确 `failed`（`attempts=3`），报告列出失败，判定 INCOMPLETE。
- `test_timeout_then_retry_success` — 客户端超时记录一次失败后重试成功。
- `test_empty_queue_waits_for_scheduled_retry` — 队列为空但有退避中的
  任务时不判完成、也不可提前领取。
- `test_crash_after_response` / `test_crash_after_save_before_commit` /
  `test_crash_after_commit_before_ack` — 在三个时点以子进程硬杀
  （退出码 42）：前两者回滚干净（无半成品结果）、重启后该页恰好重抓
  1 次；`post_commit` 提交已持久化、重启后**不再重抓**；三种情况最终
  任务集合与采集结果均与无故障运行完全一致，不漏记录、不重复落库。

CLI 实测输出（3 worker）：`tasks: total=33 done=33 pending=0 leased=0
failed=0`，`result rows: 33`，`VERDICT: COMPLETE`。

## 未完成项 / 已知限制

- 站点为 HTTP/1.1 单机模拟，未实现 HTTPS、重定向、robots 等。
- 租约无心跳续期：超长页面（超过 `lease_seconds`）可能被重领导致重复
  抓取（结果仍唯一，靠 fencing 保证）；默认 30s 对本场景足够。
- 调度器只负责拉起与等待 worker 进程，未做 worker 崩溃后的自动重启
  （重新执行 `run` 即可恢复）；未做跨机器分布式部署。

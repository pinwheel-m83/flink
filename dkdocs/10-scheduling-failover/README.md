# 10 — Scheduling & Failover

> **다루는 영역**: SchedulerNG (Adaptive vs Default), Failover 전략. 본인 환경의 AdaptiveScheduler가 메인.
> **선행**: [`../03-graph-transformation/03-execution-graph.md`](../03-graph-transformation/03-execution-graph.md), [`../04-runtime-architecture/03-job-master.md`](../04-runtime-architecture/03-job-master.md)

## 문서 목록

| # | 문서 | 한 줄 |
|---|------|------|
| 01 | [`01-adaptive-scheduler.md`](./01-adaptive-scheduler.md) | ★ AdaptiveScheduler 상태 머신 + Operator autoscaler 연동 |
| 02 | [`02-default-scheduler-comparison.md`](./02-default-scheduler-comparison.md) | DefaultScheduler vs Adaptive (region failover 등) |
| 03 | [`03-failover-strategies.md`](./03-failover-strategies.md) | RestartStrategy + FailoverStrategy 정책 |

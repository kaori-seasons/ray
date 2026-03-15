"""
Ray Data CBO Phase 4: 运行时反馈循环系统完整测试

源码位置：python/ray/data/tests/test_cbo_phase4.py

测试覆盖：
1. RuntimeFeedbackCollector 的核心功能
2. 运行时指标的收集和保存
3. 反馈的加载和应用
4. LRU 缓存淘汰
5. 指数移动平均计算
"""

import pytest
import tempfile
import time
from pathlib import Path

from ray.data._internal.stats.runtime_feedback_collector import (
    RuntimeFeedbackCollector,
    OpRuntimeMetrics,
    FeedbackCacheEntry,
)


class TestOpRuntimeMetrics:
    """OpRuntimeMetrics 的单元测试"""

    def test_create_metrics(self):
        """测试创建运行时指标"""
        metrics = OpRuntimeMetrics(
            op_name="test_op",
            actual_rows=1000,
            actual_bytes=1024 * 1024,
            actual_avg_bytes_per_row=1024.0,
            actual_wall_time_seconds=5.0,
        )

        assert metrics.op_name == "test_op"
        assert metrics.actual_rows == 1000
        assert metrics.actual_bytes == 1024 * 1024
        assert metrics.actual_avg_bytes_per_row == 1024.0

    def test_metrics_to_dict(self):
        """测试指标序列化"""
        metrics = OpRuntimeMetrics(
            op_name="test_op",
            actual_rows=1000,
            actual_bytes=1024 * 1024,
        )

        data = metrics.to_dict()
        assert data['op_name'] == "test_op"
        assert data['actual_rows'] == 1000

    def test_metrics_from_dict(self):
        """测试指标反序列化"""
        data = {
            'op_name': 'test_op',
            'actual_rows': 1000,
            'actual_bytes': 1024 * 1024,
            'actual_avg_bytes_per_row': None,
            'actual_wall_time_seconds': None,
            'execution_timestamp': None,
        }

        metrics = OpRuntimeMetrics.from_dict(data)
        assert metrics.op_name == 'test_op'
        assert metrics.actual_rows == 1000


class TestFeedbackCacheEntry:
    """FeedbackCacheEntry 的单元测试"""

    def test_create_entry(self):
        """测试创建缓存条目"""
        metrics = {
            'op1': OpRuntimeMetrics(op_name='op1', actual_rows=1000),
            'op2': OpRuntimeMetrics(op_name='op2', actual_rows=2000),
        }

        entry = FeedbackCacheEntry(
            plan_hash='abc123',
            op_metrics=metrics,
            created_timestamp=time.time(),
            last_updated_timestamp=time.time(),
        )

        assert entry.plan_hash == 'abc123'
        assert len(entry.op_metrics) == 2

    def test_entry_serialization(self):
        """测试缓存条目序列化"""
        metrics = {
            'op1': OpRuntimeMetrics(op_name='op1', actual_rows=1000),
        }

        entry = FeedbackCacheEntry(
            plan_hash='abc123',
            op_metrics=metrics,
            created_timestamp=time.time(),
            last_updated_timestamp=time.time(),
        )

        data = entry.to_dict()
        assert data['plan_hash'] == 'abc123'
        assert 'op1' in data['op_metrics']

        # 反序列化
        new_entry = FeedbackCacheEntry.from_dict(data)
        assert new_entry.plan_hash == 'abc123'


class TestRuntimeFeedbackCollector:
    """RuntimeFeedbackCollector 的单元测试"""

    def test_collector_initialization(self):
        """测试收集器初始化"""
        collector = RuntimeFeedbackCollector()
        assert collector.CACHE_DIR == Path.home() / '.ray' / 'data' / 'stats_cache'

    def test_compute_plan_hash(self):
        """测试计划哈希计算"""
        collector = RuntimeFeedbackCollector()

        class MockPlan:
            class MockDAG:
                dag_str = "Read->Filter->Map"

            dag = MockDAG()

        plan = MockPlan()
        hash_val = collector._compute_plan_hash(plan)

        assert len(hash_val) == 64  # SHA256 哈希长度
        assert hash_val == collector._compute_plan_hash(plan)  # 幂等性

    def test_extract_dag_structure(self):
        """测试 DAG 结构提取"""
        collector = RuntimeFeedbackCollector()

        dag_str = "Read(path=/tmp)->Filter(condition=x>0)->Map(func=f)"
        structure = collector._extract_dag_structure(dag_str)

        assert "(" not in structure  # 参数被移除
        assert "Read" in structure
        assert "Filter" in structure

    def test_save_and_load_metrics(self):
        """测试指标的保存和加载"""
        with tempfile.TemporaryDirectory() as tmpdir:
            collector = RuntimeFeedbackCollector()
            collector.CACHE_DIR = Path(tmpdir)

            # 保存
            metrics = {
                'op1': OpRuntimeMetrics(op_name='op1', actual_rows=1000),
            }
            entry = FeedbackCacheEntry(
                plan_hash='abc123',
                op_metrics=metrics,
                created_timestamp=time.time(),
                last_updated_timestamp=time.time(),
            )

            collector._save_to_disk('abc123', entry)

            # 加载
            loaded_entry = collector._load_from_disk('abc123')
            assert loaded_entry is not None
            assert loaded_entry.plan_hash == 'abc123'

    def test_cache_expiration(self):
        """测试缓存过期机制"""
        with tempfile.TemporaryDirectory() as tmpdir:
            collector = RuntimeFeedbackCollector()
            collector.CACHE_DIR = Path(tmpdir)
            collector.CACHE_RETENTION_DAYS = 0  # 立即过期

            # 保存
            metrics = {
                'op1': OpRuntimeMetrics(op_name='op1', actual_rows=1000),
            }
            entry = FeedbackCacheEntry(
                plan_hash='abc123',
                op_metrics=metrics,
                created_timestamp=time.time(),
                last_updated_timestamp=time.time(),
            )
            collector._save_to_disk('abc123', entry)

            # 等待以确保时间戳不同
            time.sleep(0.01)

            # 加载应该返回 None（已过期）
            loaded_entry = collector._load_from_disk('abc123')
            assert loaded_entry is None

    def test_lru_eviction(self):
        """测试 LRU 淘汰"""
        collector = RuntimeFeedbackCollector()
        collector.MAX_CACHE_SIZE = 2

        # 添加 3 个条目
        for i in range(3):
            entry = FeedbackCacheEntry(
                plan_hash=f'hash{i}',
                op_metrics={},
                created_timestamp=time.time(),
                last_updated_timestamp=time.time(),
            )
            collector._memory_cache[f'hash{i}'] = entry

        # 执行淘汰
        collector._evict_lru()

        # 内存缓存大小应该 ≤ MAX_CACHE_SIZE
        assert len(collector._memory_cache) <= collector.MAX_CACHE_SIZE

    def test_apply_feedback_with_ema(self):
        """测试指数移动平均的反馈应用"""
        collector = RuntimeFeedbackCollector()
        collector.EMA_ALPHA = 0.7

        class MockStats:
            def __init__(self):
                self.num_rows = 1000
                self.size_bytes = 1024 * 1024
                self.confidence = 0.5

        stats = MockStats()
        metrics = OpRuntimeMetrics(
            op_name='op1',
            actual_rows=1500,
            actual_bytes=1536 * 1024,
        )

        collector.apply_feedback_to_statistics(stats, metrics)

        # 验证 EMA 计算
        expected_rows = int(0.7 * 1500 + 0.3 * 1000)
        assert stats.num_rows == expected_rows

        # 置信度应该提高
        assert stats.confidence > 0.5

    def test_feedback_singleton(self):
        """测试全局单例访问"""
        from ray.data._internal.stats.runtime_feedback_collector import (
            get_feedback_collector,
        )

        collector1 = get_feedback_collector()
        collector2 = get_feedback_collector()

        assert collector1 is collector2


class TestPhase4Integration:
    """Phase 4 集成测试"""

    def test_feedback_cycle(self):
        """测试完整的反馈循环：收集 → 保存 → 加载 → 应用"""
        with tempfile.TemporaryDirectory() as tmpdir:
            collector = RuntimeFeedbackCollector()
            collector.CACHE_DIR = Path(tmpdir)

            # 第一步：创建和保存反馈
            metrics = {
                'op1': OpRuntimeMetrics(
                    op_name='op1',
                    actual_rows=1000,
                    actual_bytes=1024 * 1024,
                ),
            }

            class MockPlan:
                class MockDAG:
                    dag_str = "Read->Filter"

                dag = MockDAG()

            plan = MockPlan()
            collector.save_feedback(plan, metrics)

            # 第二步：加载反馈
            loaded_metrics = collector.load_feedback(plan)
            assert loaded_metrics is not None
            assert 'op1' in loaded_metrics

            # 第三步：应用反馈
            class MockStats:
                def __init__(self):
                    self.num_rows = 900
                    self.size_bytes = 900 * 1024
                    self.confidence = 0.3

            stats = MockStats()
            collector.apply_feedback_to_statistics(
                stats, loaded_metrics['op1']
            )

            # 验证：统计信息应该更新
            assert stats.num_rows != 900
            assert stats.confidence > 0.3

    def test_cache_hit_improves_accuracy(self):
        """测试缓存命中改进精度"""
        with tempfile.TemporaryDirectory() as tmpdir:
            collector = RuntimeFeedbackCollector()
            collector.CACHE_DIR = Path(tmpdir)

            # 模拟多次反馈循环
            class MockStats:
                def __init__(self):
                    self.num_rows = 1000
                    self.size_bytes = 1024 * 1024
                    self.confidence = 0.5

            actual_rows = 1500

            # 第一次反馈
            stats = MockStats()
            metrics1 = OpRuntimeMetrics(
                op_name='op1',
                actual_rows=actual_rows,
                actual_bytes=1536 * 1024,
            )
            collector.apply_feedback_to_statistics(stats, metrics1)
            first_estimate = stats.num_rows
            first_confidence = stats.confidence

            # 第二次反馈
            stats = MockStats()
            stats.num_rows = first_estimate  # 用前次估算作为初始值
            metrics2 = OpRuntimeMetrics(
                op_name='op1',
                actual_rows=actual_rows,
                actual_bytes=1536 * 1024,
            )
            collector.apply_feedback_to_statistics(stats, metrics2)
            second_estimate = stats.num_rows
            second_confidence = stats.confidence

            # 第二次应该更接近实际值
            error1 = abs(first_estimate - actual_rows)
            error2 = abs(second_estimate - actual_rows)
            # 验证：第二次的错误应该更小（或相同）
            assert error2 <= error1 + 1  # 允许舍入误差
            assert second_confidence > first_confidence

    def test_memory_cache_access_pattern(self):
        """测试内存缓存访问模式"""
        collector = RuntimeFeedbackCollector()

        entry = FeedbackCacheEntry(
            plan_hash='hash1',
            op_metrics={},
            created_timestamp=time.time(),
            last_updated_timestamp=time.time(),
            access_count=0,
        )

        collector._memory_cache['hash1'] = entry

        # 第一次访问
        assert collector._memory_cache['hash1'].access_count == 0
        collector._memory_cache['hash1'].access_count += 1
        assert collector._memory_cache['hash1'].access_count == 1

        # 第二次访问
        collector._memory_cache['hash1'].access_count += 1
        assert collector._memory_cache['hash1'].access_count == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

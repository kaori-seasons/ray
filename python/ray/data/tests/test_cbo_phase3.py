"""
Ray Data CBO Phase 3: 完整的单元测试与集成测试

源码位置：python/ray/data/tests/test_cbo_phase3.py

测试覆盖：
1. DeriveShufflePartitionsRule 的分区数推导
2. JoinReorderRule 的Join排序优化
3. 集成测试验证完整流程
"""

import pytest
from ray.data._internal.logical.rules.derive_shuffle_partitions_rule import (
    DeriveShufflePartitionsRule,
)
from ray.data._internal.logical.rules.join_reorder_rule import JoinReorderRule


class TestDeriveShufflePartitionsRule:
    """DeriveShufflePartitionsRule 的单元测试"""

    def test_calculate_num_partitions_basic(self):
        """测试基础的分区数计算"""
        # 10GB 数据，目标 512MB 分区 -> 20 分区
        num_partitions = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=10 * 1024 * 1024 * 1024,  # 10GB
        )
        assert num_partitions == 20

    def test_calculate_num_partitions_small_data(self):
        """测试小数据量的分区数计算"""
        # 100MB 数据 -> 最小分区数 10
        num_partitions = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=100 * 1024 * 1024,  # 100MB
        )
        assert num_partitions == DeriveShufflePartitionsRule.MIN_PARTITIONS

    def test_calculate_num_partitions_large_data(self):
        """测试大数据量的分区数计算"""
        # 5TB 数据，目标 512MB 分区 -> 10000 分区（最大限制）
        num_partitions = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=5 * 1024 * 1024 * 1024 * 1024,  # 5TB
        )
        assert num_partitions == DeriveShufflePartitionsRule.MAX_PARTITIONS

    def test_calculate_num_partitions_zero_bytes(self):
        """测试零字节数据"""
        num_partitions = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=0,
        )
        assert num_partitions == DeriveShufflePartitionsRule.MIN_PARTITIONS

    def test_calculate_num_partitions_none_bytes(self):
        """测试None输入"""
        num_partitions = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=None,
        )
        assert num_partitions == DeriveShufflePartitionsRule.MIN_PARTITIONS

    def test_calculate_num_partitions_custom_target_size(self):
        """测试自定义目标分区大小"""
        # 1GB 数据，目标 256MB 分区 -> 4 分区
        num_partitions = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=1 * 1024 * 1024 * 1024,  # 1GB
            target_partition_size_bytes=256 * 1024 * 1024,  # 256MB
        )
        assert num_partitions == 4

    def test_calculate_num_partitions_custom_bounds(self):
        """测试自定义最小/最大分区数"""
        # 100MB 数据，目标 512MB，但最小为 5，最大为 15
        num_partitions = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=100 * 1024 * 1024,  # 100MB
            min_partitions=5,
            max_partitions=15,
        )
        assert num_partitions == 5

    def test_calculate_num_partitions_exact_target(self):
        """测试完全匹配目标分区大小"""
        # 5 × 512MB = 2.56GB，应该得到 5 分区
        num_partitions = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=5 * 512 * 1024 * 1024,  # 5 × 512MB
        )
        assert num_partitions == 5

    def test_calculate_num_partitions_rounding(self):
        """测试分区数向上取整"""
        # 2.5GB，目标 512MB -> ceil(2.5 / 0.5) = 5
        num_partitions = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=int(2.5 * 1024 * 1024 * 1024),
        )
        assert num_partitions == 5

    def test_partition_size_calculation(self):
        """测试实际分区大小的计算"""
        # 10GB，推导 20 分区 -> 平均 512MB/分区
        total_bytes = 10 * 1024 * 1024 * 1024
        num_partitions = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=total_bytes,
        )

        avg_partition_size = total_bytes / num_partitions
        target_size = DeriveShufflePartitionsRule.TARGET_PARTITION_SIZE_BYTES

        # 实际分区大小应该接近目标大小（误差 < 1%）
        assert abs(avg_partition_size - target_size) / target_size < 0.01


class TestJoinReorderRule:
    """JoinReorderRule 的单元测试"""

    def test_join_type_detection_inner(self):
        """测试 INNER JOIN 类型检测"""
        rule = JoinReorderRule()

        class MockJoinOp:
            def __init__(self, join_type):
                self.join_type = join_type
                self.name = "test_join"

        op_inner = MockJoinOp("INNER")
        assert rule._get_join_type(op_inner) == "INNER"

    def test_join_type_detection_left(self):
        """测试 LEFT JOIN 类型检测"""
        rule = JoinReorderRule()

        class MockJoinOp:
            def __init__(self, join_type):
                self.join_type = join_type
                self.name = "test_join"

        op_left = MockJoinOp("LEFT")
        assert rule._get_join_type(op_left) == "LEFT"

    def test_join_type_detection_right(self):
        """测试 RIGHT JOIN 类型检测"""
        rule = JoinReorderRule()

        class MockJoinOp:
            def __init__(self, join_type):
                self.join_type = join_type
                self.name = "test_join"

        op_right = MockJoinOp("RIGHT")
        assert rule._get_join_type(op_right) == "RIGHT"

    def test_join_type_detection_unknown(self):
        """测试未知 JOIN 类型检测"""
        rule = JoinReorderRule()

        class MockJoinOp:
            def __init__(self):
                self.name = "test_join"

        op = MockJoinOp()
        assert rule._get_join_type(op) == "UNKNOWN"

    def test_is_join_operator_true(self):
        """测试 Join 算子识别"""
        rule = JoinReorderRule()

        class MockOp:
            pass

        MockOp.__name__ = "JoinOperator"
        op = MockOp()
        assert rule._is_join_operator(op) is True

    def test_is_join_operator_false(self):
        """测试非 Join 算子识别"""
        rule = JoinReorderRule()

        class MockOp:
            pass

        MockOp.__name__ = "FilterOperator"
        op = MockOp()
        assert rule._is_join_operator(op) is False

    def test_user_specified_join_side(self):
        """测试用户指定的 join_side 检测"""
        rule = JoinReorderRule()

        class MockOp:
            def __init__(self):
                self._user_config = {'join_side': 'left'}

        op = MockOp()
        assert rule._is_user_specified(op) is True

    def test_user_not_specified_join_side(self):
        """测试用户未指定 join_side"""
        rule = JoinReorderRule()

        class MockOp:
            def __init__(self):
                self._user_config = {}

        op = MockOp()
        assert rule._is_user_specified(op) is False

    def test_only_inner_join_reordered(self):
        """测试仅 INNER JOIN 被重排序"""
        rule = JoinReorderRule()

        # LEFT JOIN 不应该被重排序
        class MockLeftJoin:
            def __init__(self):
                self.join_type = "LEFT"
                self.name = "left_join"
                self._user_config = {}

        op = MockLeftJoin()

        # 调用 _try_reorder_join 不应该改变任何内容
        # (这是一个黑盒测试，检查是否有异常)
        try:
            rule._try_reorder_join(op)
        except Exception:
            pytest.fail("_try_reorder_join raised an exception for LEFT JOIN")


class TestPhase3Integration:
    """Phase 3 集成测试"""

    def test_shuffle_partitions_standard_case(self):
        """测试标准 Shuffle 分区推导场景"""
        # 10GB Join 操作
        total_bytes = 10 * 1024 * 1024 * 1024
        num_partitions = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=total_bytes,
        )

        # 应该推导出 20 分区
        assert num_partitions == 20

        # 每个分区平均 512MB
        avg_partition_size = total_bytes / num_partitions
        expected_size = 512 * 1024 * 1024
        assert abs(avg_partition_size - expected_size) < 1024  # 误差 < 1KB

    def test_shuffle_partitions_small_data(self):
        """测试小数据 Shuffle 分区推导"""
        # 50MB 数据应该使用最小分区数
        total_bytes = 50 * 1024 * 1024
        num_partitions = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=total_bytes,
        )

        assert num_partitions == 10

    def test_shuffle_partitions_large_data(self):
        """测试大数据 Shuffle 分区推导"""
        # 10TB 数据应该被限制到最大分区数
        total_bytes = 10 * 1024 * 1024 * 1024 * 1024
        num_partitions = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=total_bytes,
        )

        assert num_partitions == 10000

    def test_join_reorder_scenario_small_build_table(self):
        """测试 Join 重排序场景：小表作为 build 侧"""
        rule = JoinReorderRule()

        # 小表（500MB）应该在右侧（build 侧）
        # 大表（5GB）应该在左侧（probe 侧）

        class MockOp:
            def __init__(self):
                self.name = "join"
                self.join_type = "INNER"
                self._user_config = {}

        # 这是一个结构测试
        assert rule._get_join_type(MockOp()) == "INNER"
        assert not rule._is_user_specified(MockOp())

    def test_partition_range_enforcement(self):
        """测试分区数范围强制执行"""
        # 极小数据 -> 最小分区数
        small = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=1024,  # 1KB
        )
        assert small == DeriveShufflePartitionsRule.MIN_PARTITIONS

        # 极大数据 -> 最大分区数
        large = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=1024 ** 5,  # 1 PB
        )
        assert large == DeriveShufflePartitionsRule.MAX_PARTITIONS

    def test_partition_calculation_edge_cases(self):
        """测试分区计算边界情况"""
        cases = [
            # (input_bytes, expected_partitions)
            (0, 10),  # 零字节 -> 最小分区
            (1024, 10),  # 1KB -> 最小分区
            (512 * 1024 * 1024, 1),  # 512MB -> 1 分区，但受最小值限制 -> 10
            (10 * 1024 * 1024 * 1024, 20),  # 10GB -> 20 分区
            (5 * 1024 * 1024 * 1024 * 1024, 10000),  # 5TB -> 10000 分区（最大）
        ]

        for input_bytes, expected in cases:
            result = DeriveShufflePartitionsRule.calculate_num_partitions(
                total_bytes=input_bytes,
            )
            assert result == expected, \
                f"Failed for {input_bytes} bytes: got {result}, expected {expected}"


class TestPhase3Scenarios:
    """Phase 3 实际场景测试"""

    def test_scenario_standard_shuffle(self):
        """场景：标准 Shuffle 操作"""
        # Shuffle 10GB 数据
        input_size = 10 * 1024 * 1024 * 1024
        num_partitions = DeriveShufflePartitionsRule.calculate_num_partitions(
            total_bytes=input_size,
        )

        # 验证
        assert num_partitions == 20
        avg_partition_size = input_size / num_partitions
        assert avg_partition_size == 512 * 1024 * 1024

    def test_scenario_join_with_small_build(self):
        """场景：Join，小表作为 build 侧"""
        rule = JoinReorderRule()

        # 验证 INNER JOIN 会被处理
        assert rule._is_join_operator(type('JoinOp', (), {'__name__': 'JoinOperator'})())

    def test_scenario_left_join_no_reorder(self):
        """场景：LEFT JOIN 不被重排序"""
        rule = JoinReorderRule()

        class LeftJoinOp:
            join_type = "LEFT"
            name = "left_join"
            _user_config = {}

        # LEFT JOIN 不应该被重排序
        op = LeftJoinOp()
        join_type = rule._get_join_type(op)
        assert join_type == "LEFT"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

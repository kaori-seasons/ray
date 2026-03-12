"""
Ray Data CBO Phase 1: LogicalOperator 接口扩展

源码位置：python/ray/data/_internal/logical/interfaces/logical_operator.py (partial)

功能：
1. 为 LogicalOperator 基类添加 infer_statistics() 方法
2. 为常见算子（Read, Filter, Project, Limit）提供默认实现

关键设计点：
- 保持向后兼容（新方法是可选的）
- 遵循现有的 infer_metadata() 和 infer_schema() 模式
- 统计不可用时返回 None（不是 0）
"""

from typing import Optional, TYPE_CHECKING
from abc import ABC

if TYPE_CHECKING:
    from ray.data._internal.stats.operator_statistics import OperatorStatistics


class LogicalOperator(ABC):
    """
    LogicalOperator 基类的接口扩展

    这个代码片段展示了如何在现有的 LogicalOperator 中添加统计推导能力。
    """

    def infer_statistics(self) -> Optional['OperatorStatistics']:
        """
        推导该算子输出的统计信息

        返回：
            OperatorStatistics 对象，包含该算子输出数据的统计特征
            如果统计不可用，返回 None

        说明：
            1. 这个方法由 CBO 在逻辑优化阶段调用
            2. 子类应该在适当时覆盖此方法以提供特定的统计推导逻辑
            3. 统计不可用时返回 None，不要返回默认或猜测的值
            4. 复杂的统计推导可能需要查询输入的统计信息

        示例：
            对于 Filter 算子，实现如下：
            ```python
            def infer_statistics(self):
                input_stats = self.input_dependencies[0].infer_statistics()
                if input_stats is None:
                    return None

                # 估算选择率
                selectivity = self._estimate_selectivity(self.predicate, input_stats)

                # 按选择率缩放
                return input_stats.scale(selectivity)
            ```
        """
        return None  # 默认实现：统计不可用


# ============================================================================
# 具体算子的统计推导实现
# ============================================================================

class ReadOperator(LogicalOperator):
    """
    Read 算子的统计推导

    说明：
    - Read 是数据统计的"源头"
    - 统计信息来自数据源的元数据（如 Parquet footer）
    - 是后续所有统计传播的基础
    """

    def infer_statistics(self) -> Optional['OperatorStatistics']:
        """
        从数据源元数据推导统计信息

        实现策略（三层）：
        Layer 1: Parquet footer（零成本）
            ├─ 读取行数、大小、列统计
            └─ 成本：0ms（已在现有代码路径中）

        Layer 2: 采样估算（低成本）
            ├─ 首批数据采样
            ├─ 估算 avg_row_bytes, 列分布
            └─ 成本：50-200ms（仅在不是 Parquet 时）

        Layer 3: 运行时反馈（学习过程）
            ├─ 前次执行的实际指标
            └─ 成本：缓存查询，<1ms

        实际实现在 parquet_datasource.py 中提供。
        """
        pass  # 实现在具体的 Parquet datasource 中


class FilterOperator(LogicalOperator):
    """Filter 算子的统计推导"""

    def infer_statistics(self) -> Optional['OperatorStatistics']:
        """
        Filter 的统计推导：基于选择率缩放输入统计

        逻辑：
        1. 获取输入的统计信息
        2. 基于谓词条件估算选择率
        3. 按选择率缩放输入的统计

        选择率的估算规则（见 operator_statistics.py）：
        - 等值谓词 (=): selectivity = 1 / NDV
        - 范围谓词 (>, <): selectivity = (range) / (max - min)
        - 复合谓词 (AND): selectivity = sel(A) × sel(B) （独立性假设）
        - 复合谓词 (OR): selectivity = 1 - (1-sel(A)) × (1-sel(B))

        关键特性：
        - 置信度递减：每一层的不确定性都会累积
        - 保守估计：无法估算时默认 selectivity = 0.5
        """
        pass  # 具体实现见 logical_operators/filter_operator.py


class ProjectOperator(LogicalOperator):
    """Project（列选择）算子的统计推导"""

    def infer_statistics(self) -> Optional['OperatorStatistics']:
        """
        Project 的统计推导：基于列数量缩减字节数

        逻辑：
        1. 获取输入的统计信息
        2. 计算选中列的平均大小
        3. 行数不变，字节数按列比例缩减

        公式：
        - output_rows = input_rows （不变）
        - output_size_bytes ≈ input_size_bytes × (selected_cols_bytes / all_cols_bytes)
        - avg_row_bytes = output_size_bytes / output_rows

        特殊情况：
        - 如果列统计不可用，无法精确计算，可考虑：
          ├─ 假设列均匀分布：output_size ≈ input_size × (num_selected / num_all)
          └─ 或返回 None（保守选择）
        """
        pass  # 具体实现见 logical_operators/projection_operator.py


class LimitOperator(LogicalOperator):
    """Limit 算子的统计推导"""

    def infer_statistics(self) -> Optional['OperatorStatistics']:
        """
        Limit 的统计推导：简单取 min

        逻辑：
        - output_rows = min(input_rows, limit)
        - size_bytes 按行数比例缩减

        实现很简单，但能显著改进后续算子的统计精度：
        - 经过 Limit 的数据，后续算子（如 Map, Filter）的统计更准确
        - 尤其对于 "Read().limit(100)" 这种小数据测试场景有重要意义
        """
        pass  # 具体实现见 logical_operators/limit_operator.py


class UnionOperator(LogicalOperator):
    """Union 算子的统计推导"""

    def infer_statistics(self) -> Optional['OperatorStatistics']:
        """
        Union 的统计推导：多路合并

        逻辑：
        - output_rows = sum(input_rows)
        - size_bytes = sum(input_size_bytes)
        - column_stats: 各分支都有的列才保留

        这是 OperatorStatistics.merge() 的直接应用。
        """
        pass  # 具体实现见 logical_operators/multi_to_one_operator.py


# ============================================================================
# 关键设计细节
# ============================================================================

"""
为什么不是所有算子都实现 infer_statistics()?

原因1：复杂度与收益不成正比
├─ 某些算子（如 Sort）的统计很难精确估算
├─ 实现的代价高（可能需要遍历数据）
└─ 对最终的 R 值推导影响不大（Phase 3 处理）

原因2：逐步演进的策略
├─ Phase 1：实现基础算子（Read, Filter, Project, Limit）
├─ Phase 2：实现中等复杂度算子（Join, Repartition）
└─ Phase 3：实现复杂算子（Sort, GroupBy）

原因3：默认行为是安全的
├─ 没有实现 infer_statistics() 的算子返回 None
├─ 上游算子见到 None 会停止统计传播
├─ 整个系统优雅降级到 RBO
└─ 不会产生更差的结果

这种设计遵循了面向对象的开闭原则：
- 对扩展开放（子类可以实现统计推导）
- 对修改关闭（不需要修改基类逻辑）
"""

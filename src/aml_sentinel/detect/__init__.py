"""Detection service: rolling transaction graph + typology rules."""

from aml_sentinel.detect.graph import DegreeStats, WindowedGraph
from aml_sentinel.detect.service import (
    DEFAULT_WINDOW,
    DetectionService,
    DetectionStats,
    DetectorSink,
)
from aml_sentinel.detect.typologies import (
    CycleDetection,
    Finding,
    HighValueDegreeOutlier,
    Structuring,
    Typology,
    default_typologies,
)

__all__ = [
    "DEFAULT_WINDOW",
    "CycleDetection",
    "DegreeStats",
    "DetectionService",
    "DetectionStats",
    "DetectorSink",
    "Finding",
    "HighValueDegreeOutlier",
    "Structuring",
    "Typology",
    "WindowedGraph",
    "default_typologies",
]

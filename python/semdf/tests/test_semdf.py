from __future__ import annotations

import pyarrow as pa
import pytest

from semdf import (
    GURU_ILLEGAL_AGG,
    GURU_POLYMORPHIC,
    MEASURE_IRI,
    SemdfError,
    check_aggregation,
    measure_metadata,
)


def test_arrow_round_trip_and_sum_refused() -> None:
    meta = measure_metadata(
        measure_iri="https://signals.zndx.org/sdg#Ricci",
        unit="1",
        quantity_kind="intensive",
        grain="graph×snapshot",
        aggregations=["AVG", "MIN", "MAX", "COUNT"],
        join_keys=["https://signals.zndx.org/sdg#EpochDay"],
    )
    field = pa.field("value", pa.float32(), metadata=meta)
    restored = {k.decode(): v.decode() for k, v in field.metadata.items()}
    assert restored[MEASURE_IRI] == "https://signals.zndx.org/sdg#Ricci"
    check_aggregation(field.metadata, "avg")
    with pytest.raises(SemdfError) as ei:
        check_aggregation(field.metadata, "SUM")
    assert ei.value.code == GURU_ILLEGAL_AGG


def test_count_always_ok() -> None:
    check_aggregation(None, "COUNT")


def test_polymorphic() -> None:
    with pytest.raises(SemdfError) as ei:
        check_aggregation({}, "SUM")
    assert ei.value.code == "#SL.00000001.NOSEMDF"

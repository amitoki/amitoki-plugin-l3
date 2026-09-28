"""固定した負荷と、各機能を一つずつ変える比較条件。"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Scenario:
    name: str
    scheduler: str = "priority"
    bulk_rate: int = 0
    paths: str = "1"
    delay_ms: int = 0
    loss: bool = False
    replica_budget: int = 30_000
    short_deadline_us: int = 20_000
    receiver_rate: int = 1000
    retries: int = 0
    router: bool = True
    clock_sync: bool = False
    clock_drift_ppm: int = 0
    stop_clock_replies: bool = False
    expect_unreachable: bool = False
    recover_clock_replies: bool = False


SCENARIOS = [
    Scenario("idle_fifo", scheduler="fifo"),
    Scenario("idle_priority"),
    Scenario("congested_fifo", scheduler="fifo", bulk_rate=500),
    Scenario("congested_priority", bulk_rate=500),
    Scenario("delayed_single", delay_ms=30),
    Scenario("delayed_dual", delay_ms=30, paths="1,2"),
    Scenario("delayed_no_replica_budget", delay_ms=30, paths="1,2", replica_budget=0),
    Scenario("primary_loss_dual", loss=True, paths="1,2"),
    Scenario("dual_duplicates", paths="1,2"),
    Scenario("receiver_limited", receiver_rate=20),
    Scenario("retry_delayed", delay_ms=8, retries=1, short_deadline_us=50_000),
    Scenario("missing_router", router=False),
    Scenario("clock_offsets", clock_sync=True),
    Scenario("clock_drift", clock_sync=True, clock_drift_ppm=200),
    Scenario("clock_asymmetric", clock_sync=True, delay_ms=1),
    Scenario("clock_holdover", clock_sync=True, stop_clock_replies=True),
    Scenario("clock_recovery", clock_sync=True, stop_clock_replies=True, recover_clock_replies=True),
    Scenario("clock_too_uncertain", clock_sync=True, delay_ms=10, expect_unreachable=True),
]

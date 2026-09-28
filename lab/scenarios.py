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
]

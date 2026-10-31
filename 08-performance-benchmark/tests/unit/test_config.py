"""Strict workload config and the claims a lane may not make (sections 3.1, 21.1).

Beyond parser strictness, this pins :meth:`WorkloadConfig.claim_limitations`,
which is where several adversarial cases from section 18.4 are caught *before* a
run starts: a query lane that is disabled cannot produce a p95 (E2E-38's
cousin), a one-minute steady phase is not a capacity claim (E2E-43), and
``acks=0`` is not a Kafka speedup (E2E-47).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from p08_bench.config import (
    CV_REFERENCE_PARTITIONS_PER_TOPIC,
    PROTOCOL_STEADY_SECONDS,
    ArrivalModel,
    CacheProfile,
    Profile,
    build_config,
    load_config,
)
from p08_bench.errors import ConfigError

from ..conftest import workload_dict


class TestStrictParsing:
    def test_a_complete_config_parses(self):
        config = build_config(workload_dict())
        assert config.run.id == "P08-TEST-0001"
        assert config.load.model is ArrivalModel.OPEN_LOOP
        assert config.query.cache_profile is CacheProfile.WARM

    def test_an_unknown_top_level_section_is_refused(self):
        raw = workload_dict()
        raw["tuning"] = {"linger_ms": 5}
        with pytest.raises(ConfigError, match="unknown top-level sections"):
            build_config(raw)

    def test_a_missing_section_is_refused(self):
        raw = workload_dict()
        del raw["safety"]
        with pytest.raises(ConfigError, match=r"missing \[safety\] section"):
            build_config(raw)

    def test_an_unknown_key_is_refused(self):
        # A typo would otherwise leave the intended setting at a default and the
        # run would quietly measure something else.
        raw = workload_dict(load={"linger_ms": 5})
        with pytest.raises(ConfigError, match="unknown keys"):
            build_config(raw)

    def test_a_missing_key_is_refused(self):
        raw = workload_dict()
        del raw["load"]["steady_seconds"]
        with pytest.raises(ConfigError, match="missing keys"):
            build_config(raw)

    def test_a_section_that_is_not_a_table_is_refused(self):
        raw = workload_dict()
        raw["load"] = 5
        with pytest.raises(ConfigError, match="must be a table"):
            build_config(raw)

    def test_a_boolean_is_not_an_integer(self):
        raw = workload_dict(load={"steady_seconds": True})
        with pytest.raises(ConfigError, match="must be an integer, not a boolean"):
            build_config(raw)

    def test_a_wrong_type_names_both_types(self):
        raw = workload_dict(load={"steady_seconds": "30"})
        with pytest.raises(ConfigError, match="must be int, got str"):
            build_config(raw)

    def test_an_unknown_enum_value_lists_the_allowed_set(self):
        raw = workload_dict(run={"profile": "PRODUCTION"})
        with pytest.raises(ConfigError, match="LAB_CAPACITY"):
            build_config(raw)


class TestValidation:
    def test_an_empty_run_id_is_refused(self):
        with pytest.raises(ConfigError, match="run.id may not be empty"):
            build_config(workload_dict(run={"id": "  "}))

    def test_zero_steady_seconds_is_refused(self):
        with pytest.raises(ConfigError, match="steady_seconds must be positive"):
            build_config(workload_dict(load={"steady_seconds": 0}))

    def test_a_negative_warmup_is_refused(self):
        with pytest.raises(ConfigError, match="may not be negative"):
            build_config(workload_dict(load={"warmup_seconds": -1}))

    def test_both_targets_zero_is_refused(self):
        with pytest.raises(ConfigError, match="nothing would be issued"):
            build_config(
                workload_dict(load={"market_tick_target_per_s": 0, "quote_target_per_s": 0})
            )

    def test_identical_tick_and_quote_topics_are_refused(self):
        with pytest.raises(ConfigError, match="must differ"):
            build_config(workload_dict(kafka={"quote_topic": "md.ticks.v1"}))

    def test_an_unknown_acks_value_is_refused(self):
        with pytest.raises(ConfigError, match="producer_acks"):
            build_config(workload_dict(kafka={"producer_acks": "quorum"}))

    def test_a_malformed_tick_quote_ratio_is_refused(self):
        with pytest.raises(ConfigError, match="4:1"):
            build_config(workload_dict(dataset={"tick_to_quote_ratio": "four to one"}))

    def test_a_non_integer_ratio_is_refused(self):
        with pytest.raises(ConfigError, match="integers"):
            build_config(workload_dict(dataset={"tick_to_quote_ratio": "4.5:1"}))

    def test_a_fill_percent_with_no_headroom_is_refused(self):
        with pytest.raises(ConfigError, match="headroom"):
            build_config(workload_dict(safety={"max_test_volume_fill_percent": 100}))

    def test_zero_concurrency_is_refused(self):
        with pytest.raises(ConfigError, match="concurrency must be positive"):
            build_config(workload_dict(query={"concurrency": 0}))


class TestDerivedProperties:
    def test_quotes_per_tick_comes_from_the_declared_ratio(self):
        config = build_config(workload_dict(dataset={"tick_to_quote_ratio": "4:1"}))
        assert config.dataset.quotes_per_tick == 0.25

    def test_a_ticks_only_ratio_gives_zero_quotes_per_tick(self):
        config = build_config(workload_dict(dataset={"tick_to_quote_ratio": "1:0"}))
        assert config.dataset.quotes_per_tick == 0.0

    def test_total_event_target_is_not_the_tick_target(self):
        # INV-12 in config form: 100k ticks + 25k quotes is 125k events.
        config = build_config(
            workload_dict(
                load={"market_tick_target_per_s": 100_000, "quote_target_per_s": 25_000}
            )
        )
        assert config.load.market_tick_target_per_s == 100_000
        assert config.load.total_event_target_per_s == 125_000

    def test_the_durability_contract_needs_acks_all_and_idempotence(self):
        assert build_config(workload_dict()).kafka.durability_contract_intact
        assert not build_config(
            workload_dict(kafka={"producer_acks": "0"})
        ).kafka.durability_contract_intact
        assert not build_config(
            workload_dict(kafka={"producer_idempotent": False})
        ).kafka.durability_contract_intact

    def test_the_cv_reference_topology_is_eight_partitions_per_topic(self):
        assert CV_REFERENCE_PARTITIONS_PER_TOPIC == 8
        assert build_config(workload_dict()).kafka.cv_reference_topology
        assert not build_config(
            workload_dict(kafka={"partitions_per_topic": 16})
        ).kafka.cv_reference_topology

    def test_the_steady_protocol_threshold_is_thirty_minutes(self):
        assert PROTOCOL_STEADY_SECONDS == 1800
        assert build_config(workload_dict(load={"steady_seconds": 1800})).load.meets_steady_protocol
        assert not build_config(workload_dict(load={"steady_seconds": 1799})).load.meets_steady_protocol

    def test_closed_loop_is_complementary_only(self):
        # ADR-01: closed-loop pacing depends on responses and so cannot provide
        # the primary latency distribution.
        config = build_config(workload_dict(load={"model": "closed_loop"}))
        assert not config.load.model.valid_for_primary_latency

    def test_a_disabled_query_lane_supports_no_latency_claim(self):
        assert not build_config(workload_dict()).query.supports_latency_claim
        assert build_config(workload_dict(query={"enabled": True})).query.supports_latency_claim

    def test_the_sample_floor_knows_about_nfr08(self):
        assert not build_config(workload_dict()).query.sample_floor_is_protocol_grade
        assert build_config(
            workload_dict(query={"min_complete_samples": 10_000})
        ).query.sample_floor_is_protocol_grade


class TestClaimLimitations:
    def test_the_test_lane_declares_all_of_its_limits(self):
        limits = build_config(workload_dict()).claim_limitations()
        assert any("proves no query p95" in text for text in limits)
        assert any("sustainable-capacity claim" in text for text in limits)

    def test_a_protocol_grade_lane_has_no_limitations(self):
        config = build_config(
            workload_dict(
                load={"steady_seconds": 1800},
                query={"enabled": True, "min_complete_samples": 10_000},
            )
        )
        assert config.claim_limitations() == []

    def test_acks_zero_is_flagged_as_a_weaker_durability_contract(self):
        # Case E2E-47: the speedup is real and the claim is not transferable.
        limits = build_config(workload_dict(kafka={"producer_acks": "0"})).claim_limitations()
        assert any("durability contract" in text for text in limits)

    def test_a_non_reference_topology_is_flagged(self):
        limits = build_config(workload_dict(kafka={"partitions_per_topic": 4})).claim_limitations()
        assert any("reference topology" in text for text in limits)

    def test_quotes_in_the_ticks_only_lane_are_flagged(self):
        # L07 exists so that ticks/s is unambiguous; adding quotes defeats it.
        limits = build_config(
            workload_dict(
                run={"lane": "L07"},
                load={"quote_target_per_s": 25_000, "steady_seconds": 1800},
                query={"enabled": True, "min_complete_samples": 10_000},
            )
        ).claim_limitations()
        assert any("ticks-only rate-identity lane" in text for text in limits)

    def test_a_closed_loop_lane_is_flagged(self):
        limits = build_config(
            workload_dict(
                load={"model": "closed_loop", "steady_seconds": 1800},
                query={"enabled": True, "min_complete_samples": 10_000},
            )
        ).claim_limitations()
        assert any("complementary only" in text for text in limits)


class TestDigest:
    def test_the_digest_is_stable_across_key_order(self):
        first = build_config(workload_dict()).sha256
        reordered = workload_dict()
        reordered["safety"] = dict(reversed(list(reordered["safety"].items())))
        assert build_config(reordered).sha256 == first

    def test_the_digest_changes_when_a_value_changes(self):
        # Case G01-08: a producer config changed after preflight must not look
        # like the same baseline.
        assert build_config(workload_dict()).sha256 != build_config(
            workload_dict(kafka={"producer_acks": "1"})
        ).sha256

    def test_the_digest_ignores_the_source_path(self):
        a = build_config(workload_dict(), source_path="/a.toml")
        b = build_config(workload_dict(), source_path="/b.toml")
        assert a.sha256 == b.sha256

    def test_the_digest_excludes_derived_properties(self):
        # Derived values are functions of the inputs; including them would make
        # the digest move when a derivation changes rather than the config.
        resolved = build_config(workload_dict()).resolved()
        assert "quotes_per_tick" not in resolved["dataset"]
        assert "meets_steady_protocol" not in resolved["load"]

    def test_as_dict_includes_the_digest_and_the_limitations(self):
        payload = build_config(workload_dict()).as_dict()
        assert payload["config_sha256"]
        assert isinstance(payload["claim_limitations"], list)


class TestFileLoading:
    def test_a_missing_file_is_a_config_error(self, tmp_path: Path):
        with pytest.raises(ConfigError, match="not found"):
            load_config(tmp_path / "nope.toml")

    def test_invalid_toml_is_a_config_error(self, tmp_path: Path):
        path = tmp_path / "bad.toml"
        path.write_text("[run\n", encoding="utf-8")
        with pytest.raises(ConfigError, match="not valid TOML"):
            load_config(path)

    def test_the_shipped_profiles_all_parse(self):
        # These are the only tests that read the shipped configs, and they assert
        # only that they are valid -- never a specific tuned value.
        configs = sorted((Path(__file__).resolve().parents[2] / "configs").glob("*.toml"))
        assert configs, "no shipped profiles found"
        for path in configs:
            config = load_config(path)
            assert config.run.profile in set(Profile)
            assert config.sha256

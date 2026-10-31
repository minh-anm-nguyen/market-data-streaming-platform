"""Config validation and dataset identity (section 6, cases C01-C14, D02-D05).

Validation is the project's first line of defence: every one of these must fail
*before* a socket is bound or a byte is written.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from conftest import config_dict, symbol_dict
from market_sim.config import (
    GENERATOR_VERSION,
    MAX_SEQUENCE,
    build_config,
    load_config,
    safe_resolve_under,
)
from market_sim.errors import ConfigError


def build(tmp_path: Path, **kwargs):
    return build_config(config_dict(**kwargs), base_dir=tmp_path)


def expect_error(tmp_path: Path, code: str, **kwargs):
    with pytest.raises(ConfigError) as exc:
        build(tmp_path, **kwargs)
    assert exc.value.code == code, f"expected {code}, got {exc.value.code}: {exc.value}"
    return exc.value


class TestHappyPath:
    def test_valid_config_resolves(self, tmp_path):
        cfg = build(tmp_path)
        assert cfg.run.mode == "record"
        assert cfg.symbol_order == ("EURUSD", "GBPUSD", "USDJPY")
        assert cfg.total_weight == 6
        assert cfg.expected_slots() == 100

    def test_root_dir_is_resolved_relative_to_the_config(self, tmp_path):
        assert build(tmp_path).file.root_dir == (tmp_path / "runs").resolve()

    def test_duration_is_converted_to_a_slot_count(self, tmp_path):
        cfg = build(tmp_path, count=None, duration_s=2.5, rate=1000)
        assert cfg.expected_slots() == 2500


class TestUnknownAndMissingKeys:
    def test_unknown_top_level_key(self, tmp_path):
        raw = config_dict()
        raw["telemetry"] = {"enabled": True}
        with pytest.raises(ConfigError) as exc:
            build_config(raw, base_dir=tmp_path)
        assert exc.value.code == "CONFIG_UNKNOWN_KEY"

    @pytest.mark.parametrize(
        "section,key",
        [
            ("run", "rate_per_second"),
            ("faults", "plan"),
        ],
    )
    def test_unknown_nested_key(self, tmp_path, section, key):
        raw = config_dict()
        raw[section][key] = 1
        with pytest.raises(ConfigError) as exc:
            build_config(raw, base_dir=tmp_path)
        assert exc.value.code == "CONFIG_UNKNOWN_KEY"

    def test_unknown_output_file_key(self, tmp_path):
        raw = config_dict()
        raw["output"]["file"]["fsync_evry_events"] = 1
        with pytest.raises(ConfigError) as exc:
            build_config(raw, base_dir=tmp_path)
        assert exc.value.code == "CONFIG_UNKNOWN_KEY"
        assert exc.value.context["section"] == "output.file"

    def test_unknown_symbol_key(self, tmp_path):
        raw = config_dict()
        raw["symbols"]["EURUSD"]["tick_size"] = 1
        with pytest.raises(ConfigError) as exc:
            build_config(raw, base_dir=tmp_path)
        assert exc.value.code == "CONFIG_UNKNOWN_KEY"

    def test_missing_required_key(self, tmp_path):
        raw = config_dict()
        del raw["run"]["seed"]
        with pytest.raises(ConfigError) as exc:
            build_config(raw, base_dir=tmp_path)
        assert exc.value.code == "CONFIG_MISSING_KEY"
        assert exc.value.context["key"] == "seed"

    def test_toml_syntax_error_is_reported_with_the_path(self, tmp_path):
        bad = tmp_path / "bad.toml"
        bad.write_text("run = [unclosed\n", encoding="utf-8")
        with pytest.raises(ConfigError) as exc:
            load_config(bad)
        assert exc.value.code == "CONFIG_TOML_SYNTAX"

    def test_missing_file(self, tmp_path):
        with pytest.raises(ConfigError) as exc:
            load_config(tmp_path / "nope.toml")
        assert exc.value.code == "CONFIG_FILE_NOT_FOUND"


class TestRunValidation:
    @pytest.mark.parametrize("rate", [0, -1, -1000])
    def test_rate_must_be_positive(self, tmp_path, rate):
        # Case T02: a zero or negative rate must be rejected, not turned into a
        # busy loop or a division by zero.
        expect_error(tmp_path, "CONFIG_OUT_OF_RANGE", rate=rate)

    def test_rate_ceiling(self, tmp_path):
        expect_error(tmp_path, "CONFIG_OUT_OF_RANGE", rate=10_000_001)

    def test_count_zero_is_a_valid_empty_dataset(self, tmp_path):
        # Case T03: count=0 means "a complete dataset with zero events".
        assert build(tmp_path, count=0).expected_slots() == 0

    @pytest.mark.parametrize("count", [-1, -1000])
    def test_negative_count_rejected(self, tmp_path, count):
        expect_error(tmp_path, "CONFIG_OUT_OF_RANGE", count=count)

    def test_count_and_duration_together_is_an_error(self, tmp_path):
        # Case T14: never silently pick one.
        expect_error(
            tmp_path, "CONFIG_COUNT_DURATION_CONFLICT", count=100, duration_s=10.0
        )

    def test_neither_count_nor_duration_is_an_error(self, tmp_path):
        expect_error(
            tmp_path, "CONFIG_COUNT_DURATION_MISSING", count=None, duration_s=None
        )

    @pytest.mark.parametrize("duration", [0.0, -1.0, 86_400 * 8])
    def test_duration_bounds(self, tmp_path, duration):
        expect_error(
            tmp_path, "CONFIG_OUT_OF_RANGE", count=None, duration_s=duration
        )

    def test_sequence_ceiling_is_enforced(self, tmp_path):
        # Case S10: refuse a run that would approach the sequence limit rather
        # than wrapping to zero later.
        expect_error(tmp_path, "CONFIG_OUT_OF_RANGE", count=MAX_SEQUENCE + 1)

    @pytest.mark.parametrize("pct", [-1, 101])
    def test_quote_ratio_bounds(self, tmp_path, pct):
        expect_error(tmp_path, "CONFIG_OUT_OF_RANGE", quote_ratio_pct=pct)

    @pytest.mark.parametrize("pct", [0, 100])
    def test_quote_ratio_extremes_are_valid(self, tmp_path, pct):
        # Case D09: 0% and 100% are legal and must not divide by zero.
        assert build(tmp_path, quote_ratio_pct=pct).run.quote_ratio_pct == pct

    def test_unknown_lag_policy(self, tmp_path):
        expect_error(tmp_path, "CONFIG_INVALID_CHOICE", on_scheduler_lag="panic")

    def test_unknown_bound_policy(self, tmp_path):
        expect_error(tmp_path, "CONFIG_INVALID_CHOICE", bound_policy="wrap")

    def test_unknown_mode(self, tmp_path):
        expect_error(tmp_path, "CONFIG_INVALID_CHOICE", mode="stream")

    def test_seed_must_be_a_non_negative_integer(self, tmp_path):
        expect_error(tmp_path, "CONFIG_OUT_OF_RANGE", seed=-1)

    def test_bool_is_not_an_integer_seed(self, tmp_path):
        raw = config_dict()
        raw["run"]["seed"] = True
        with pytest.raises(ConfigError) as exc:
            build_config(raw, base_dir=tmp_path)
        assert exc.value.code == "CONFIG_TYPE_ERROR"

    def test_drain_timeout_must_be_positive_and_bounded(self, tmp_path):
        expect_error(tmp_path, "CONFIG_OUT_OF_RANGE", shutdown_drain_timeout_s=0.0)
        expect_error(tmp_path, "CONFIG_OUT_OF_RANGE", shutdown_drain_timeout_s=3601.0)


class TestSymbolValidation:
    def test_empty_order_rejected(self, tmp_path):
        expect_error(tmp_path, "CONFIG_SYMBOL_ORDER_INVALID", order=[], symbols={})

    def test_duplicate_in_order(self, tmp_path):
        expect_error(
            tmp_path,
            "CONFIG_SYMBOL_DUPLICATE",
            order=["EURUSD", "EURUSD"],
            symbols={"EURUSD": symbol_dict()},
        )

    def test_symbol_in_order_but_not_configured(self, tmp_path):
        expect_error(
            tmp_path,
            "CONFIG_SYMBOL_UNDEFINED",
            order=["EURUSD", "XAUUSD"],
            symbols={"EURUSD": symbol_dict()},
        )

    def test_configured_symbol_missing_from_order(self, tmp_path):
        # Refusing to guess whether an unlisted table should be generated.
        expect_error(
            tmp_path,
            "CONFIG_SYMBOL_NOT_IN_ORDER",
            order=["EURUSD"],
            symbols={"EURUSD": symbol_dict(), "GBPUSD": symbol_dict()},
        )

    @pytest.mark.parametrize("name", ["", "EU", "e" * 17, "eurusd", "EUR USD", "EUR/USD"])
    def test_invalid_symbol_names(self, tmp_path, name):
        # Case D04: never invent a default scale for an unknown symbol.
        expect_error(
            tmp_path,
            "CONFIG_INVALID_SYMBOL",
            order=[name],
            symbols={name: symbol_dict()},
        )

    def test_price_range_must_not_be_empty(self, tmp_path):
        expect_error(
            tmp_path,
            "CONFIG_PRICE_RANGE_EMPTY",
            order=["EURUSD"],
            symbols={"EURUSD": symbol_dict(low=120000, high=100000)},
        )

    def test_initial_price_must_be_inside_the_band(self, tmp_path):
        expect_error(
            tmp_path,
            "CONFIG_INITIAL_PRICE_OUT_OF_RANGE",
            order=["EURUSD"],
            symbols={"EURUSD": symbol_dict(initial=99999)},
        )

    @pytest.mark.parametrize("field", ["price_scale", "min_price_ticks", "weight"])
    def test_non_positive_values_rejected(self, tmp_path, field):
        # Case D02: a zero or negative price must never be a valid configuration.
        sym = symbol_dict()
        sym[field] = 0
        expect_error(
            tmp_path, "CONFIG_OUT_OF_RANGE", order=["EURUSD"], symbols={"EURUSD": sym}
        )

    def test_inverted_spread_range(self, tmp_path):
        expect_error(
            tmp_path,
            "CONFIG_SPREAD_RANGE_EMPTY",
            order=["EURUSD"],
            symbols={"EURUSD": symbol_dict(min_spread=5, max_spread=1)},
        )

    def test_spread_wider_than_the_price_band(self, tmp_path):
        # Caught here so the generator can always satisfy both bid >= min and
        # ask <= max; case D05 would otherwise be unsatisfiable at runtime.
        expect_error(
            tmp_path,
            "CONFIG_SPREAD_EXCEEDS_BAND",
            order=["EURUSD"],
            symbols={
                "EURUSD": symbol_dict(
                    low=100000, high=100010, initial=100005, max_spread=50
                )
            },
        )

    def test_zero_spread_is_allowed_when_configured(self, tmp_path):
        cfg = build(
            tmp_path,
            order=["EURUSD"],
            symbols={"EURUSD": symbol_dict(min_spread=0, max_spread=0)},
        )
        assert cfg.symbol("EURUSD").max_spread_ticks == 0

    def test_unknown_symbol_lookup_raises(self, tmp_path):
        with pytest.raises(ConfigError) as exc:
            build(tmp_path).symbol("XAUUSD")
        assert exc.value.code == "CONFIG_UNKNOWN_SYMBOL"


class TestOutputValidation:
    @pytest.mark.parametrize("field", ["max_queue_events", "max_queue_bytes"])
    def test_zero_queue_budget_rejected(self, tmp_path, field):
        # Case R01: asyncio.Queue(maxsize=0) means unbounded, which is exactly
        # the failure mode this project exists to avoid.
        expect_error(tmp_path, "CONFIG_OUT_OF_RANGE", **{field: 0})

    def test_queue_smaller_than_one_record_rejected(self, tmp_path):
        expect_error(
            tmp_path,
            "CONFIG_QUEUE_TOO_SMALL",
            max_queue_bytes=2048,
            max_record_bytes=4096,
        )

    def test_max_record_bytes_floor(self, tmp_path):
        expect_error(tmp_path, "CONFIG_OUT_OF_RANGE", max_record_bytes=16)

    def test_negative_fsync_seconds_rejected(self, tmp_path):
        expect_error(tmp_path, "CONFIG_OUT_OF_RANGE", fsync_every_seconds=-1.0)

    def test_fsync_zero_disables_the_policy(self, tmp_path):
        cfg = build(tmp_path, fsync_every_events=0, fsync_every_seconds=0.0)
        assert cfg.file.fsync_every_events == 0

    @pytest.mark.parametrize("port", [0, -1, 65536, 999999])
    def test_invalid_ports_rejected(self, tmp_path, port):
        expect_error(tmp_path, "CONFIG_OUT_OF_RANGE", mode="both", tcp_port=port)

    def test_loopback_is_the_default_and_needs_no_opt_in(self, tmp_path):
        assert build(tmp_path, mode="both").tcp.host == "127.0.0.1"

    @pytest.mark.parametrize("host", ["0.0.0.0", "192.168.1.10", "::"])
    def test_public_bind_requires_explicit_opt_in(self, tmp_path, host):
        # NFR-09 and cases N16/C09: a client from the network can only reach
        # this process if someone opted in, because there is no auth or TLS
        # behind it. this process has no auth and no TLS, so a
        # non-loopback listener must be a deliberate decision.
        expect_error(
            tmp_path, "CONFIG_PUBLIC_BIND_NOT_ALLOWED", mode="both", tcp_host=host
        )

    def test_public_bind_allowed_when_opted_in(self, tmp_path):
        cfg = build(tmp_path, mode="both", tcp_host="0.0.0.0", allow_public_bind=True)
        assert cfg.tcp.host == "0.0.0.0"

    def test_startup_timeout_may_not_be_infinite(self, tmp_path):
        # Section 5.2: waiting for a client forever is not an option.
        expect_error(
            tmp_path, "CONFIG_OUT_OF_RANGE", mode="both", startup_timeout_s=0.0
        )

    def test_unknown_slow_client_policy(self, tmp_path):
        raw = config_dict(mode="both")
        raw["output"]["tcp"]["slow_client_policy"] = "drop_oldest"
        with pytest.raises(ConfigError) as exc:
            build_config(raw, base_dir=tmp_path)
        assert exc.value.code == "CONFIG_INVALID_CHOICE"


class TestModeSinkCoherence:
    def test_record_requires_the_file_sink(self, tmp_path):
        expect_error(
            tmp_path, "CONFIG_MODE_SINK_CONFLICT", mode="record", file_enabled=False
        )

    def test_record_must_not_enable_tcp(self, tmp_path):
        expect_error(
            tmp_path, "CONFIG_MODE_SINK_CONFLICT", mode="record", tcp_enabled=True
        )

    def test_serve_requires_the_tcp_sink(self, tmp_path):
        expect_error(
            tmp_path, "CONFIG_MODE_SINK_CONFLICT", mode="serve", tcp_enabled=False
        )

    def test_serve_must_not_enable_the_file_sink(self, tmp_path):
        expect_error(
            tmp_path, "CONFIG_MODE_SINK_CONFLICT", mode="serve", file_enabled=True
        )

    @pytest.mark.parametrize("disabled", ["file_enabled", "tcp_enabled"])
    def test_both_requires_both_sinks(self, tmp_path, disabled):
        expect_error(
            tmp_path, "CONFIG_MODE_SINK_CONFLICT", mode="both", **{disabled: False}
        )


class TestFaultsSection:
    def test_enabled_without_a_plan_path_is_an_error(self, tmp_path):
        expect_error(tmp_path, "CONFIG_FAULT_PLAN_MISSING", faults_enabled=True)

    def test_plan_path_is_resolved_relative_to_the_config(self, tmp_path):
        cfg = build(tmp_path, faults_enabled=True, plan_path="./plans/p.toml")
        assert cfg.faults.plan_path == (tmp_path / "plans" / "p.toml").resolve()


class TestDatasetIdentity:
    def test_same_config_same_dataset_id(self, tmp_path):
        assert build(tmp_path).dataset_id == build(tmp_path).dataset_id

    @pytest.mark.parametrize(
        "change",
        [
            {"seed": 20261031},
            {"start_time_ns": 1793322000000000001},
            {"rate": 2000},
            {"count": 101},
            {"quote_ratio_pct": 50},
            {"bound_policy": "clamp"},
        ],
    )
    def test_data_affecting_changes_change_the_id(self, tmp_path, change):
        # Case S07: one unit of seed is enough. Both datasets stay valid, but
        # they must never share an identity.
        assert build(tmp_path).dataset_id != build(tmp_path, **change).dataset_id

    def test_reordering_symbols_changes_the_id(self, tmp_path):
        # Case S08: the interleaving changes, so the dataset changes. Claiming
        # identical output would be the dishonest option.
        a = build(tmp_path)
        b = build(tmp_path, order=["GBPUSD", "EURUSD", "USDJPY"])
        assert a.dataset_id != b.dataset_id

    def test_changing_a_symbol_price_model_changes_the_id(self, tmp_path):
        a = build(tmp_path, order=["EURUSD"], symbols={"EURUSD": symbol_dict()})
        b = build(
            tmp_path, order=["EURUSD"], symbols={"EURUSD": symbol_dict(max_step=9)}
        )
        assert a.dataset_id != b.dataset_id

    @pytest.mark.parametrize(
        "change",
        [
            {"scheduler_batch_ms": 50},
            {"max_burst_slots": 99},
            {"max_queue_events": 999},
            {"max_queue_bytes": 2_000_000},
            {"fsync_every_events": 7},
            {"shutdown_drain_timeout_s": 11.0},
            {"max_scheduler_lag_ms": 5000},
        ],
    )
    def test_operational_knobs_do_not_change_the_id(self, tmp_path, change):
        # This is what makes the determinism property testable: the same dataset
        # must be reproducible while timing and buffering vary (section 9.2).
        assert build(tmp_path).dataset_id == build(tmp_path, **change).dataset_id

    def test_operational_knobs_do_change_the_config_hash(self, tmp_path):
        # dataset_id is about the data; config_sha256 is about the whole run.
        a = build(tmp_path)
        b = build(tmp_path, scheduler_batch_ms=50)
        assert a.config_sha256 != b.config_sha256

    def test_fault_plan_fingerprint_is_part_of_the_id(self, tmp_path):
        raw = config_dict(faults_enabled=True, plan_path="p.toml")
        a = build_config(raw, base_dir=tmp_path, fault_plan_fingerprint="aaaa")
        b = build_config(raw, base_dir=tmp_path, fault_plan_fingerprint="bbbb")
        assert a.dataset_id != b.dataset_id

    def test_fingerprint_is_ignored_when_faults_are_off(self, tmp_path):
        a = build_config(config_dict(), base_dir=tmp_path, fault_plan_fingerprint="x")
        b = build_config(config_dict(), base_dir=tmp_path, fault_plan_fingerprint="y")
        assert a.dataset_id == b.dataset_id

    def test_generator_version_is_recorded_in_the_identity(self, tmp_path):
        assert build(tmp_path).data_dict()["generator_version"] == GENERATOR_VERSION

    def test_dataset_id_shape(self, tmp_path):
        ds = build(tmp_path).dataset_id
        assert ds.startswith("ds-") and len(ds) == 19

    def test_seed_is_always_present_in_the_resolved_config(self, tmp_path):
        # Case C02: the manifest must be able to record the seed, always.
        assert build(tmp_path).data_dict()["seed"] == 20261030


class TestPathSafety:
    def test_path_inside_root_is_accepted(self, tmp_path):
        target = tmp_path / "runs" / "a" / "events.jsonl"
        target.parent.mkdir(parents=True)
        target.write_text("x", encoding="utf-8")
        assert safe_resolve_under(target, [tmp_path]) == target.resolve()

    def test_traversal_outside_root_is_rejected(self, tmp_path):
        # Case C06: the check is on the resolved real path, not the spelling.
        with pytest.raises(ConfigError) as exc:
            safe_resolve_under(tmp_path / ".." / "escaped.jsonl", [tmp_path / "runs"])
        assert exc.value.code == "CONFIG_PATH_OUTSIDE_ROOT"

    def test_root_itself_is_accepted(self, tmp_path):
        assert safe_resolve_under(tmp_path, [tmp_path]) == tmp_path.resolve()


class TestFrozenConfig:
    def test_resolved_config_is_immutable(self, tmp_path):
        # Case C05: a run resolves its config once; changing the environment or
        # the file afterwards must require a new run.
        cfg = build(tmp_path)
        with pytest.raises((AttributeError, TypeError)):
            cfg.run.seed = 1  # type: ignore[misc]

    def test_dicts_handed_out_are_copies(self, tmp_path):
        cfg = build(tmp_path)
        snapshot = cfg.data_dict()
        snapshot["seed"] = 999
        assert cfg.data_dict()["seed"] == 20261030


class TestDottedSymbolNames:
    """A dot is legal in a symbol name, but TOML needs it quoted.

    ``[US500.CFD]`` parses as a nested table, not as a symbol -- so the parser
    must reject the resulting shape rather than silently generating a symbol
    nobody asked for. The quoted form has to work, because CFD instruments are
    named this way.
    """

    def test_a_quoted_dotted_symbol_works(self, tmp_path):
        import tomllib

        raw = tomllib.loads(
            '\n'.join(
                [
                    '[run]',
                    'mode = "record"',
                    'seed = 1',
                    'start_time_ns = 0',
                    'count = 10',
                    'rate_per_sec = 1000',
                    'quote_ratio_pct = 80',
                    'scheduler_batch_ms = 10',
                    'max_burst_slots = 100',
                    'max_scheduler_lag_ms = 1000',
                    'on_scheduler_lag = "continue-degraded"',
                    'bound_policy = "reflect"',
                    'shutdown_drain_timeout_s = 5.0',
                    '[symbols]',
                    'order = ["US500.CFD"]',
                    '["symbols"."US500.CFD"]',
                    'price_scale = 10',
                    'initial_price_ticks = 58240',
                    'min_price_ticks = 45000',
                    'max_price_ticks = 70000',
                    'min_spread_ticks = 2',
                    'max_spread_ticks = 15',
                    'max_step_ticks = 25',
                    'weight = 1',
                    '[output.file]',
                    'enabled = true',
                    'root_dir = "runs"',
                    'max_queue_events = 100',
                    'max_queue_bytes = 65536',
                    'fsync_every_events = 0',
                    'fsync_every_seconds = 0',
                    'max_record_bytes = 4096',
                    'max_total_bytes = 1048576',
                    '[output.tcp]',
                    'enabled = false',
                    'host = "127.0.0.1"',
                    'port = 9009',
                    'max_clients = 1',
                    'max_queue_events = 100',
                    'max_queue_bytes = 65536',
                    'send_timeout_ms = 1000',
                    'slow_client_policy = "disconnect"',
                    'wait_for_client = false',
                    'startup_timeout_s = 5.0',
                    'allow_public_bind = false',
                    '[faults]',
                    'enabled = false',
                ]
            )
        )
        cfg = build_config(raw, base_dir=tmp_path)
        assert cfg.symbol_order == ("US500.CFD",)
        assert cfg.symbol("US500.CFD").price_scale == 10

    def test_an_unquoted_dotted_name_is_rejected_not_misread(self, tmp_path):
        raw = config_dict(order=["US500.CFD"], symbols={"US500": {"CFD": {}}})
        with pytest.raises(ConfigError) as exc:
            build_config(raw, base_dir=tmp_path)
        # The nested table is not a configured symbol, so the order entry has no
        # definition -- which is exactly the error to report.
        assert exc.value.code in (
            "CONFIG_SYMBOL_UNDEFINED",
            "CONFIG_SYMBOL_NOT_IN_ORDER",
        )

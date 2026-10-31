"""Repo artefacts: strict config loading and the SQL bank (groups AA, B, Q)."""

from __future__ import annotations

from pathlib import Path

import pytest

from _coverage import covers
from md_analytics.config import load_config
from md_analytics.errors import ConfigError
from md_analytics.sqlbank import DDL_FILES, QUERY_FILES, SqlBank

REPO = Path(__file__).resolve().parents[2]


def write_config(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "test.toml"
    path.write_text(text, encoding="utf-8")
    return path


def base_config_text() -> str:
    return (REPO / "config" / "dev.toml").read_text(encoding="utf-8")


class TestShippedConfigs:
    @covers("P06-AA-01")
    @pytest.mark.parametrize("name", ["dev", "integration", "capacity"])
    def test_every_shipped_profile_loads(self, name):
        config = load_config(REPO / "config" / f"{name}.toml")
        assert config.profile == name
        assert len(config.sha256()) == 64

    @covers("P06-AA-01")
    def test_the_profiles_have_distinct_hashes(self):
        hashes = {
            load_config(REPO / "config" / f"{n}.toml").sha256()
            for n in ("dev", "integration", "capacity")
        }
        assert len(hashes) == 3

    @covers("P06-O-05")
    def test_retention_ships_disabled(self, dev_config):
        assert dev_config.retention["enabled"] is False

    @covers("P06-Y-05")
    def test_the_resolved_config_redacts_the_password(self, tmp_path):
        text = base_config_text().replace('password = ""', 'password = "s3cret"')
        config = load_config(write_config(tmp_path, text))
        redacted = config.resolved()["clickhouse"]["password"]
        assert redacted.startswith("<redacted:sha256:")
        assert "s3cret" not in str(config.resolved())

    @covers("P06-Y-05")
    def test_the_hash_still_changes_when_the_password_changes(self, tmp_path):
        a = load_config(write_config(tmp_path, base_config_text()))
        b_text = base_config_text().replace('password = ""', 'password = "x"')
        b = load_config(write_config(tmp_path, b_text))
        assert a.sha256() != b.sha256(), (
            "redacting to a constant would make the config hash blind to which "
            "credentials a run used"
        )


class TestStrictness:
    @covers("P06-AA-02")
    def test_an_unknown_key_is_an_error_not_a_default(self, tmp_path):
        text = base_config_text().replace(
            "require_manifest_sha256 = true",
            "require_manifest_sha256 = true\nrequire_manifest_sha256_typo = false",
        )
        with pytest.raises(ConfigError) as exc:
            load_config(write_config(tmp_path, text))
        assert exc.value.code == "CONFIG_UNKNOWN_KEY"

    @covers("P06-AA-02")
    def test_an_unknown_section_is_an_error(self, tmp_path):
        with pytest.raises(ConfigError) as exc:
            load_config(write_config(tmp_path, base_config_text() + "\n[extras]\nx = 1\n"))
        assert exc.value.code == "CONFIG_UNKNOWN_KEY"

    @covers("P06-AA-02")
    def test_a_missing_section_is_an_error(self, tmp_path):
        text = base_config_text().split("[benchmark]")[0]
        with pytest.raises(ConfigError) as exc:
            load_config(write_config(tmp_path, text))
        assert exc.value.code == "CONFIG_MISSING_SECTION"

    @covers("P06-AA-05")
    @pytest.mark.parametrize(
        "flag",
        [
            "allow_direct_raw_to_mv",
            "allow_publish_on_unknown_ack",
            "allow_publish_with_conflicts",
        ],
    )
    def test_a_correctness_gate_cannot_be_disabled_by_config(self, tmp_path, flag):
        text = base_config_text().replace(f"{flag} = false", f"{flag} = true")
        with pytest.raises(ConfigError) as exc:
            load_config(write_config(tmp_path, text))
        assert exc.value.code == "CONFIG_FORBIDDEN_FLAG"

    @covers("P06-AA-05")
    @pytest.mark.parametrize(
        "flag",
        [
            "require_manifest_sha256",
            "require_p05_offset_cut",
            "require_independent_reconcile",
            "require_unique_source_identity",
        ],
    )
    def test_a_required_gate_cannot_be_turned_off(self, tmp_path, flag):
        text = base_config_text().replace(f"{flag} = true", f"{flag} = false")
        with pytest.raises(ConfigError) as exc:
            load_config(write_config(tmp_path, text))
        assert exc.value.code == "CONFIG_REQUIRED_FLAG"

    @covers("P06-D-04")
    def test_a_non_utc_timezone_is_refused(self, tmp_path):
        text = base_config_text().replace('timezone = "UTC"', 'timezone = "Asia/Ho_Chi_Minh"')
        with pytest.raises(ConfigError) as exc:
            load_config(write_config(tmp_path, text))
        assert exc.value.code == "CONFIG_BAD_TIMEZONE"

    @covers("P06-H-13")
    def test_a_bar_interval_other_than_one_minute_is_refused(self, tmp_path):
        text = base_config_text().replace(
            "bar_interval_ns = 60000000000", "bar_interval_ns = 300000000000"
        )
        with pytest.raises(ConfigError) as exc:
            load_config(write_config(tmp_path, text))
        assert exc.value.code == "CONFIG_BAD_BAR_INTERVAL"

    @covers("P06-R-09")
    def test_minute_alignment_cannot_be_switched_off(self, tmp_path):
        text = base_config_text().replace(
            "require_minute_alignment = true", "require_minute_alignment = false"
        )
        with pytest.raises(ConfigError) as exc:
            load_config(write_config(tmp_path, text))
        assert exc.value.code == "CONFIG_REQUIRED_FLAG"

    def test_a_live_publish_mode_is_refused_in_the_mvp(self, tmp_path):
        text = base_config_text().replace(
            'publish_mode = "sealed_snapshot_only"', 'publish_mode = "live_provisional"'
        )
        with pytest.raises(ConfigError) as exc:
            load_config(write_config(tmp_path, text))
        assert exc.value.code == "CONFIG_BAD_PUBLISH_MODE"

    @pytest.mark.parametrize("value", ["0", "-1", '"5"', "1.5"])
    def test_a_bad_numeric_limit_is_refused(self, tmp_path, value):
        text = base_config_text().replace(
            "max_bars_returned = 10000", f"max_bars_returned = {value}"
        )
        with pytest.raises(ConfigError):
            load_config(write_config(tmp_path, text))

    def test_a_bad_profile_name_is_refused(self, tmp_path):
        text = base_config_text().replace('profile = "dev"', 'profile = "production"')
        with pytest.raises(ConfigError) as exc:
            load_config(write_config(tmp_path, text))
        assert exc.value.code == "CONFIG_BAD_PROFILE"

    def test_a_missing_file_is_reported_clearly(self, tmp_path):
        with pytest.raises(ConfigError) as exc:
            load_config(tmp_path / "nope.toml")
        assert exc.value.code == "CONFIG_NOT_FOUND"

    def test_invalid_toml_is_reported_clearly(self, tmp_path):
        with pytest.raises(ConfigError) as exc:
            load_config(write_config(tmp_path, "[project\n"))
        assert exc.value.code == "CONFIG_INVALID_TOML"

    @covers("P06-AA-01")
    def test_a_benchmark_ladder_must_be_positive_integers(self, tmp_path):
        text = base_config_text().replace(
            "query_concurrency = [1, 4, 16, 32]", "query_concurrency = []"
        )
        with pytest.raises(ConfigError):
            load_config(write_config(tmp_path, text))


class TestSqlBank:
    @covers("P06-B-09", "P06-AD-05")
    def test_every_ddl_and_query_file_loads(self, sql_bank):
        assert len(sql_bank.ddl()) >= 4, "database, canonical, bars, MV"
        for name in DDL_FILES + QUERY_FILES:
            assert len(sql_bank.file_sha256(name)) == 64

    @covers("P06-AD-05")
    def test_the_bank_digest_is_stable_and_covers_every_file(self, sql_bank):
        first = sql_bank.bank_digest()
        assert first == SqlBank(REPO / "sql").bank_digest()
        digests = sql_bank.digests()
        assert set(digests) == set(DDL_FILES + QUERY_FILES) | {"bank"}

    def test_named_blocks_are_discoverable(self, sql_bank):
        names = sql_bank.names()
        for required in (
            "bars_1m",
            "canonical_range",
            "canonical_duplicate_identities",
            "raw_identity_conflicts",
            "active_parts_by_partition",
        ):
            assert required in names

    def test_an_unknown_block_name_lists_the_known_ones(self, sql_bank):
        with pytest.raises(ConfigError) as exc:
            sql_bank.get("no_such_query")
        assert exc.value.code == "SQL_UNKNOWN_NAME"
        assert "bars_1m" in str(exc.value)

    @covers("P06-I-14")
    def test_the_bar_query_finalizes_every_state_with_merge(self, sql_bank):
        sql = sql_bank.get("bars_1m")
        for combinator in ("argMinMerge", "maxMerge", "minMerge", "argMaxMerge", "countMerge"):
            assert combinator in sql
        assert "FINAL" not in sql, "ADR-009: *Merge is the read path, not FINAL"

    @covers("P06-Y-04", "P06-B-12")
    def test_every_client_facing_query_uses_typed_parameters(self, sql_bank):
        for name in ("bars_1m", "canonical_range"):
            sql = sql_bank.get(name)
            assert "{build_id:String}" in sql
            assert "{start_ns:Int64}" in sql, "nanoseconds must be typed Int64"
            assert "%s" not in sql and "format(" not in sql.lower()

    @covers("P06-R-02")
    def test_the_read_queries_avoid_select_star(self, sql_bank):
        for name in sql_bank.names():
            assert "SELECT *" not in sql_bank.get(name)

    @covers("P06-H-06")
    def test_no_sql_computes_a_sum_of_prices(self, sql_bank):
        for name in sql_bank.names():
            sql = sql_bank.get(name).lower()
            assert "sum(price" not in sql, (
                "a sum of prices has no meaning without trade size (case C-08)"
            )

    @covers("P06-I-05")
    def test_the_mv_uses_argmin_argmax_with_the_full_order_tuple(self, sql_bank):
        mv = " ".join(sql_bank.statements("004_mv_canonical_to_bars.sql"))
        assert "argMinState(price_ticks, tuple(event_time_ns, sequence))" in mv
        assert "argMaxState(price_ticks, tuple(event_time_ns, sequence))" in mv
        assert "any(" not in mv, "any() would make OPEN/CLOSE non-deterministic"

    @covers("P06-A-01")
    def test_the_mv_reads_canonical_and_never_raw(self, sql_bank):
        mv = " ".join(sql_bank.statements("004_mv_canonical_to_bars.sql"))
        assert "FROM md_analytics.canonical_ticks" in mv
        assert "market_events_raw" not in mv

    @covers("P06-K-08")
    def test_the_mv_is_created_without_populate(self, sql_bank):
        assert "POPULATE" not in " ".join(
            sql_bank.statements("004_mv_canonical_to_bars.sql")
        )

    @covers("P06-I-06")
    def test_the_bars_partition_expression_pins_utc(self, sql_bank):
        ddl = " ".join(sql_bank.statements("003_bars_1m_state.sql"))
        assert "'UTC'" in ddl

    @covers("P06-O-05")
    def test_no_ddl_ships_a_ttl_clause(self, sql_bank):
        for name in DDL_FILES:
            for statement in sql_bank.statements(name):
                assert "TTL" not in statement, (
                    "TTL is enabled only after the retention gate passes (NFR-11)"
                )

    @covers("P06-B-04")
    def test_p06_never_creates_the_p05_raw_table(self, sql_bank):
        for name in DDL_FILES:
            for statement in sql_bank.statements(name):
                if "CREATE TABLE" in statement:
                    assert "market_events_raw" not in statement

    def test_a_missing_sql_directory_fails_loudly(self, tmp_path):
        with pytest.raises(ConfigError) as exc:
            SqlBank(tmp_path)
        assert exc.value.code == "SQL_FILE_MISSING"

from __future__ import annotations

import csv
import hashlib
import json
import os
import threading
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

MARKET_ZONE = ZoneInfo('America/New_York')
VALID_STORAGE_MODES = {'csv', 'postgres', 'both'}


def normalize_storage_mode(raw_mode: str | None, default: str = 'postgres') -> str:
    mode = str(raw_mode or default).strip().lower()
    return mode if mode in VALID_STORAGE_MODES else default


def load_properties_file(path: Path) -> dict[str, str]:
    props: dict[str, str] = {}
    if not path.exists() or not path.is_file():
        return props

    for raw_line in path.read_text(encoding='utf-8').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        props[key.strip()] = value.strip()
    return props


def load_postgres_settings(root_dir: str | Path | None = None) -> dict[str, str]:
    base_dir = Path(root_dir or Path(__file__).resolve().parent)
    merged: dict[str, str] = {}
    merged.update(load_properties_file(base_dir / 'src' / 'main' / 'resources' / 'application.properties'))

    # Support git worktree layouts by scanning upward for runtime/postgres-local.properties.
    # Parent candidates are loaded first; closer paths override farther ones.
    candidates: list[Path] = []
    seen: set[Path] = set()
    for directory in [*base_dir.parents, base_dir]:
        candidate = (directory / 'runtime' / 'postgres-local.properties').resolve()
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.exists() and candidate.is_file():
            candidates.append(candidate)

    for candidate in candidates:
        merged.update(load_properties_file(candidate))

    url = (
        os.getenv('HARVEST_DB_URL')
        or os.getenv('SPRING_DATASOURCE_URL')
        or merged.get('spring.datasource.url', '')
    ).strip()
    username = (
        os.getenv('HARVEST_DB_USER')
        or os.getenv('SPRING_DATASOURCE_USERNAME')
        or merged.get('spring.datasource.username', '')
    ).strip()
    password = (
        os.getenv('HARVEST_DB_PASSWORD')
        or os.getenv('SPRING_DATASOURCE_PASSWORD')
        or merged.get('spring.datasource.password', '')
    )

    return {
        'url': _normalize_postgres_url(url),
        'username': username,
        'password': password,
    }


class PostgresHarvestStore:
    def __init__(self, root_dir: str | Path | None = None, app_name: str = 'harvester'):
        self.root_dir = Path(root_dir or Path(__file__).resolve().parent)
        self.app_name = app_name
        self.settings = load_postgres_settings(self.root_dir)
        self.url = self.settings.get('url', '').strip()
        self.username = self.settings.get('username', '').strip()
        self.password = self.settings.get('password', '')
        self._lock = threading.Lock()
        self._psycopg = None
        self._conn = None

        if not self.url:
            raise RuntimeError(
                'PostgreSQL harvesting is enabled, but no datasource URL was found. '
                'Set HARVEST_DB_URL or spring.datasource.url.'
            )

        self._ensure_driver()
        self._connect()
        self._ensure_schema()

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                finally:
                    self._conn = None

    def summary(self) -> str:
        user = self.username or '<default>'
        return f'postgres url={self.url} user={user}'

    def write_bar(self, symbol: str, bar_dt: datetime, row_dict: dict[str, Any]) -> bool:
        bar_time_utc = _to_utc_datetime(bar_dt)
        market_day = _to_market_date(bar_dt)
        payload = _json_payload(row_dict)
        sql = """
            INSERT INTO harvest_5s_bars (symbol, bar_time, market_day, payload, created_at, updated_at)
            VALUES (%s, %s, %s, %s::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (symbol, bar_time)
            DO UPDATE SET
                market_day = EXCLUDED.market_day,
                payload = EXCLUDED.payload,
                updated_at = CURRENT_TIMESTAMP
        """
        return self._execute(sql, (normalize_symbol(symbol), bar_time_utc, market_day, payload), 'bar')

    def write_tick(self, symbol: str, tick_dt: datetime, row_dict: dict[str, Any]) -> bool:
        tick_time_utc = _to_utc_datetime(tick_dt)
        market_day = _to_market_date(tick_dt)
        event_key = hashlib.sha1(
            f"{normalize_symbol(symbol)}|{tick_time_utc.isoformat()}|{row_dict.get('price', '')}|{row_dict.get('size', '')}|{row_dict.get('last_exchange', '')}|{row_dict.get('bid', '')}|{row_dict.get('ask', '')}".encode('utf-8')
        ).hexdigest()
        payload = _json_payload(row_dict)
        sql = """
            INSERT INTO harvest_live_ticks (event_key, symbol, tick_time, market_day, price, size, payload, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (event_key)
            DO UPDATE SET
                payload = EXCLUDED.payload,
                updated_at = CURRENT_TIMESTAMP
        """
        return self._execute(
            sql,
            (
                event_key,
                normalize_symbol(symbol),
                tick_time_utc,
                market_day,
                _safe_float(row_dict.get('price')),
                _safe_float(row_dict.get('size')),
                payload,
            ),
            'tick',
        )

    def write_news(self, symbol: str, published_dt: datetime, row_dict: dict[str, Any]) -> bool:
        published_utc = _to_utc_datetime(published_dt)
        market_day = _to_market_date(published_dt)
        event_key = hashlib.sha1(
            f"{normalize_symbol(symbol)}|{published_utc.isoformat()}|{row_dict.get('provider', '')}|{row_dict.get('article_id', '')}|{row_dict.get('headline', '')}".encode('utf-8')
        ).hexdigest()
        payload = _json_payload(row_dict)
        sql = """
            INSERT INTO harvest_news_events (
                event_key, symbol, published_ts, market_day, provider, article_id, headline, payload, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (event_key)
            DO UPDATE SET
                payload = EXCLUDED.payload,
                updated_at = CURRENT_TIMESTAMP
        """
        return self._execute(
            sql,
            (
                event_key,
                normalize_symbol(symbol),
                published_utc,
                market_day,
                str(row_dict.get('provider', '') or ''),
                str(row_dict.get('article_id', '') or ''),
                str(row_dict.get('headline', '') or ''),
                payload,
            ),
            'news',
        )

    def _ensure_driver(self) -> None:
        try:
            import psycopg  # type: ignore
        except ImportError as exc:  # pragma: no cover - exercised by runtime env
            raise RuntimeError(
                'PostgreSQL harvesting requires psycopg. Install requirements-harvester.txt or pip install "psycopg[binary]".'
            ) from exc
        self._psycopg = psycopg

    def _connect(self) -> None:
        if self._conn is not None and not getattr(self._conn, 'closed', True):
            return
        self._conn = self._psycopg.connect(
            self.url,
            user=self.username or None,
            password=self.password or None,
            autocommit=True,
            application_name=self.app_name,
        )

    def _ensure_schema(self) -> None:
        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS harvest_5s_bars (
                symbol TEXT NOT NULL,
                bar_time TIMESTAMPTZ NOT NULL,
                market_day DATE NOT NULL,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, bar_time)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_harvest_5s_bars_market_day ON harvest_5s_bars (market_day, symbol)",
            """
            CREATE TABLE IF NOT EXISTS harvest_live_ticks (
                event_key TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                tick_time TIMESTAMPTZ NOT NULL,
                market_day DATE NOT NULL,
                price DOUBLE PRECISION,
                size DOUBLE PRECISION,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_harvest_live_ticks_symbol_time ON harvest_live_ticks (symbol, tick_time)",
            """
            CREATE TABLE IF NOT EXISTS harvest_news_events (
                event_key TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                published_ts TIMESTAMPTZ NOT NULL,
                market_day DATE NOT NULL,
                provider TEXT,
                article_id TEXT,
                headline TEXT,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_harvest_news_events_symbol_time ON harvest_news_events (symbol, published_ts)",
        ]
        for ddl in ddl_statements:
            self._execute(ddl, (), 'schema', raise_on_failure=True)

    def _execute(self, sql: str, params: Iterable[Any], op_name: str, raise_on_failure: bool = False) -> bool:
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                with self._lock:
                    self._connect()
                    with self._conn.cursor() as cur:
                        cur.execute(sql, tuple(params))
                return True
            except Exception as exc:  # pragma: no cover - depends on runtime DB availability
                last_error = exc
                with self._lock:
                    if self._conn is not None:
                        try:
                            self._conn.close()
                        except Exception:
                            pass
                        self._conn = None
                if attempt == 0:
                    continue
        if raise_on_failure:
            raise RuntimeError(f'PostgreSQL {op_name} operation failed: {last_error}') from last_error
        print(f"[STORAGE] PostgreSQL {op_name} write failed: {last_error}")
        return False


class HarvestStorageManager:
    def __init__(self, mode: str, root_dir: str | Path | None = None):
        self.mode = normalize_storage_mode(mode)
        self.root_dir = Path(root_dir or Path(__file__).resolve().parent)
        self.csv_enabled = self.mode in {'csv', 'both'}
        self.postgres_enabled = self.mode in {'postgres', 'both'}
        self.postgres_store = PostgresHarvestStore(self.root_dir) if self.postgres_enabled else None

    def close(self) -> None:
        if self.postgres_store is not None:
            self.postgres_store.close()

    def summary(self) -> str:
        parts = [f'mode={self.mode}']
        if self.csv_enabled:
            parts.append('csv=enabled')
        if self.postgres_enabled and self.postgres_store is not None:
            parts.append(self.postgres_store.summary())
        return ' | '.join(parts)

    def write_bar(self, symbol: str, bar_dt: datetime, header: list[str], row_values: list[Any], csv_path: str | None = None) -> None:
        if self.csv_enabled and csv_path:
            append_csv_row(csv_path, row_values)
        if self.postgres_enabled and self.postgres_store is not None:
            self.postgres_store.write_bar(symbol, bar_dt, dict(zip(header, map(_json_safe_value, row_values))))

    def write_tick(self, symbol: str, tick_dt: datetime, header: list[str], row_values: list[Any], csv_path: str | None = None) -> None:
        if self.csv_enabled and csv_path:
            append_csv_row(csv_path, row_values)
        if self.postgres_enabled and self.postgres_store is not None:
            self.postgres_store.write_tick(symbol, tick_dt, dict(zip(header, map(_json_safe_value, row_values))))

    def write_news(self, symbol: str, published_dt: datetime, header: list[str], row_values: list[Any], csv_path: str | None = None) -> None:
        if self.csv_enabled and csv_path:
            append_csv_row(csv_path, row_values)
        if self.postgres_enabled and self.postgres_store is not None:
            self.postgres_store.write_news(symbol, published_dt, dict(zip(header, map(_json_safe_value, row_values))))


def append_csv_row(csv_path: str, row_values: list[Any]) -> None:
    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', newline='', encoding='utf-8') as handle:
        csv.writer(handle).writerow(row_values)


def normalize_symbol(symbol: str | None) -> str:
    return '' if symbol is None else str(symbol).strip().upper()


def _normalize_postgres_url(url: str) -> str:
    text = str(url or '').strip()
    if not text:
        return ''
    if text.startswith('jdbc:postgresql://'):
        return 'postgresql://' + text[len('jdbc:postgresql://'):]
    if text.startswith('postgresql://'):
        return text
    return text


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_payload(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=_json_default)


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _to_utc_datetime(raw_dt: datetime | None) -> datetime:
    if raw_dt is None:
        return datetime.now(timezone.utc)
    if raw_dt.tzinfo is None:
        return raw_dt.replace(tzinfo=timezone.utc)
    return raw_dt.astimezone(timezone.utc)


def _to_market_date(raw_dt: datetime | None) -> date:
    return _to_utc_datetime(raw_dt).astimezone(MARKET_ZONE).date()


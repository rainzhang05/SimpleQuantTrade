"""SQLite persistence for bot runtime state."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Iterator

from qtbot.ledger import compute_buy_update, compute_sell_update
from qtbot.strategy.signals import PositionSnapshot


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class StateStore:
    """Provides transactional reads/writes for runtime state."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path

    def initialize(self, *, initial_budget_cad: float) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        now = utc_now_iso()

        with self._connect() as conn:
            self._apply_schema(conn)
            row = conn.execute(
                """
                SELECT initial_budget_cad
                FROM bot_state
                WHERE id = 1
                """
            ).fetchone()

            if row is None:
                conn.execute(
                    """
                    INSERT INTO bot_state (
                        id,
                        initial_budget_cad,
                        bot_cash_cad,
                        realized_pnl_cad,
                        fees_paid_cad,
                        run_status,
                        last_command,
                        loop_count,
                        last_loop_started_at_utc,
                        last_loop_completed_at_utc,
                        last_event,
                        updated_at_utc
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        1,
                        initial_budget_cad,
                        initial_budget_cad,
                        0.0,
                        0.0,
                        "INITIALIZED",
                        "STOP",
                        0,
                        None,
                        None,
                        "state initialized",
                        now,
                    ),
                )
                self._insert_event(
                    conn,
                    event_type="STATE_INITIALIZED",
                    detail=f"initial_budget_cad={initial_budget_cad:.2f}",
                )
                return

            existing_budget = float(row["initial_budget_cad"])
            if abs(existing_budget - initial_budget_cad) > 1e-9:
                raise ValueError(
                    "Existing state uses a different initial budget. "
                    f"existing={existing_budget:.2f} requested={initial_budget_cad:.2f}"
                )

    def set_status(self, *, run_status: str, last_command: str, event_detail: str) -> None:
        now = utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE bot_state
                SET
                    run_status = ?,
                    last_command = ?,
                    last_event = ?,
                    updated_at_utc = ?
                WHERE id = 1
                """,
                (run_status, last_command, event_detail, now),
            )
            self._insert_event(conn, event_type="STATUS_CHANGED", detail=event_detail)

    def record_loop(
        self,
        *,
        last_command: str,
        loop_started_at_utc: str,
        loop_completed_at_utc: str,
        event_detail: str,
    ) -> int:
        now = utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE bot_state
                SET
                    run_status = ?,
                    last_command = ?,
                    loop_count = loop_count + 1,
                    last_loop_started_at_utc = ?,
                    last_loop_completed_at_utc = ?,
                    last_event = ?,
                    updated_at_utc = ?
                WHERE id = 1
                """,
                (
                    "RUNNING",
                    last_command,
                    loop_started_at_utc,
                    loop_completed_at_utc,
                    event_detail,
                    now,
                ),
            )
            row = conn.execute(
                """
                SELECT loop_count
                FROM bot_state
                WHERE id = 1
                """
            ).fetchone()
            self._insert_event(conn, event_type="LOOP_RECORDED", detail=event_detail)
            return int(row["loop_count"])

    def get_snapshot(self) -> dict[str, object] | None:
        if not self._db_path.exists():
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    initial_budget_cad,
                    bot_cash_cad,
                    realized_pnl_cad,
                    fees_paid_cad,
                    run_status,
                    last_command,
                    loop_count,
                    last_loop_started_at_utc,
                    last_loop_completed_at_utc,
                    last_event,
                    updated_at_utc
                FROM bot_state
                WHERE id = 1
                """
            ).fetchone()
            if row is None:
                return None
            return dict(row)

    def get_bot_cash_cad(self) -> float:
        if not self._db_path.exists():
            return 0.0
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT bot_cash_cad
                FROM bot_state
                WHERE id = 1
                """
            ).fetchone()
            if row is None:
                return 0.0
            return float(row["bot_cash_cad"])

    def get_positions(self) -> dict[str, PositionSnapshot]:
        if not self._db_path.exists():
            return {}
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, qty, avg_entry_price, entry_time, last_exit_time
                FROM positions
                """
            ).fetchall()
            result: dict[str, PositionSnapshot] = {}
            for row in rows:
                symbol = str(row["symbol"])
                result[symbol] = PositionSnapshot(
                    symbol=symbol,
                    qty=float(row["qty"]),
                    avg_entry_price=float(row["avg_entry_price"]),
                    entry_time=row["entry_time"],
                    last_exit_time=row["last_exit_time"],
                )
            return result

    def apply_buy_fill(
        self,
        *,
        symbol: str,
        qty: float,
        avg_price: float,
        fee_cad: float,
        filled_at_utc: str,
        order_id: int,
        ndax_symbol: str,
    ) -> None:
        if qty <= 0:
            raise ValueError("qty must be > 0 for buy fills.")
        if avg_price <= 0:
            raise ValueError("avg_price must be > 0 for buy fills.")
        if fee_cad < 0:
            raise ValueError("fee_cad must be >= 0 for buy fills.")

        now = utc_now_iso()
        with self._connect() as conn:
            state_row = conn.execute(
                """
                SELECT bot_cash_cad
                FROM bot_state
                WHERE id = 1
                """
            ).fetchone()
            if state_row is None:
                raise ValueError("bot_state row missing.")

            position = self._read_position(conn, symbol=symbol)
            update = compute_buy_update(
                current_qty=position.qty,
                current_avg_entry_price=position.avg_entry_price,
                fill_qty=qty,
                fill_price=avg_price,
                fee_cad=fee_cad,
            )

            new_cash = float(state_row["bot_cash_cad"]) + update.cash_delta_cad
            if new_cash < -1e-9:
                raise ValueError(
                    f"Buy fill would make bot cash negative for {symbol}: new_cash={new_cash:.8f}"
                )

            conn.execute(
                """
                UPDATE bot_state
                SET
                    bot_cash_cad = ?,
                    fees_paid_cad = fees_paid_cad + ?,
                    realized_pnl_cad = realized_pnl_cad + ?,
                    updated_at_utc = ?,
                    last_event = ?
                WHERE id = 1
                """,
                (
                    max(0.0, new_cash),
                    update.fee_delta_cad,
                    update.realized_pnl_delta_cad,
                    now,
                    f"buy_fill:{ndax_symbol}:{order_id}",
                ),
            )

            entry_time = position.entry_time or filled_at_utc
            conn.execute(
                """
                INSERT INTO positions (symbol, qty, avg_entry_price, entry_time, last_exit_time, updated_at_utc)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    qty = excluded.qty,
                    avg_entry_price = excluded.avg_entry_price,
                    entry_time = excluded.entry_time,
                    last_exit_time = excluded.last_exit_time,
                    updated_at_utc = excluded.updated_at_utc
                """,
                (
                    symbol,
                    update.new_qty,
                    update.new_avg_entry_price,
                    entry_time,
                    position.last_exit_time,
                    now,
                ),
            )

            self._insert_event(
                conn,
                event_type="BUY_FILL_APPLIED",
                detail=(
                    f"symbol={symbol} ndax_symbol={ndax_symbol} order_id={order_id} "
                    f"qty={qty:.12g} avg_price={avg_price:.12g} fee_cad={fee_cad:.12g}"
                ),
            )

    def apply_sell_fill(
        self,
        *,
        symbol: str,
        qty: float,
        avg_price: float,
        fee_cad: float,
        filled_at_utc: str,
        order_id: int,
        ndax_symbol: str,
    ) -> None:
        if qty <= 0:
            raise ValueError("qty must be > 0 for sell fills.")
        if avg_price <= 0:
            raise ValueError("avg_price must be > 0 for sell fills.")
        if fee_cad < 0:
            raise ValueError("fee_cad must be >= 0 for sell fills.")

        now = utc_now_iso()
        with self._connect() as conn:
            state_row = conn.execute(
                """
                SELECT bot_cash_cad
                FROM bot_state
                WHERE id = 1
                """
            ).fetchone()
            if state_row is None:
                raise ValueError("bot_state row missing.")

            position = self._read_position(conn, symbol=symbol)
            update = compute_sell_update(
                current_qty=position.qty,
                current_avg_entry_price=position.avg_entry_price,
                fill_qty=qty,
                fill_price=avg_price,
                fee_cad=fee_cad,
            )
            new_cash = float(state_row["bot_cash_cad"]) + update.cash_delta_cad

            conn.execute(
                """
                UPDATE bot_state
                SET
                    bot_cash_cad = ?,
                    fees_paid_cad = fees_paid_cad + ?,
                    realized_pnl_cad = realized_pnl_cad + ?,
                    updated_at_utc = ?,
                    last_event = ?
                WHERE id = 1
                """,
                (
                    max(0.0, new_cash),
                    update.fee_delta_cad,
                    update.realized_pnl_delta_cad,
                    now,
                    f"sell_fill:{ndax_symbol}:{order_id}",
                ),
            )

            if update.new_qty <= 0:
                conn.execute(
                    """
                    INSERT INTO positions (symbol, qty, avg_entry_price, entry_time, last_exit_time, updated_at_utc)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        qty = excluded.qty,
                        avg_entry_price = excluded.avg_entry_price,
                        entry_time = excluded.entry_time,
                        last_exit_time = excluded.last_exit_time,
                        updated_at_utc = excluded.updated_at_utc
                    """,
                    (
                        symbol,
                        0.0,
                        0.0,
                        None,
                        filled_at_utc,
                        now,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO positions (symbol, qty, avg_entry_price, entry_time, last_exit_time, updated_at_utc)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        qty = excluded.qty,
                        avg_entry_price = excluded.avg_entry_price,
                        entry_time = excluded.entry_time,
                        last_exit_time = excluded.last_exit_time,
                        updated_at_utc = excluded.updated_at_utc
                    """,
                    (
                        symbol,
                        update.new_qty,
                        update.new_avg_entry_price,
                        position.entry_time,
                        position.last_exit_time,
                        now,
                    ),
                )

            self._insert_event(
                conn,
                event_type="SELL_FILL_APPLIED",
                detail=(
                    f"symbol={symbol} ndax_symbol={ndax_symbol} order_id={order_id} "
                    f"qty={qty:.12g} avg_price={avg_price:.12g} fee_cad={fee_cad:.12g} "
                    f"realized_pnl_delta={update.realized_pnl_delta_cad:.12g}"
                ),
            )

    def reconcile_position(
        self,
        *,
        symbol: str,
        ndax_qty: float,
        reference_price: float | None,
        reconciled_at_utc: str,
        reason: str,
    ) -> bool:
        if ndax_qty < 0:
            raise ValueError("ndax_qty must be >= 0.")
        now = utc_now_iso()
        with self._connect() as conn:
            position = self._read_position(conn, symbol=symbol)
            if abs(position.qty - ndax_qty) <= 1e-9:
                return False

            if ndax_qty <= 1e-9:
                new_qty = 0.0
                new_avg_entry_price = 0.0
                new_entry_time = None
                new_last_exit_time = (
                    reconciled_at_utc if position.qty > 1e-9 else position.last_exit_time
                )
            else:
                new_qty = ndax_qty
                if position.qty > 1e-9 and position.avg_entry_price > 0:
                    new_avg_entry_price = position.avg_entry_price
                elif reference_price is not None and reference_price > 0:
                    new_avg_entry_price = float(reference_price)
                else:
                    new_avg_entry_price = 0.0
                new_entry_time = position.entry_time or reconciled_at_utc
                new_last_exit_time = position.last_exit_time

            conn.execute(
                """
                INSERT INTO positions (symbol, qty, avg_entry_price, entry_time, last_exit_time, updated_at_utc)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    qty = excluded.qty,
                    avg_entry_price = excluded.avg_entry_price,
                    entry_time = excluded.entry_time,
                    last_exit_time = excluded.last_exit_time,
                    updated_at_utc = excluded.updated_at_utc
                """,
                (
                    symbol,
                    new_qty,
                    new_avg_entry_price,
                    new_entry_time,
                    new_last_exit_time,
                    now,
                ),
            )
            conn.execute(
                """
                UPDATE bot_state
                SET
                    last_event = ?,
                    updated_at_utc = ?
                WHERE id = 1
                """,
                (
                    f"position_reconciled:{symbol}:{reason}",
                    now,
                ),
            )
            self._insert_event(
                conn,
                event_type="POSITION_RECONCILED",
                detail=(
                    f"symbol={symbol} reason={reason} internal_qty={position.qty:.12g} "
                    f"ndax_qty={ndax_qty:.12g} reference_price={reference_price}"
                ),
            )
            return True

    def cap_bot_cash(self, *, max_cash_cad: float, reason: str) -> bool:
        if max_cash_cad < 0:
            raise ValueError("max_cash_cad must be >= 0.")
        now = utc_now_iso()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT bot_cash_cad
                FROM bot_state
                WHERE id = 1
                """
            ).fetchone()
            if row is None:
                raise ValueError("bot_state row missing.")
            current_cash = float(row["bot_cash_cad"])
            if current_cash <= max_cash_cad + 1e-9:
                return False
            conn.execute(
                """
                UPDATE bot_state
                SET
                    bot_cash_cad = ?,
                    last_event = ?,
                    updated_at_utc = ?
                WHERE id = 1
                """,
                (
                    max_cash_cad,
                    f"bot_cash_capped:{reason}",
                    now,
                ),
            )
            self._insert_event(
                conn,
                event_type="BOT_CASH_CAPPED",
                detail=(
                    f"reason={reason} old_bot_cash_cad={current_cash:.12g} "
                    f"new_bot_cash_cad={max_cash_cad:.12g}"
                ),
            )
            return True

    def add_event(self, *, event_type: str, detail: str) -> None:
        with self._connect() as conn:
            self._insert_event(conn, event_type=event_type, detail=detail)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _apply_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                initial_budget_cad REAL NOT NULL,
                bot_cash_cad REAL NOT NULL,
                realized_pnl_cad REAL NOT NULL DEFAULT 0,
                fees_paid_cad REAL NOT NULL DEFAULT 0,
                run_status TEXT NOT NULL,
                last_command TEXT NOT NULL,
                loop_count INTEGER NOT NULL DEFAULT 0,
                last_loop_started_at_utc TEXT,
                last_loop_completed_at_utc TEXT,
                last_event TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            )
            """
        )
        self._ensure_column(
            conn,
            table_name="bot_state",
            column_name="realized_pnl_cad",
            ddl="ALTER TABLE bot_state ADD COLUMN realized_pnl_cad REAL NOT NULL DEFAULT 0",
        )
        self._ensure_column(
            conn,
            table_name="bot_state",
            column_name="fees_paid_cad",
            ddl="ALTER TABLE bot_state ADD COLUMN fees_paid_cad REAL NOT NULL DEFAULT 0",
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS state_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_time_utc TEXT NOT NULL,
                event_type TEXT NOT NULL,
                detail TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
                symbol TEXT PRIMARY KEY,
                qty REAL NOT NULL DEFAULT 0,
                avg_entry_price REAL NOT NULL DEFAULT 0,
                entry_time TEXT,
                last_exit_time TEXT,
                updated_at_utc TEXT NOT NULL
            )
            """
        )

    def _read_position(self, conn: sqlite3.Connection, *, symbol: str) -> PositionSnapshot:
        row = conn.execute(
            """
            SELECT symbol, qty, avg_entry_price, entry_time, last_exit_time
            FROM positions
            WHERE symbol = ?
            """,
            (symbol,),
        ).fetchone()
        if row is None:
            return PositionSnapshot(
                symbol=symbol,
                qty=0.0,
                avg_entry_price=0.0,
                entry_time=None,
                last_exit_time=None,
            )
        return PositionSnapshot(
            symbol=symbol,
            qty=float(row["qty"]),
            avg_entry_price=float(row["avg_entry_price"]),
            entry_time=row["entry_time"],
            last_exit_time=row["last_exit_time"],
        )

    def _ensure_column(
        self,
        conn: sqlite3.Connection,
        *,
        table_name: str,
        column_name: str,
        ddl: str,
    ) -> None:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        existing = {str(row["name"]) for row in rows}
        if column_name not in existing:
            conn.execute(ddl)

    def _insert_event(self, conn: sqlite3.Connection, *, event_type: str, detail: str) -> None:
        conn.execute(
            """
            INSERT INTO state_events (event_time_utc, event_type, detail)
            VALUES (?, ?, ?)
            """,
            (utc_now_iso(), event_type, detail),
        )

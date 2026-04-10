
import time
from market_data import now_local_str, epoch_to_local_str

RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"

def ha_color_style(color):
    if color == "BULL":
        return GREEN, "BULL"
    if color == "BEAR":
        return RED, "BEAR"
    return YELLOW, "DOJI"

def print_dashboard(app):
    now_str = now_local_str()
    market_stats = app.market.market_snapshot()

    conn_time = market_stats["last_ws_connect_time"]
    uptime = "-" if not conn_time else f"{int(time.time() - conn_time)}s"

    print("\033[2J\033[H", end="")
    print(f"  {BOLD}Dhan WS HA Paper Trader (Modular){RESET}  │  TF {app.strategy_tf}m  │  {now_str}  │  WS uptime: {uptime}")
    print(f"  {DIM}State: {app.state_path.name}  |  Log: {app.log_path.name}  |  Trades: {app.trade_log_path.name}{RESET}")
    print("═" * 168)
    print(
        f"  {'Symbol':16}  {'LTP':>10}  {'Pos':>6}  {'Entry':>10}  {'Pending':>6}  {'Trigger':>10}  "
        f"{'uPnL':>12}  {'rPnL':>12}  {'Lot':>5}  {'HA':>11}  {'HTF':>5}  {'Event':<42}"
    )
    print("─" * 168)

    total_unreal = 0.0
    total_real = 0.0

    for sec, inst in app.sec_to_inst.items():
        base = app.market.engines[sec].snapshot()
        strat = app.strategies[sec].snapshot()
        paper = app.paper[sec].snapshot(base["ltp"])
        prec = int(inst["display_prec"])

        ltp = base["ltp"]
        pos = paper["position_side"] or "-"
        pending = strat["pending_side"] or "-"
        trig = strat["pending_trigger"]
        entry = paper["entry_price"]
        lot = int(paper["lot_size"])
        unreal = float(paper["unrealized_pnl"])
        real = float(paper["realized_pnl"])
        total_unreal += unreal
        total_real += real

        ha_last = strat["ha_last"]
        if ha_last:
            ccode, ctext = ha_color_style(ha_last["color"])
            ha_text = f"{ctext} x{int(ha_last['streak'])}"
            htf_time = epoch_to_local_str(int(ha_last["bucket"]), False)
        else:
            ccode, ha_text, htf_time = YELLOW, "-", "-"

        up_col = GREEN if unreal >= 0 else RED
        rp_col = GREEN if real >= 0 else RED
        event_text = paper["last_event"] if paper["last_event"] != "-" else strat["last_event"]

        print(
            f"  {inst['name'][:16].ljust(16)}  "
            f"{(f'{ltp:.{prec}f}' if ltp is not None else '-'):>10}  "
            f"{pos:>6}  "
            f"{(f'{entry:.{prec}f}' if entry is not None else '-'):>10}  "
            f"{pending:>6}  "
            f"{(f'{trig:.{prec}f}' if trig is not None else '-'):>10}  "
            f"{up_col}{unreal:>12.2f}{RESET}  "
            f"{rp_col}{real:>12.2f}{RESET}  "
            f"{lot:>5}  "
            f"{ccode}{ha_text:>11}{RESET}  "
            f"{htf_time:>5}  "
            f"{str(event_text)[:42]:<42}"
        )

    print("─" * 168)
    total_u_col = GREEN if total_unreal >= 0 else RED
    total_r_col = GREEN if total_real >= 0 else RED
    print(f"  Total {'':<42}{total_u_col}{total_unreal:>12.2f}{RESET}  {total_r_col}{total_real:>12.2f}{RESET}")

    pkt = market_stats["packet_counts"]
    print(f"  Packets: ticker={pkt.get(2,0)}  prev_close={pkt.get(6,0)}  other={pkt.get('other',0)}  disconnect={pkt.get(50,0)}")
    if market_stats["last_ws_error"]:
        print(f"  {DIM}Last WS error: {market_stats['last_ws_error']}{RESET}")

    print(f"\n  {BOLD}Last 5 completed strategy candles (Heikin Ashi){RESET}")
    print(
        f"  {'Symbol':16}  {'Time':5}  {'HA Open':>10}  {'HA High':>10}  {'HA Low':>10}  "
        f"{'HA Close':>10}  {'Color':>7}  {'Streak':>6}"
    )
    print("  " + "─" * 96)

    any_hist = False
    for sec, inst in app.sec_to_inst.items():
        hist = app.strategies[sec].snapshot()["ha_history"][-5:]
        prec = int(inst["display_prec"])
        if not hist:
            continue
        any_hist = True
        first = True
        for row in reversed(hist):
            ccode, ctext = ha_color_style(row["color"])
            label = inst["name"][:16].ljust(16) if first else " " * 16
            first = False
            print(
                f"  {label}  {epoch_to_local_str(int(row['bucket']), False):>5}  "
                f"{float(row['open']):>10.{prec}f}  {float(row['high']):>10.{prec}f}  "
                f"{float(row['low']):>10.{prec}f}  {ccode}{float(row['close']):>10.{prec}f}{RESET}  "
                f"{ccode}{ctext:>7}{RESET}  {ccode}{int(row['streak']):>6}{RESET}"
            )
    if not any_hist:
        print(f"  {DIM}No completed strategy candle yet{RESET}")

    print(f"\n  {DIM}Commodities only • TF: 1/30/45/65/130 • Use --squareoff ALL or symbols at startup • Press Ctrl+C to stop.{RESET}")

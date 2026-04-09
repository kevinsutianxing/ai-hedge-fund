"""A-share data adapter for ai-hedge-fund.

Uses tushare (pro) + akshare (free fallback) to provide the same interface as api.py.
All functions return the same Pydantic models (FinancialMetrics, Price, LineItem, etc.).

Usage:
    DATA_PROVIDER=a_share TUSHARE_TOKEN=your_token python src/main.py --ticker 600519.SH,000858.SZ
"""

import logging
import os
from datetime import datetime, timedelta

import pandas as pd

from src.data.models import (
    CompanyFacts,
    CompanyFactsResponse,
    CompanyNews,
    CompanyNewsResponse,
    FinancialMetrics,
    FinancialMetricsResponse,
    InsiderTrade,
    InsiderTradeResponse,
    LineItem,
    LineItemResponse,
    Price,
    PriceResponse,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# tushare setup
# ---------------------------------------------------------------------------

_tushare = None


def _get_tushare():
    global _tushare
    if _tushare is not None:
        return _tushare
    import tushare as ts
    token = os.environ.get("TUSHARE_TOKEN", "")
    if token:
        ts.set_token(token)
        _tushare = ts.pro_api()
    else:
        logger.warning("TUSHARE_TOKEN not set — tushare pro unavailable, using akshare fallback")
    return _tushare


def _ts_code(ticker: str) -> str:
    """600519.SH → 600519.SH (passthrough, already tushare format)."""
    return ticker


def _stock_code(ticker: str) -> str:
    """600519.SH → 600519 (strip suffix for akshare)."""
    return ticker.split(".")[0]


def _report_end_date(end_date: str) -> str:
    """Convert YYYY-MM-DD to YYYYMMDD."""
    return end_date.replace("-", "")


# ---------------------------------------------------------------------------
# Prices
# ---------------------------------------------------------------------------


def get_prices(ticker: str, start_date: str, end_date: str, api_key: str = None) -> list[Price]:
    code = _stock_code(ticker)
    start = start_date.replace("-", "")
    end = end_date.replace("-", "")
    try:
        import akshare as ak
        df = ak.stock_zh_a_hist(
            symbol=code, period="daily",
            start_date=start, end_date=end, adjust="qfq",
        )
        if df is None or df.empty:
            return []
        result = []
        for _, row in df.iterrows():
            result.append(Price(
                open=float(row["开盘"]),
                close=float(row["收盘"]),
                high=float(row["最高"]),
                low=float(row["最低"]),
                volume=int(row["成交量"]),
                time=str(row["日期"])[:10],
            ))
        return result
    except Exception as e:
        logger.error(f"get_prices failed for {ticker}: {e}")
        return []


# ---------------------------------------------------------------------------
# Financial Metrics
# ---------------------------------------------------------------------------


def get_financial_metrics(
    ticker: str,
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
    api_key: str = None,
) -> list[FinancialMetrics]:
    ts_api = _get_tushare()
    ts_code = _ts_code(ticker)
    end_dt = _report_end_date(end_date)

    # Determine how far back to look (roughly limit * 4 months for quarterly data)
    start_dt = (datetime.strptime(end_dt, "%Y%m%d") - timedelta(days=limit * 120)).strftime("%Y%m%d")

    metrics_list = []

    if ts_api:
        try:
            # 1) fina_indicator — financial indicators (ROE, margins, ratios, etc.)
            df_ind = ts_api.fina_indicator(
                ts_code=ts_code,
                start_date=start_dt, end_date=end_dt,
            )

            # 2) daily_basic — market cap, PE, PB on end_date
            df_daily = ts_api.daily_basic(
                ts_code=ts_code, start_date=end_dt, end_date=end_dt,
                fields="ts_code,trade_date,close,pe,pb,ps,total_mv,circ_mv",
            )

            # 3) income — for revenue / operating income
            df_income = ts_api.income(
                ts_code=ts_code, start_date=start_dt, end_date=end_dt,
            )

            # 4) balancesheet — for debt/equity ratios
            df_bs = ts_api.balancesheet(
                ts_code=ts_code, start_date=start_dt, end_date=end_dt,
            )

            # 5) cashflow — for FCF
            df_cf = ts_api.cashflow(
                ts_code=ts_code, start_date=start_dt, end_date=end_dt,
            )

            # Build lookup dicts by end_date (dedup by end_date, keep first)
            ind_records = []
            if df_ind is not None and not df_ind.empty:
                seen = set()
                for r in df_ind.to_dict("records"):
                    d = r.get("end_date")
                    if d and d not in seen:
                        seen.add(d)
                        ind_records.append(r)
            ind_by_date = {r["end_date"]: r for r in ind_records}
            daily_map = {}
            if df_daily is not None and not df_daily.empty:
                row = df_daily.iloc[-1]
                daily_map = {
                    "total_mv": row.get("total_mv"),
                    "circ_mv": row.get("circ_mv"),
                    "close": row.get("close"),
                    "pe": row.get("pe"),
                    "pb": row.get("pb"),
                    "ps": row.get("ps"),
                }
            # Helper to dedup records by end_date
            def _dedup(df):
                if df is None or df.empty:
                    return {}
                seen, result = set(), []
                for r in df.to_dict("records"):
                    d = r.get("end_date")
                    if d and d not in seen:
                        seen.add(d)
                        result.append(r)
                return {r["end_date"]: r for r in result}

            income_by_date = _dedup(df_income)
            bs_by_date = _dedup(df_bs)
            cf_by_date = _dedup(df_cf)

            # Use fina_indicator dates as base
            dates = sorted(ind_by_date.keys(), reverse=True)[:limit]
            for idx, d in enumerate(dates):
                ind = ind_by_date[d]
                inc = income_by_date.get(d, {})
                bs = bs_by_date.get(d, {})
                cf = cf_by_date.get(d, {})

                # Market cap (tushare returns in 万元)
                total_mv = daily_map.get("total_mv")
                market_cap = float(total_mv * 10000) if total_mv and pd.notna(total_mv) else None

                # PE/PB/PS from daily_basic
                pe = daily_map.get("pe") if daily_map.get("pe") and pd.notna(daily_map.get("pe")) else None
                pb = daily_map.get("pb") if daily_map.get("pb") and pd.notna(daily_map.get("pb")) else None
                ps = daily_map.get("ps") if daily_map.get("ps") and pd.notna(daily_map.get("ps")) else None

                # Financial ratios from fina_indicator
                def _v(row, key):
                    val = row.get(key)
                    if val is None:
                        return None
                    if isinstance(val, float) and pd.isna(val):
                        return None
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        return None

                def _safe_div(a, b):
                    if a is not None and b is not None and b != 0:
                        return float(a) / float(b)
                    return None

            # Revenue growth (from fina_indicator revenue_yoy if available, else compute)
                rev = _v(inc, "revenue")
                net_profit = _v(inc, "net_profit")
                equity = _v(bs, "total_equity")
                total_assets = _v(bs, "total_assets")
                
                rev_yoy = _v(ind, "revenue_yoy")
                if rev_yoy:
                    rev_growth = rev_yoy / 100 if abs(rev_yoy) > 1 else rev_yoy
                else:
                    prev_rev = None
                    idx2 = dates.index(d) if d in dates else -1
                    if idx2 + 1 < len(dates):
                        prev_d2 = dates[idx2 + 1]
                        prev_inc = income_by_date.get(prev_d2, {})
                        prev_rev = _v(prev_inc, "revenue")
                    rev_growth = _safe_div(rev - prev_rev, prev_rev) if prev_rev and rev else None

                # Earnings growth
                net_yoy = _v(ind, "netprofit_yoy")
                if net_yoy:
                    earn_growth = net_yoy / 100 if abs(net_yoy) > 1 else net_yoy
                else:
                    prev_net = None
                    idx2 = dates.index(d) if d in dates else -1
                    if idx2 + 1 < len(dates):
                        prev_d2 = dates[idx2 + 1]
                        prev_inc = income_by_date.get(prev_d2, {})
                        prev_net = _v(prev_inc, "net_profit")
                    earn_growth = _safe_div(net_profit - prev_net, prev_net) if prev_net and net_profit else None

                # FCF = operating cash flow - capex
                ocf = _v(cf, "net_cash_flow_operate")
                capex = _v(cf, "cash_pay_for_asset")
                fcf = (ocf - capex) if ocf and capex else ocf

                # Book value growth
                prev_equity = None
                if idx + 1 < len(dates):
                    prev_d = dates[idx + 1]
                    prev_bs = bs_by_date.get(prev_d, {})
                    prev_equity = _v(prev_bs, "total_equity")
                bv_growth = _safe_div(equity - prev_equity, prev_equity) if prev_equity and equity else None

                # FCF growth
                fcf_growth = None
                if idx + 1 < len(dates):
                    prev_d = dates[idx + 1]
                    prev_cf = cf_by_date.get(prev_d, {})
                    prev_ocf = _v(prev_cf, "net_cash_flow_operate")
                    prev_capex = _v(prev_cf, "cash_pay_for_asset")
                    prev_fcf = (prev_ocf - prev_capex) if prev_ocf and prev_capex else prev_ocf
                    if prev_fcf and fcf:
                        fcf_growth = _safe_div(fcf - prev_fcf, prev_fcf)

                # Operating income growth
                op_income = _v(inc, "operate_profit")
                prev_op = None
                if idx + 1 < len(dates):
                    prev_d = dates[idx + 1]
                    prev_inc = income_by_date.get(prev_d, {})
                    prev_op = _v(prev_inc, "operate_profit")
                op_growth = _safe_div(op_income - prev_op, prev_op) if prev_op and op_income else None

                # EBITDA growth (approximate from operating profit growth)
                ebitda_growth = op_growth

                # Operating cash flow ratio
                ocf_ratio = _safe_div(ocf, _v(bs, "total_cur_liab"))

                # Debt ratios
                debt_to_eq = _safe_div(_v(bs, "total_liab"), equity)
                debt_to_assets = _safe_div(_v(bs, "total_liab"), _v(bs, "total_assets"))

                # Interest coverage (EBIT / interest expense, approximate)
                ebit = _v(inc, "operate_profit")  # approximate EBIT as operating profit
                interest_exp = None  # tushare fina_indicator doesn't easily expose this
                interest_coverage = None

                # Payout ratio
                net_profit_val = _v(inc, "net_profit")
                fcff = _v(ind, "fcff")
                payout = _safe_div(net_profit_val - fcf, net_profit_val) if net_profit_val and fcf else None

                # Shares outstanding (from balancesheet / share data — use total_mv / close as proxy)
                close_price = daily_map.get("close")
                shares_outstanding = None
                if close_price and market_cap and close_price > 0:
                    shares_outstanding = int(market_cap / float(close_price))

                # EV/EBITDA and EV/Revenue (approximate)
                ev = market_cap  # simplified, ignoring net debt
                ev_to_ebitda = _safe_div(ev, op_income) if ev and op_income else None
                ev_to_rev = _safe_div(ev, rev) if ev and rev else None

                # ROA
                total_assets = _v(bs, "total_assets")
                roa = _safe_div(net_profit, total_assets) if net_profit and total_assets else None

                # ROIC (simplified)
                invested_cap = total_assets - _v(bs, "total_cur_liab") if total_assets else None
                roic = _safe_div(net_profit, invested_cap) if net_profit and invested_cap and invested_cap > 0 else None

                # Enterprise value to free cash flow yield
                fcf_yield = _safe_div(fcf, market_cap) if fcf and market_cap else None

                # EPS growth
                eps = _v(ind, "eps")
                prev_eps = None
                if idx + 1 < len(dates):
                    prev_d = dates[idx + 1]
                    prev_ind = ind_by_date.get(prev_d, {})
                    prev_eps = _v(prev_ind, "eps")
                eps_growth = _safe_div(eps - prev_eps, prev_eps) if prev_eps and eps else None

                # Working capital turnover
                tca = _v(bs, "total_cur_assets")
                tcl = _v(bs, "total_cur_liab")
                wc = (tca - tcl) if tca and tcl else None
                wc_turnover = _safe_div(rev, wc) if rev and wc else None

                report_date_str = d
                metrics_list.append(FinancialMetrics(
                    ticker=ticker,
                    report_period=report_date_str,
                    period="quarterly",
                    currency="CNY",
                    market_cap=market_cap,
                    enterprise_value=ev,
                    price_to_earnings_ratio=pe,
                    price_to_book_ratio=pb,
                    price_to_sales_ratio=ps,
                    enterprise_value_to_ebitda_ratio=ev_to_ebitda,
                    enterprise_value_to_revenue_ratio=ev_to_rev,
                    free_cash_flow_yield=fcf_yield,
                    peg_ratio=None,  # requires EPS growth and PE
                    gross_margin=_v(ind, "grossprofit_margin") / 100 if _v(ind, "grossprofit_margin") else None,
                    operating_margin=_v(ind, "profit_to_gr") / 100 if _v(ind, "profit_to_gr") else None,
                    net_margin=_v(ind, "netprofit_margin") / 100 if _v(ind, "netprofit_margin") else None,
                    return_on_equity=_v(ind, "roe") / 100 if _v(ind, "roe") else None,
                    return_on_assets=roa,
                    return_on_invested_capital=roic,
                    asset_turnover=_v(ind, "assets_turn"),
                    inventory_turnover=_v(ind, "inv_turn"),
                    receivables_turnover=_v(ind, "ar_turn"),
                    days_sales_outstanding=_v(ind, "turn_days"),
                    operating_cycle=None,
                    working_capital_turnover=_v(ind, "wc_turn"),
                    current_ratio=_v(ind, "current_ratio"),
                    quick_ratio=_v(ind, "quick_ratio"),
                    cash_ratio=_v(ind, "cash_ratio"),
                    operating_cash_flow_ratio=ocf_ratio,
                    debt_to_equity=_v(ind, "debt_to_eq"),
                    debt_to_assets=_v(ind, "debt_to_assets") / 100 if _v(ind, "debt_to_assets") else None,
                    interest_coverage=interest_coverage,
                    revenue_growth=rev_growth,
                    earnings_growth=earn_growth,
                    book_value_growth=bv_growth,
                    earnings_per_share_growth=eps_growth,
                    free_cash_flow_growth=fcf_growth,
                    operating_income_growth=op_growth,
                    ebitda_growth=ebitda_growth,
                    payout_ratio=payout,
                    earnings_per_share=eps,
                    book_value_per_share=_v(ind, "bps"),
                    free_cash_flow_per_share=_v(ind, "ocfps"),
                ))
        except Exception as e:
            logger.error(f"tushare financial_metrics failed for {ticker}: {e}", exc_info=True)
            # Fall through to akshare
    else:
        logger.info(f"tushare unavailable, using akshare for {ticker}")

    # akshare fallback (basic financial summary)
    if not metrics_list:
        try:
            import akshare as ak
            code = _stock_code(ticker)
            df = ak.stock_financial_abstract_ths(symbol=code)
            if df is not None and not df.empty:
                # Use akshare's simplified financial data
                close_price = None
                # Get latest price
                price_df = ak.stock_zh_a_hist(symbol=code, period="daily",
                                              start_date=end_dt, end_date=end_dt, adjust="qfq")
                if price_df is not None and not price_df.empty:
                    close_price = float(price_df.iloc[-1]["收盘"])

                for _, row in df.head(limit).iterrows():
                    try:
                        eps_val = float(row.get("每股收益", 0) or 0)
                        bvps = float(row.get("每股净资产", 0) or 0)
                        roe_val = float(row.get("净资产收益率", 0) or 0) / 100 if row.get("净资产收益率") else None
                        gv = float(row.get("每股经营现金流", 0) or 0)
                        rev_val = float(row.get("营业收入", 0) or 0)
                        net_val = float(row.get("净利润", 0) or 0)
                        gm = float(row.get("毛利率", 0) or 0) / 100 if row.get("毛利率") else None
                        nm = float(row.get("净利率", 0) or 0) / 100 if row.get("净利率") else None
                        rp = str(row.get("报告期", ""))
                        rp_date = rp.replace("-", "")[:8] if rp else ""

                        market_cap = None
                        if close_price and bvps and bvps > 0:
                            # Rough estimate: need total shares
                            pass

                        metrics_list.append(FinancialMetrics(
                            ticker=ticker,
                            report_period=rp_date,
                            period="quarterly",
                            currency="CNY",
                            market_cap=market_cap,
                            return_on_equity=roe_val,
                            gross_margin=gm,
                            net_margin=nm,
                            earnings_per_share=eps_val if eps_val else None,
                            book_value_per_share=bvps if bvps else None,
                            free_cash_flow_per_share=gv if gv else None,
                        ))
                    except (ValueError, TypeError):
                        continue
        except Exception as e:
            logger.error(f"akshare financial_metrics fallback failed for {ticker}: {e}")

    return metrics_list


# ---------------------------------------------------------------------------
# Market Cap
# ---------------------------------------------------------------------------


def get_market_cap(ticker: str, end_date: str, api_key: str = None) -> float | None:
    code = _stock_code(ticker)
    end = end_date.replace("-", "")
    try:
        import akshare as ak
        # Use stock_individual_info_em for market cap
        df = ak.stock_individual_info_em(symbol=code)
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                if row.get("item") == "总市值":
                    val = row.get("value")
                    if val:
                        # akshare returns string like "1960000000000" or formatted
                        return float(str(val).replace(",", ""))
    except Exception:
        pass

    # Fallback: price * shares from tushare daily_basic
    ts_api = _get_tushare()
    if ts_api:
        try:
            df = ts_api.daily_basic(
                ts_code=_ts_code(ticker), start_date=end, end_date=end,
                fields="ts_code,total_mv",
            )
            if df is not None and not df.empty:
                mv = df.iloc[-1]["total_mv"]
                if mv and pd.notna(mv):
                    return float(mv * 10000)  # 万元 → 元
        except Exception:
            pass
    return None


# ---------------------------------------------------------------------------
# Line Items (Financial Statement Details)
# ---------------------------------------------------------------------------

# Mapping from project field names to tushare fina_indicator / statement fields
LINE_ITEM_MAP = {
    "capital_expenditure": ("cf", "cash_pay_for_asset"),
    "depreciation_and_amortization": ("cf", None),  # not directly in tushare
    "net_income": ("income", "net_profit"),
    "outstanding_shares": ("bs", None),  # derived from market_cap / price
    "total_assets": ("bs", "total_assets"),
    "total_liabilities": ("bs", "total_liab"),
    "shareholders_equity": ("bs", "total_equity"),
    "dividends_and_other_cash_distributions": ("cf", None),  # limited in tushare
    "issuance_or_purchase_of_equity_shares": ("cf", None),
    "gross_profit": ("income", None),  # revenue - cost, compute
    "revenue": ("income", "revenue"),
    "free_cash_flow": ("cf", None),  # operating CF - capex, compute
}


def search_line_items(
    ticker: str,
    line_items: list[str],
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
    api_key: str = None,
) -> list[LineItem]:
    ts_api = _get_tushare()
    if not ts_api:
        logger.warning("search_line_items: tushare unavailable, returning empty")
        return []

    ts_code = _ts_code(ticker)
    end_dt = _report_end_date(end_date)
    start_dt = (datetime.strptime(end_dt, "%Y%m%d") - timedelta(days=limit * 120)).strftime("%Y%m%d")

    try:
        df_income = ts_api.income(
            ts_code=ts_code, start_date=start_dt, end_date=end_dt,
        )
        df_bs = ts_api.balancesheet(
            ts_code=ts_code, start_date=start_dt, end_date=end_dt,
        )
        df_cf = ts_api.cashflow(
            ts_code=ts_code, start_date=start_dt, end_date=end_dt,
        )
        df_ind = ts_api.fina_indicator(
            ts_code=ts_code, start_date=start_dt, end_date=end_dt,
        )

        # daily_basic for market cap / price
        df_daily = ts_api.daily_basic(
            ts_code=ts_code, start_date=end_dt, end_date=end_dt,
            fields="ts_code,close,total_mv",
        )

        def _dedup(df):
            if df is None or df.empty:
                return {}
            seen, result = set(), []
            for r in df.to_dict("records"):
                d = r.get("end_date")
                if d and d not in seen:
                    seen.add(d)
                    result.append(r)
            return {r["end_date"]: r for r in result}

        inc_by_date = _dedup(df_income)
        bs_by_date = _dedup(df_bs)
        cf_by_date = _dedup(df_cf)
        ind_by_date = _dedup(df_ind)

        close_price = None
        total_mv = None
        if df_daily is not None and not df_daily.empty:
            close_price = float(df_daily.iloc[-1]["close"])
            total_mv = float(df_daily.iloc[-1]["total_mv"] * 10000)

        def _v(row, key):
            val = row.get(key)
            if val is None or (isinstance(val, float) and pd.notna(val) is False):
                return None
            return float(val)

        # Use income dates as base
        dates = sorted(inc_by_date.keys(), reverse=True)[:limit]

        result = []
        for d in dates:
            inc = inc_by_date.get(d, {})
            bs = bs_by_date.get(d, {})
            cf = cf_by_date.get(d, {})
            ind = ind_by_date.get(d, {})

            item = LineItem(
                ticker=ticker,
                report_period=d,
                period="quarterly",
                currency="CNY",
            )

            # Revenue
            if "revenue" in line_items:
                item.revenue = _v(inc, "revenue")

            # Net income
            if "net_income" in line_items:
                item.net_income = _v(inc, "net_profit")

            # Gross profit = revenue - operating cost
            if "gross_profit" in line_items:
                rev = _v(inc, "revenue")
                cost = _v(inc, "operate_cost")
                if rev and cost:
                    item.gross_profit = rev - cost

            # Total assets
            if "total_assets" in line_items:
                item.total_assets = _v(bs, "total_assets")

            # Total liabilities
            if "total_liabilities" in line_items:
                item.total_liabilities = _v(bs, "total_liab")

            # Shareholders equity
            if "shareholders_equity" in line_items:
                item.shareholders_equity = _v(bs, "total_equity")

            # Outstanding shares (derived)
            if "outstanding_shares" in line_items:
                if close_price and close_price > 0 and total_mv:
                    item.outstanding_shares = int(total_mv / close_price)

            # Capital expenditure
            if "capital_expenditure" in line_items:
                item.capital_expenditure = _v(cf, "cash_pay_for_asset")

            # Depreciation and amortization (not directly available, estimate from CF)
            if "depreciation_and_amortization" in line_items:
                item.depreciation_and_amortization = None  # not easily available

            # Dividends
            if "dividends_and_other_cash_distributions" in line_items:
                item.dividends_and_other_cash_distributions = _v(cf, "cash_pay_div_int")

            # Issuance or purchase of equity shares
            if "issuance_or_purchase_of_equity_shares" in line_items:
                item.issuance_or_purchase_of_equity_shares = _v(cf, "absorb_inv_cash")

            # Free cash flow = operating CF - capex
            if "free_cash_flow" in line_items:
                ocf = _v(cf, "net_cash_flow_operate")
                capex = _v(cf, "cash_pay_for_asset")
                if ocf is not None:
                    item.free_cash_flow = (ocf - capex) if capex else ocf

            result.append(item)

        return result
    except Exception as e:
        logger.error(f"search_line_items failed for {ticker}: {e}")
        return []


# ---------------------------------------------------------------------------
# Company News
# ---------------------------------------------------------------------------


def get_company_news(ticker: str, end_date: str, limit: int = 100, api_key: str = None) -> list[CompanyNews]:
    code = _stock_code(ticker)
    try:
        import akshare as ak
        df = ak.stock_news_em(symbol=code)
        if df is None or df.empty:
            return []
        result = []
        for _, row in df.head(limit).iterrows():
            result.append(CompanyNews(
                ticker=ticker,
                title=str(row.get("新闻标题", "")),
                author=None,
                source="东方财富",
                date=str(row.get("发布时间", "")),
                url=str(row.get("新闻链接", "")),
                sentiment=None,
            ))
        return result
    except Exception as e:
        logger.error(f"get_company_news failed for {ticker}: {e}")
        return []


# ---------------------------------------------------------------------------
# Insider Trades (N/A for A-shares)
# ---------------------------------------------------------------------------


def get_insider_trades(ticker: str, api_key: str = None) -> list[InsiderTrade]:
    return []


# ---------------------------------------------------------------------------
# Company Facts
# ---------------------------------------------------------------------------


def get_company_facts(ticker: str, api_key: str = None) -> CompanyFactsResponse:
    code = _stock_code(ticker)
    ts_code = _ts_code(ticker)
    ts_api = _get_tushare()

    name = code
    industry = None
    sector = None
    exchange = ticker.split(".")[1] if "." in ticker else None
    market_cap = None

    if ts_api:
        try:
            df = ts_api.stock_basic(ts_code=ts_code, fields="ts_code,name,industry,list_date")
            if df is not None and not df.empty:
                row = df.iloc[0]
                name = row.get("name", code)
                industry = row.get("industry")
                # listing_date = row.get("list_date")
        except Exception:
            pass

    # Market cap
    try:
        mc = get_market_cap(ticker, datetime.now().strftime("%Y-%m-%d"))
        if mc:
            market_cap = mc
    except Exception:
        pass

    facts = CompanyFacts(
        ticker=ticker,
        name=name,
        industry=industry,
        sector=sector,
        exchange=exchange,
        is_active=True,
        market_cap=market_cap,
    )
    return CompanyFactsResponse(company_facts=facts)


# ---------------------------------------------------------------------------
# Price response wrapper
# ---------------------------------------------------------------------------


def get_prices_response(ticker: str, start_date: str, end_date: str, api_key: str = None) -> PriceResponse:
    return PriceResponse(
        ticker=ticker,
        prices=get_prices(ticker, start_date, end_date, api_key),
    )


def get_financial_metrics_response(ticker: str, end_date: str, period: str = "ttm", limit: int = 10, api_key: str = None) -> FinancialMetricsResponse:
    return FinancialMetricsResponse(
        financial_metrics=get_financial_metrics(ticker, end_date, period, limit, api_key),
    )


def get_line_items_response(ticker: str, line_items: list[str], end_date: str, period: str = "ttm", limit: int = 10, api_key: str = None) -> LineItemResponse:
    return LineItemResponse(
        search_results=search_line_items(ticker, line_items, end_date, period, limit, api_key),
    )


def get_insider_trades_response(ticker: str, api_key: str = None) -> InsiderTradeResponse:
    return InsiderTradeResponse(insider_trades=get_insider_trades(ticker, api_key))


def get_company_news_response(ticker: str, end_date: str, limit: int = 100, api_key: str = None) -> CompanyNewsResponse:
    return CompanyNewsResponse(news=get_company_news(ticker, end_date, limit, api_key))

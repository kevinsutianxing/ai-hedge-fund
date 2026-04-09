"""Non-interactive runner for AI Hedge Fund with A-share data."""
import os
import sys

os.environ["DATA_PROVIDER"] = "a_share"
os.environ["OPENAI_API_KEY"] = os.environ.get("ZAI_API_KEY", "")
os.environ["OPENAI_API_BASE"] = "https://open.bigmodel.cn/api/coding/paas/v4"

# Add project to path
sys.path.insert(0, "/home/ubuntu/.openclaw/workspace-kevinCoderHK/ai-hedge-fund-ashare")

from dotenv import load_dotenv
load_dotenv("/home/ubuntu/.openclaw/workspace-kevinCoderHK/ai-hedge-fund-ashare/.env")

from src.main import run_hedge_fund, create_workflow

tickers = ["600519.SH", "000858.SZ"]
start_date = "2025-01-01"
end_date = "2025-03-31"
portfolio = {
    "cash": 100000.0,
    "margin_requirement": 0.0,
    "margin_used": 0.0,
    "positions": {
        t: {"long": 0, "short": 0, "long_cost_basis": 0.0, "short_cost_basis": 0.0, "short_margin_used": 0.0}
        for t in tickers
    },
    "realized_gains": {t: {"long": 0.0, "short": 0.0} for t in tickers},
}

print(f"\n{'='*60}")
print(f"  AI Hedge Fund — A-Share Mode")
print(f"  Tickers: {', '.join(tickers)}")
print(f"  Period: {start_date} → {end_date}")
print(f"  Model: GLM-5 via ZAI")
print(f"{'='*60}\n")

result = run_hedge_fund(
    tickers=tickers,
    start_date=start_date,
    end_date=end_date,
    portfolio=portfolio,
    show_reasoning=True,
    model_name="glm-5-turbo",
    model_provider="OpenAI",
)

if result and result.get("decisions"):
    from src.utils.display import print_trading_output
    print_trading_output(result)
else:
    print("No decisions returned.")

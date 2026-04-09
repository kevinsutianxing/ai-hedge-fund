# AI Hedge Fund - A股版

基于 [virattt/ai-hedge-fund](https://github.com/virattt/ai-hedge-fund) 改造，支持 A 股数据源。

## 改动

- 新增 `src/tools/api_a_share.py` — A 股数据适配层（tushare + akshare）
- `src/tools/__init__.py` — 数据源路由器
- Portfolio Manager 在 A 股模式下自动禁用做空
- 所有 agent 导入路径统一改为 `from src.tools import`

## 使用

```bash
# 设置环境变量
export DATA_PROVIDER=a_share
export TUSHARE_TOKEN=your_tushare_token   # 需要积分 ≥ 120
export OPENAI_API_KEY=your_key            # 或用 deepseek/ollama

# 运行
python src/main.py --ticker 600519.SH,000858.SZ --start-date 2025-01-01 --end-date 2025-12-31

# 回测
python src/backtester.py --ticker 600519.SH,000858.SZ --start-date 2024-01-01 --end-date 2025-12-31
```

## 数据源

| 数据类型 | 主数据源 | 备用 |
|---------|---------|------|
| 行情 (OHLCV) | tushare daily | akshare stock_zh_a_hist |
| 财务指标 | tushare fina_indicator | akshare stock_financial_abstract_ths |
| 利润表 | tushare income | — |
| 资产负债表 | tushare balancesheet | — |
| 现金流量表 | tushare cashflow | — |
| 市值 | tushare daily_basic | akshare stock_individual_info_em |
| 新闻 | akshare stock_news_em | — |

## 注意

- A 股 ticker 格式：`600519.SH`（上交所）、`000858.SZ`（深交所）、`300750.SZ`（创业板）
- tushare 财务接口需要积分（fina_indicator/income/balancesheet/cashflow）
- 行情数据和新闻通过 akshare 免费获取
- A 股模式下自动禁用 short/cover 操作

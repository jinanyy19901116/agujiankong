# Upbit Arkham Monitor V2

监控 Arkham WebSocket 中与 Upbit 相关的小币种大额链上转账，并发送 Telegram 告警。

## V2 增强点

- 识别 **交易所 -> 交易所** 资金转移，并自动降权
- 过滤 **Upbit 内部调拨 / 关联地址调拨**
- 对 **连续同向资金流** 加分
- 对 **疑似拆单 / 归集钱包** 降权
- 最终输出 **偏多 / 偏空 / 观望** 提示

## 本地运行

```bash
pip install -r requirements.txt
cp .env.example .env
python main.py
```

Windows PowerShell:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
python -X utf8 main.py
```

## Railway 部署

1. 把本项目推到 GitHub
2. Railway -> New Project -> Deploy from GitHub repo
3. 选择仓库
4. 在 Variables 中填写 `.env.example` 里的变量
5. Deploy

## 告警含义

- `流入Upbit`：偏空，可能是充值准备卖出
- `流出Upbit`：偏多，可能是提出交易所囤币
- `交易所 -> 交易所`：自动降权，避免把跨所搬砖误判为强信号
- `Upbit内部调拨`：忽略

## 推荐参数

```env
TOKEN_WHITELIST=ankr,animecoin,steem,gas,bard,ong,wld,sui,shib
CHAIN_WHITELIST=ethereum,solana,base,arbitrum_one
USD_THRESHOLD=50000
MIN_ALERT_SCORE=60
SIGNAL_WINDOW_SECONDS=300
```

## 注意

本项目监控的是链上转账，不是 Upbit 站内撮合成交。

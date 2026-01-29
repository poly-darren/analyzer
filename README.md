# Polymarket 101: Markets, Prices, and the Order Book

This repo focuses on analyzing **Polymarket weather markets**. If you're new to Polymarket, this README explains the core concepts behind **how prices form** and why you can see a "price" even when **volume is $0**.

## What Polymarket Is (In One Sentence)

Polymarket is a prediction market where you trade contracts that settle to **$1 if a specific outcome happens** and **$0 if it doesn't**.

Because the payoff is binary ($0 or $1), the contract price (e.g., `70c`) is often interpreted as an *implied probability* (roughly `70%`), but the market price can deviate from "true probability" due to spreads, fees, and supply/demand.

## Contracts and Outcomes

### Binary markets

In a binary market (e.g., "Will it rain tomorrow?"):

- **YES** pays `$1` if the event happens, else `$0`.
- **NO** pays `$1` if the event does *not* happen, else `$0`.

### Multi-outcome markets (like temperature ranges)

In a multi-outcome market (e.g., "Highest temperature in Seoul on Jan 31"):

- Each row (e.g., `-1C`, `0C`, `1C`, ...) is its own outcome that pays `$1` only if that outcome is the winner.
- The outcomes are **mutually exclusive** (only one can win), so in a frictionless world the sum of all outcome prices would be close to `$1.00` (100c).
- In reality, sums can be above/below 100c because each outcome has its own order book with spreads and uneven liquidity.

## How Prices Actually Form: The Order Book

Polymarket uses a **central limit order book**:

- A **bid** is an order to buy at or below a price (liquidity you provide).
- An **ask** is an order to sell at or above a price (liquidity someone else provides).
- A trade only happens when a buy and sell order **cross** (agree on a price).

### "Buy Yes" and "Buy No" buttons

The UI typically shows the *best available* price to immediately buy:

- `Buy Yes 69c` means: the lowest ask to buy YES right now is 69c.
- `Buy No 80c` means: the lowest ask to buy NO right now is 80c.

If you try to "buy immediately" but there is no matching liquidity on the other side, your order can't fill instantly.

## Why `$0 Vol.` Can Still Show a Non-Zero "Price"

`$0 Vol.` means **no trades have executed** (in that time window). It does *not* mean "no orders exist".

You can have:

- Resting limit orders in the book (quotes exist),
- But nobody has hit them yet (no fills),
- So executed volume stays at `$0`.

### The displayed percent is often a "mid"

Many Polymarket screens display a percent that behaves like a **mark price**, commonly the midpoint between:

- best YES bid (what you can sell YES for right now), and
- best YES ask (what you can buy YES for right now).

In markets where the UI shows "Buy No", you can infer the best YES bid from the best NO ask:

- Buying NO at `80c` implies a YES bid of about `1 - 0.80 = 0.20` (20c), ignoring fees/frictions.

Example (matches the common "-1C row shows ~44% with $0 volume" situation):

```text
Best YES ask = 0.69
Best NO  ask = 0.80  -> implied YES bid ~= 0.20
Mid (mark)   ~= (0.69 + 0.20) / 2 = 0.445  -> 44.5%
Executed vol = $0 (if nobody traded yet)
```

This is why a row can display a "high-ish" percent even when volume is zero: the percent is reflecting **quotes**, not **completed trades**.

## "If I'm First, Who Sells to Me?"

If you're the first person to place an order:

- Your limit order becomes the first visible quote (a bid or ask).
- You do **not** get filled until another trader (often a market maker) submits an order that crosses yours.

If nobody crosses your order, it just sits unfilled.

## Practical Notes (Trading Intuition)

- **Liquidity matters:** wide spreads mean the displayed price can be misleading; your actual fill could be far from the midpoint.
- **Volume is not liquidity:** volume is past trades; liquidity is current resting orders.
- **"Probability" is an interpretation:** price is what traders are willing to pay, not a guarantee of accuracy.

## Glossary

- **Bid:** highest price someone is currently offering to buy.
- **Ask:** lowest price someone is currently offering to sell.
- **Spread:** `ask - bid`; wider spread usually means less liquidity.
- **Mid / mark:** `(bid + ask) / 2`; often used for display/charting.
- **Volume:** executed trade volume over a time window.

---

Not financial advice. This is a mechanics explanation so you can reason about what the UI is showing.

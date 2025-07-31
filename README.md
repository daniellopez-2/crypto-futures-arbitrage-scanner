# ⚡ crypto futures arbitrage scanner

real-time crypto futures arbitrage scanner, built in go and plain javascript. connect to multiple exchanges right at the websocket layer. shows live price gaps and where the spread hides.

## what's this about?

market microstructure is the study of how prices form in split-up, messy markets. a single asset never has a single price. every exchange has its own order book, its own little quirks. so, the price drifts—sometimes by a lot, usually for just milliseconds.

in crypto, this isn't hidden behind expensive pro feeds. you can actually see the gaps yourself if you have the right tools. that's what this project does: surfaces live, structural arbitrage opportunities, so you can watch price discovery as it happens.

## what can it do?

- connect to 5 spot/futures exchanges (binance, bybit, hyperliquid, okx, gate.io) over websockets
- live arbitrage matrix: highlights when the price difference is big enough
- watch multiple pairs: btcusdt, ethusdt, xrpusdt, solusdt
- auto adjusts decimals by asset/price
- live uplot charts
- spot and alert on inefficient price gaps, in real time

## how does it work?

- **backend (go):**
    - every exchange runs in its own goroutine, fetches orderbook prices live
    - all the data gets passed through go channels, no locks slowing things down
    - no missed trades or stalls if an exchange lags
    - once prices land, calculates spreads & arbitrage. broadcasts over one websocket to all frontends

- **frontend:**
    - vanilla js
    - uses uplot for charting millions of points

## how to run

1. open a terminal, start the backend:

    ```
    go run main.go
    ```

2. open your browser. head over to `http://localhost:8080`

## pairs & exchanges

- btcusdt
- ethusdt
- xrpusdt
- solusdt

covers:
- binance futures
- bybit futures
- hyperliquid (dex) futures
- okx futures
- gate.io futures

## config

set your own minimum spread for alerts in the ui (default is 0.05%, after estimated fees).

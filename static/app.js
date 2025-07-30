class FuturesArbitrageScanner {
    constructor() {
        this.currentSymbol = 'BTCUSDT';
        this.exchanges = new Map();
        this.priceHistory = new Map();
        this.arbitrageAlerts = [];
        this.maxHistoryPoints = 1000;
        this.connectedExchanges = new Set();
        
        this.chart = null;
        this.cvdChart = null;
        this.chartData = [[], [], [], [], [], []]; // timestamps, binance, bybit, hyperliquid, kraken, okx, bitget
        this.cvdChartData = [[], [], [], [], [], []]; // timestamps, binance_cvd, bybit_cvd, hyperliquid_cvd, kraken_cvd, okx_cvd, bitget_cvd
        this.cvdHistory = new Map();
        this.ws = null;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 10;
        
        this.fps = 0;
        this.frameCount = 0;
        this.lastTime = performance.now();
        this.chartUpdatePending = false;
        this.lastChartUpdate = 0;
        this.chartUpdateThrottle = 100; // ms
        
        this.init();
    }

    init() {
        this.setupEventListeners();
        this.setupChart();
        this.setupCVDChart();
        this.connectWebSocket();
        this.startFPSCounter();
        this.startRenderLoop();
    }

    setupEventListeners() {
        const symbolInput = document.getElementById('symbolInput');
        symbolInput.addEventListener('change', (e) => {
            this.changeSymbol(e.target.value.toUpperCase());
        });


        window.addEventListener('resize', () => {
            if (this.chart) {
                this.chart.setSize({
                    width: this.getChartWidth(),
                    height: this.getChartHeight()
                });
            }
            if (this.cvdChart) {
                this.cvdChart.setSize({
                    width: this.getCVDChartWidth(),
                    height: this.getCVDChartHeight()
                });
            }
        });
    }

    setupChart() {
        const chartContainer = document.getElementById('chart');
        
        // Initialize with minimal data to show chart immediately
        const now = Date.now() / 1000;
        this.chartData = [
            [now - 60, now],
            [null, null],
            [null, null],
            [null, null],
            [null, null],
            [null, null],
            [null, null]
        ];
        
        const opts = {
            width: this.getChartWidth(),
            height: this.getChartHeight(),
            plugins: [
                {
                    hooks: {
                        drawClear: [
                            u => {
                                const ctx = u.ctx;
                                ctx.fillStyle = '#0f0f0f';
                                ctx.fillRect(0, 0, u.over.width, u.over.height);
                            }
                        ]
                    }
                }
            ],
            scales: {
                x: {
                    time: true,
                    space: 60,
                },
                y: {
                    auto: true,
                    space: 60,
                }
            },
            axes: [
                {
                    stroke: '#444',
                    grid: {
                        show: true,
                        stroke: '#222',
                        width: 1,
                    },
                    ticks: {
                        show: true,
                        stroke: '#444',
                        width: 1,
                        size: 5,
                    },
                    font: '11px JetBrains Mono, Monaco, Consolas, monospace',
                    labelFont: '11px JetBrains Mono, Monaco, Consolas, monospace',
                    size: 60,
                    gap: 8,
                    stroke: '#888',
                },
                {
                    stroke: '#444',
                    grid: {
                        show: true,
                        stroke: '#222',
                        width: 1,
                    },
                    ticks: {
                        show: true,
                        stroke: '#444',
                        width: 1,
                        size: 5,
                    },
                    font: '11px JetBrains Mono, Monaco, Consolas, monospace',
                    labelFont: '11px JetBrains Mono, Monaco, Consolas, monospace',
                    size: 80,
                    gap: 5,
                    stroke: '#888',
                    values: (_, vals) => vals.map(v => '$' + v.toFixed(2)),
                }
            ],
            series: [
                {},
                {
                    label: "Binance Futures",
                    stroke: "#f0b90b",
                    width: 2,
                    spanGaps: false,
                    value: (_, v) => v == null ? '' : '$' + v.toFixed(2),
                },
                {
                    label: "Bybit Futures",
                    stroke: "#f7931a",
                    width: 2,
                    spanGaps: false,
                    value: (_, v) => v == null ? '' : '$' + v.toFixed(2),
                },
                {
                    label: "Hyperliquid Futures",
                    stroke: "#97FCE4",
                    width: 2,
                    spanGaps: false,
                    value: (_, v) => v == null ? '' : '$' + v.toFixed(2),
                },
                {
                    label: "Kraken Futures",
                    stroke: "#5a5aff",
                    width: 2,
                    spanGaps: false,
                    value: (_, v) => v == null ? '' : '$' + v.toFixed(2),
                },
                {
                    label: "OKX Futures",
                    stroke: "#ff6b6b",
                    width: 2,
                    spanGaps: false,
                    value: (_, v) => v == null ? '' : '$' + v.toFixed(2),
                },
                {
                    label: "Bitget Futures",
                    stroke: "#9b59b6",
                    width: 2,
                    spanGaps: false,
                    value: (_, v) => v == null ? '' : '$' + v.toFixed(2),
                }
            ],
            legend: {
                show: false,
            },
            cursor: {
                show: true,
                x: true,
                y: true,
                lock: false,
                focus: {
                    prox: 16,
                },
                drag: {
                    setScale: false,
                    x: true,
                    y: false,
                },
            },
            select: {
                show: false,
            },
            hooks: {
                setCursor: [
                    (u) => {
                        this.updateCustomLegend(u);
                    }
                ]
            }
        };

        this.chart = new uPlot(opts, this.chartData, chartContainer);
        this.updateChartTitle();
    }

    setupCVDChart() {
        const cvdChartContainer = document.getElementById('cvdChart');
        
        // Initialize with minimal data to show chart immediately
        const now = Date.now() / 1000;
        this.cvdChartData = [
            [now - 60, now],
            [null, null],
            [null, null],
            [null, null],
            [null, null],
            [null, null],
            [null, null]
        ];
        
        const opts = {
            width: this.getCVDChartWidth(),
            height: this.getCVDChartHeight(),
            plugins: [
                {
                    hooks: {
                        drawClear: [
                            u => {
                                const ctx = u.ctx;
                                ctx.fillStyle = '#0f0f0f';
                                ctx.fillRect(0, 0, u.over.width, u.over.height);
                            }
                        ]
                    }
                }
            ],
            scales: {
                x: {
                    time: true,
                    space: 60,
                },
                y: {
                    auto: true,
                    space: 60,
                }
            },
            axes: [
                {
                    stroke: '#444',
                    grid: {
                        show: true,
                        stroke: '#222',
                        width: 1,
                    },
                    ticks: {
                        show: true,
                        stroke: '#444',
                        width: 1,
                        size: 5,
                    },
                    font: '11px JetBrains Mono, Monaco, Consolas, monospace',
                    labelFont: '11px JetBrains Mono, Monaco, Consolas, monospace',
                    size: 60,
                    gap: 8,
                    stroke: '#888',
                },
                {
                    stroke: '#444',
                    grid: {
                        show: true,
                        stroke: '#222',
                        width: 1,
                    },
                    ticks: {
                        show: true,
                        stroke: '#444',
                        width: 1,
                        size: 5,
                    },
                    font: '11px JetBrains Mono, Monaco, Consolas, monospace',
                    labelFont: '11px JetBrains Mono, Monaco, Consolas, monospace',
                    size: 80,
                    gap: 5,
                    stroke: '#888',
                    values: (_, vals) => vals.map(v => v.toFixed(0)),
                }
            ],
            series: [
                {},
                {
                    label: "Binance CVD",
                    stroke: "#f0b90b",
                    width: 2,
                    spanGaps: false,
                    value: (_, v) => v == null ? '' : v.toFixed(0),
                },
                {
                    label: "Bybit CVD",
                    stroke: "#f7931a",
                    width: 2,
                    spanGaps: false,
                    value: (_, v) => v == null ? '' : v.toFixed(0),
                },
                {
                    label: "Hyperliquid CVD",
                    stroke: "#97FCE4",
                    width: 2,
                    spanGaps: false,
                    value: (_, v) => v == null ? '' : v.toFixed(0),
                },
                {
                    label: "Kraken CVD",
                    stroke: "#5a5aff",
                    width: 2,
                    spanGaps: false,
                    value: (_, v) => v == null ? '' : v.toFixed(0),
                },
                {
                    label: "OKX CVD",
                    stroke: "#ff6b6b",
                    width: 2,
                    spanGaps: false,
                    value: (_, v) => v == null ? '' : v.toFixed(0),
                },
                {
                    label: "Bitget CVD",
                    stroke: "#9b59b6",
                    width: 2,
                    spanGaps: false,
                    value: (_, v) => v == null ? '' : v.toFixed(0),
                }
            ],
            legend: {
                show: false,
            },
            cursor: {
                show: true,
                x: true,
                y: true,
                lock: false,
                focus: {
                    prox: 16,
                },
                drag: {
                    setScale: false,
                    x: true,
                    y: false,
                },
            },
            select: {
                show: false,
            },
            hooks: {
                setCursor: [
                    (u) => {
                        this.updateCVDCustomLegend(u);
                    }
                ]
            }
        };

        this.cvdChart = new uPlot(opts, this.cvdChartData, cvdChartContainer);
        this.updateCVDChartTitle();
    }

    getChartWidth() {
        const chartContainer = document.getElementById('chart');
        return chartContainer ? chartContainer.clientWidth - 20 : 800;
    }

    getChartHeight() {
        const chartContainer = document.getElementById('chart');
        if (!chartContainer) return 400;
        
        const containerHeight = chartContainer.clientHeight;
        return Math.max(containerHeight - 25, 300);
    }

    getCVDChartWidth() {
        const cvdChartContainer = document.getElementById('cvdChart');
        return cvdChartContainer ? cvdChartContainer.clientWidth - 20 : 800;
    }

    getCVDChartHeight() {
        const cvdChartContainer = document.getElementById('cvdChart');
        if (!cvdChartContainer) return 300;
        
        const containerHeight = cvdChartContainer.clientHeight;
        return Math.max(containerHeight - 25, 200);
    }

    connectWebSocket() {
        const wsStatus = document.getElementById('wsStatus');
        const wsStatusText = document.getElementById('wsStatusText');
        
        wsStatus.className = 'status-dot disconnected';
        wsStatusText.textContent = 'Connecting...';

        try {
            this.ws = new WebSocket('ws://localhost:8080/ws');
            
            this.ws.onopen = () => {
                console.log('WebSocket connected');
                wsStatus.className = 'status-dot connected';
                wsStatusText.textContent = 'Connected';
                this.reconnectAttempts = 0;
            };

            this.ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    this.handleWebSocketMessage(data);
                } catch (error) {
                    console.error('Error parsing WebSocket message:', error);
                }
            };

            this.ws.onclose = () => {
                console.log('WebSocket disconnected');
                wsStatus.className = 'status-dot disconnected';
                wsStatusText.textContent = 'Disconnected';
                this.scheduleReconnect();
            };

            this.ws.onerror = (error) => {
                console.error('WebSocket error:', error);
                wsStatus.className = 'status-dot disconnected';
                wsStatusText.textContent = 'Error';
            };

        } catch (error) {
            console.error('Failed to create WebSocket connection:', error);
            this.scheduleReconnect();
        }
    }

    scheduleReconnect() {
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
            console.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);
            setTimeout(() => this.connectWebSocket(), delay);
        }
    }

    handleWebSocketMessage(data) {
        if (data.type === 'prices') {
            this.updatePrices(data.prices);
        } else if (data.type === 'price_update') {
            this.handlePriceUpdate(data);
        } else if (data.type === 'cvd_update') {
            this.handleCVDUpdate(data);
        } else if (data.type === 'arbitrage') {
            this.handleArbitrageOpportunity(data.opportunity);
        }
    }

    updatePrices(prices) {
        for (const [symbol, exchangePrices] of Object.entries(prices)) {
            if (symbol === this.currentSymbol) {
                for (const [exchange, price] of Object.entries(exchangePrices)) {
                    this.updateExchangePrice(exchange, price);
                    this.addPriceToHistory(exchange, price);
                }
                this.updateExchangeList();
                this.updateChart();
                break;
            }
        }
    }

    handlePriceUpdate(data) {
        if (data.symbol === this.currentSymbol) {
            this.updateExchangePrice(data.exchange, data.price);
            this.addPriceToHistory(data.exchange, data.price, data.timestamp);
            this.updateExchangeList();
            this.updateChart();
        }
    }

    handleCVDUpdate(data) {
        if (data.symbol === this.currentSymbol) {
            this.addCVDToHistory(data.exchange, data.cvd, data.timestamp);
            this.updateCVDChart();
        }
    }

    addCVDToHistory(exchange, cvd, timestamp = null) {
        const ts = timestamp ? timestamp / 1000 : Date.now() / 1000;
        
        if (!this.cvdHistory.has(exchange)) {
            this.cvdHistory.set(exchange, []);
        }

        const history = this.cvdHistory.get(exchange);
        history.push([ts, cvd]);

        if (history.length > this.maxHistoryPoints) {
            history.shift();
        }
    }

    updateExchangePrice(exchange, price) {
        const previousPrice = this.exchanges.get(exchange)?.price || price;
        const change = price - previousPrice;
        const changePercent = previousPrice !== 0 ? (change / previousPrice) * 100 : 0;

        this.exchanges.set(exchange, {
            price: price,
            previousPrice: previousPrice,
            change: change,
            changePercent: changePercent,
            lastUpdate: Date.now()
        });

        this.connectedExchanges.add(exchange);
        this.updateExchangeTooltip();
    }

    addPriceToHistory(exchange, price, timestamp = null) {
        const ts = timestamp ? timestamp / 1000 : Date.now() / 1000;
        
        if (!this.priceHistory.has(exchange)) {
            this.priceHistory.set(exchange, []);
        }

        const history = this.priceHistory.get(exchange);
        history.push([ts, price]);

        if (history.length > this.maxHistoryPoints) {
            history.shift();
        }
    }

    updateExchangeList() {
        const exchangeList = document.getElementById('exchangeList');
        
        if (this.exchanges.size === 0) {
            exchangeList.innerHTML = '<div class="loading">No data available</div>';
            return;
        }

        const exchangeColors = {
            'binance_futures': '#f0b90b',
            'bybit_futures': '#f7931a',
            'hyperliquid_futures': '#97FCE4',
            'kraken_futures': '#5a5aff',
            'okx_futures': '#ff6b6b',
            'bitget_futures': '#9b59b6'
        };

        let html = '';
        for (const [exchange, data] of this.exchanges.entries()) {
            const changeClass = data.change >= 0 ? 'up' : 'down';
            const changeSymbol = data.change >= 0 ? '↑' : '↓';
            const color = exchangeColors[exchange] || '#888';
            
            html += `
                <div class="exchange-item">
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <div class="exchange-color-dot" style="background: ${color};"></div>
                        <div class="exchange-name">${exchange.replace('_', ' ')}</div>
                    </div>
                    <div>
                        <span class="exchange-price">$${data.price.toFixed(2)}</span>
                        <span class="price-change ${changeClass}">
                            ${changeSymbol} ${Math.abs(data.changePercent).toFixed(3)}%
                        </span>
                    </div>
                </div>
            `;
        }
        
        exchangeList.innerHTML = html;
    }

    updateChart() {
        const now = performance.now();
        if (now - this.lastChartUpdate < this.chartUpdateThrottle) {
            if (!this.chartUpdatePending) {
                this.chartUpdatePending = true;
                setTimeout(() => {
                    this.chartUpdatePending = false;
                    this.performChartUpdate();
                }, this.chartUpdateThrottle - (now - this.lastChartUpdate));
            }
            return;
        }
        
        this.performChartUpdate();
    }

    performChartUpdate() {
        if (!this.chart || this.priceHistory.size === 0) return;

        const binanceHistory = this.priceHistory.get('binance_futures') || [];
        const bybitHistory = this.priceHistory.get('bybit_futures') || [];
        const hyperliquidHistory = this.priceHistory.get('hyperliquid_futures') || [];
        const krakenHistory = this.priceHistory.get('kraken_futures') || [];
        const okxHistory = this.priceHistory.get('okx_futures') || [];
        const bitgetHistory = this.priceHistory.get('bitget_futures') || [];
        
        if (binanceHistory.length === 0 && bybitHistory.length === 0 && hyperliquidHistory.length === 0 && krakenHistory.length === 0 && okxHistory.length === 0 && bitgetHistory.length === 0) return;

        // Create combined timestamp array
        const allTimestamps = new Set();
        binanceHistory.forEach(point => allTimestamps.add(point[0]));
        bybitHistory.forEach(point => allTimestamps.add(point[0]));
        hyperliquidHistory.forEach(point => allTimestamps.add(point[0]));
        krakenHistory.forEach(point => allTimestamps.add(point[0]));
        okxHistory.forEach(point => allTimestamps.add(point[0]));
        bitgetHistory.forEach(point => allTimestamps.add(point[0]));
        
        const timestamps = Array.from(allTimestamps).sort((a, b) => a - b);
        
        // Create price arrays with forward-filled values for missing data points
        const binancePrices = [];
        const bybitPrices = [];
        const hyperliquidPrices = [];
        const krakenPrices = [];
        const okxPrices = [];
        const bitgetPrices = [];
        
        const binanceMap = new Map(binanceHistory);
        const bybitMap = new Map(bybitHistory);
        const hyperliquidMap = new Map(hyperliquidHistory);
        const krakenMap = new Map(krakenHistory);
        const okxMap = new Map(okxHistory);
        const bitgetMap = new Map(bitgetHistory);
        
        let lastBinancePrice = null;
        let lastBybitPrice = null;
        let lastHyperliquidPrice = null;
        let lastKrakenPrice = null;
        let lastOkxPrice = null;
        let lastBitgetPrice = null;
        
        timestamps.forEach(timestamp => {
            const binancePrice = binanceMap.get(timestamp);
            const bybitPrice = bybitMap.get(timestamp);
            const hyperliquidPrice = hyperliquidMap.get(timestamp);
            const krakenPrice = krakenMap.get(timestamp);
            const okxPrice = okxMap.get(timestamp);
            const bitgetPrice = bitgetMap.get(timestamp);
            
            if (binancePrice !== undefined) {
                lastBinancePrice = binancePrice;
            }
            if (bybitPrice !== undefined) {
                lastBybitPrice = bybitPrice;
            }
            if (hyperliquidPrice !== undefined) {
                lastHyperliquidPrice = hyperliquidPrice;
            }
            if (krakenPrice !== undefined) {
                lastKrakenPrice = krakenPrice;
            }
            if (okxPrice !== undefined) {
                lastOkxPrice = okxPrice;
            }
            if (bitgetPrice !== undefined) {
                lastBitgetPrice = bitgetPrice;
            }
            
            binancePrices.push(lastBinancePrice);
            bybitPrices.push(lastBybitPrice);
            hyperliquidPrices.push(lastHyperliquidPrice);
            krakenPrices.push(lastKrakenPrice);
            okxPrices.push(lastOkxPrice);
            bitgetPrices.push(lastBitgetPrice);
        });

        this.chartData = [timestamps, binancePrices, bybitPrices, hyperliquidPrices, krakenPrices, okxPrices, bitgetPrices];
        this.chart.setData(this.chartData);
        this.lastChartUpdate = performance.now();
    }

    updateCVDChart() {
        const now = performance.now();
        if (now - this.lastChartUpdate < this.chartUpdateThrottle) {
            if (!this.chartUpdatePending) {
                this.chartUpdatePending = true;
                setTimeout(() => {
                    this.chartUpdatePending = false;
                    this.performCVDChartUpdate();
                }, this.chartUpdateThrottle - (now - this.lastChartUpdate));
            }
            return;
        }
        
        this.performCVDChartUpdate();
    }

    performCVDChartUpdate() {
        if (!this.cvdChart || this.cvdHistory.size === 0) return;

        const binanceCVDHistory = this.cvdHistory.get('binance_futures') || [];
        const bybitCVDHistory = this.cvdHistory.get('bybit_futures') || [];
        const hyperliquidCVDHistory = this.cvdHistory.get('hyperliquid_futures') || [];
        const krakenCVDHistory = this.cvdHistory.get('kraken_futures') || [];
        const okxCVDHistory = this.cvdHistory.get('okx_futures') || [];
        const bitgetCVDHistory = this.cvdHistory.get('bitget_futures') || [];
        
        if (binanceCVDHistory.length === 0 && bybitCVDHistory.length === 0 && hyperliquidCVDHistory.length === 0 && krakenCVDHistory.length === 0 && okxCVDHistory.length === 0 && bitgetCVDHistory.length === 0) return;

        // Create combined timestamp array
        const allTimestamps = new Set();
        binanceCVDHistory.forEach(point => allTimestamps.add(point[0]));
        bybitCVDHistory.forEach(point => allTimestamps.add(point[0]));
        hyperliquidCVDHistory.forEach(point => allTimestamps.add(point[0]));
        krakenCVDHistory.forEach(point => allTimestamps.add(point[0]));
        okxCVDHistory.forEach(point => allTimestamps.add(point[0]));
        bitgetCVDHistory.forEach(point => allTimestamps.add(point[0]));
        
        const timestamps = Array.from(allTimestamps).sort((a, b) => a - b);
        
        // Create CVD arrays with forward-filled values for missing data points
        const binanceCVDs = [];
        const bybitCVDs = [];
        const hyperliquidCVDs = [];
        const krakenCVDs = [];
        const okxCVDs = [];
        const bitgetCVDs = [];
        
        const binanceMap = new Map(binanceCVDHistory);
        const bybitMap = new Map(bybitCVDHistory);
        const hyperliquidMap = new Map(hyperliquidCVDHistory);
        const krakenMap = new Map(krakenCVDHistory);
        const okxMap = new Map(okxCVDHistory);
        const bitgetMap = new Map(bitgetCVDHistory);
        
        let lastBinanceCVD = null;
        let lastBybitCVD = null;
        let lastHyperliquidCVD = null;
        let lastKrakenCVD = null;
        let lastOkxCVD = null;
        let lastBitgetCVD = null;
        
        timestamps.forEach(timestamp => {
            const binanceCVD = binanceMap.get(timestamp);
            const bybitCVD = bybitMap.get(timestamp);
            const hyperliquidCVD = hyperliquidMap.get(timestamp);
            const krakenCVD = krakenMap.get(timestamp);
            const okxCVD = okxMap.get(timestamp);
            const bitgetCVD = bitgetMap.get(timestamp);
            
            if (binanceCVD !== undefined) {
                lastBinanceCVD = binanceCVD;
            }
            if (bybitCVD !== undefined) {
                lastBybitCVD = bybitCVD;
            }
            if (hyperliquidCVD !== undefined) {
                lastHyperliquidCVD = hyperliquidCVD;
            }
            if (krakenCVD !== undefined) {
                lastKrakenCVD = krakenCVD;
            }
            if (okxCVD !== undefined) {
                lastOkxCVD = okxCVD;
            }
            if (bitgetCVD !== undefined) {
                lastBitgetCVD = bitgetCVD;
            }
            
            binanceCVDs.push(lastBinanceCVD);
            bybitCVDs.push(lastBybitCVD);
            hyperliquidCVDs.push(lastHyperliquidCVD);
            krakenCVDs.push(lastKrakenCVD);
            okxCVDs.push(lastOkxCVD);
            bitgetCVDs.push(lastBitgetCVD);
        });

        this.cvdChartData = [timestamps, binanceCVDs, bybitCVDs, hyperliquidCVDs, krakenCVDs, okxCVDs, bitgetCVDs];
        this.cvdChart.setData(this.cvdChartData);
    }

    handleArbitrageOpportunity(opportunity) {
        this.arbitrageAlerts.unshift(opportunity);
        
        if (this.arbitrageAlerts.length > 10) {
            this.arbitrageAlerts = this.arbitrageAlerts.slice(0, 10);
        }

        this.updateArbitrageAlerts();
    }

    updateArbitrageAlerts() {
        const alertsContainer = document.getElementById('arbitrageAlerts');
        
        if (this.arbitrageAlerts.length === 0) {
            alertsContainer.innerHTML = '<div class="loading">No opportunities detected</div>';
            return;
        }

        let html = '';
        this.arbitrageAlerts.forEach(alert => {
            const timestamp = new Date(alert.timestamp).toLocaleTimeString();
            html += `
                <div class="arbitrage-alert">
                    <div class="opportunity-symbol">${alert.symbol}</div>
                    <div class="profit">+${alert.profit_pct.toFixed(3)}% profit</div>
                    <div class="trade-info">
                        <div>Buy: ${alert.buy_exchange} @ $${alert.buy_price.toFixed(2)}</div>
                        <div>Sell: ${alert.sell_exchange} @ $${alert.sell_price.toFixed(2)}</div>
                    </div>
                    <div style="font-size: 10px; color: #666; margin-top: 4px;">${timestamp}</div>
                </div>
            `;
        });
        
        alertsContainer.innerHTML = html;
    }

    changeSymbol(newSymbol) {
        if (newSymbol === this.currentSymbol) return;
        
        this.currentSymbol = newSymbol;
        this.exchanges.clear();
        this.priceHistory.clear();
        this.cvdHistory.clear();
        this.arbitrageAlerts = [];
        
        this.chartData = [[], [], [], [], [], [], []];
        this.cvdChartData = [[], [], [], [], [], [], []];
        if (this.chart) {
            this.chart.setData(this.chartData);
        }
        if (this.cvdChart) {
            this.cvdChart.setData(this.cvdChartData);
        }

        this.updateChartTitle();
        this.updateCVDChartTitle();
        this.updateExchangeList();
        this.updateArbitrageAlerts();
        
        document.getElementById('symbolStatus').textContent = newSymbol;
        
        console.log(`Switched to symbol: ${newSymbol}`);
    }

    updateChartTitle() {
        const chartTitle = document.getElementById('chartTitle');
        chartTitle.textContent = `Price Chart - ${this.currentSymbol} (Live)`;
    }

    updateCVDChartTitle() {
        const cvdChartTitle = document.getElementById('cvdChartTitle');
        cvdChartTitle.textContent = `CVD Chart - ${this.currentSymbol} (Live)`;
    }

    updateExchangeTooltip() {
        const tooltip = document.getElementById('exchangeTooltip');
        if (this.connectedExchanges.size === 0) {
            tooltip.textContent = 'No exchanges connected';
        } else {
            const exchangeNames = Array.from(this.connectedExchanges)
                .map(ex => ex.replace('_', ' ').toUpperCase())
                .join(', ');
            tooltip.textContent = `Connected: ${exchangeNames}`;
        }
    }

    updateCustomLegend(u) {
        const legend = document.getElementById('chartLegend');
        const legendTime = document.getElementById('legendTime');
        const binanceValue = document.getElementById('binanceValue');
        const bybitValue = document.getElementById('bybitValue');
        const hyperliquidValue = document.getElementById('hyperliquidValue');
        const krakenValue = document.getElementById('krakenValue');
        const okxValue = document.getElementById('okxValue');
        const bitgetValue = document.getElementById('bitgetValue');

        if (u.cursor.idx === null) {
            legend.classList.remove('visible');
            return;
        }

        legend.classList.add('visible');

        // Get the data at cursor position
        const idx = u.cursor.idx;
        const timestamp = u.data[0][idx];
        const binancePrice = u.data[1][idx];
        const bybitPrice = u.data[2][idx];
        const hyperliquidPrice = u.data[3][idx];
        const krakenPrice = u.data[4][idx];
        const okxPrice = u.data[5][idx];
        const bitgetPrice = u.data[6][idx];

        // Format timestamp with milliseconds
        if (timestamp) {
            const date = new Date(timestamp * 1000);
            const timeString = date.toLocaleString();
            const ms = Math.floor((timestamp * 1000) % 1000);
            legendTime.textContent = `${timeString}.${ms.toString().padStart(3, '0')}`;
        } else {
            legendTime.textContent = '--';
        }

        // Update values
        binanceValue.textContent = binancePrice ? `$${binancePrice.toFixed(2)}` : '--';
        bybitValue.textContent = bybitPrice ? `$${bybitPrice.toFixed(2)}` : '--';
        hyperliquidValue.textContent = hyperliquidPrice ? `$${hyperliquidPrice.toFixed(2)}` : '--';
        krakenValue.textContent = krakenPrice ? `$${krakenPrice.toFixed(2)}` : '--';
        if (okxValue) okxValue.textContent = okxPrice ? `$${okxPrice.toFixed(2)}` : '--';
        if (bitgetValue) bitgetValue.textContent = bitgetPrice ? `$${bitgetPrice.toFixed(2)}` : '--';
    }

    updateCVDCustomLegend(u) {
        const legend = document.getElementById('cvdChartLegend');
        const legendTime = document.getElementById('cvdLegendTime');
        const binanceCVDValue = document.getElementById('binanceCVDValue');
        const bybitCVDValue = document.getElementById('bybitCVDValue');
        const hyperliquidCVDValue = document.getElementById('hyperliquidCVDValue');
        const krakenCVDValue = document.getElementById('krakenCVDValue');
        const okxCVDValue = document.getElementById('okxCVDValue');
        const bitgetCVDValue = document.getElementById('bitgetCVDValue');

        if (u.cursor.idx === null) {
            legend.classList.remove('visible');
            return;
        }

        legend.classList.add('visible');

        // Get the data at cursor position
        const idx = u.cursor.idx;
        const timestamp = u.data[0][idx];
        const binanceCVD = u.data[1][idx];
        const bybitCVD = u.data[2][idx];
        const hyperliquidCVD = u.data[3][idx];
        const krakenCVD = u.data[4][idx];
        const okxCVD = u.data[5][idx];
        const bitgetCVD = u.data[6][idx];

        // Format timestamp with milliseconds
        if (timestamp) {
            const date = new Date(timestamp * 1000);
            const timeString = date.toLocaleString();
            const ms = Math.floor((timestamp * 1000) % 1000);
            legendTime.textContent = `${timeString}.${ms.toString().padStart(3, '0')}`;
        } else {
            legendTime.textContent = '--';
        }

        // Update values
        binanceCVDValue.textContent = binanceCVD ? binanceCVD.toFixed(0) : '--';
        bybitCVDValue.textContent = bybitCVD ? bybitCVD.toFixed(0) : '--';
        hyperliquidCVDValue.textContent = hyperliquidCVD ? hyperliquidCVD.toFixed(0) : '--';
        krakenCVDValue.textContent = krakenCVD ? krakenCVD.toFixed(0) : '--';
        if (okxCVDValue) okxCVDValue.textContent = okxCVD ? okxCVD.toFixed(0) : '--';
        if (bitgetCVDValue) bitgetCVDValue.textContent = bitgetCVD ? bitgetCVD.toFixed(0) : '--';
    }


    startFPSCounter() {
        const fpsCounter = document.getElementById('fpsCounter');
        
        setInterval(() => {
            fpsCounter.textContent = `${this.fps} FPS`;
        }, 1000);
    }

    startRenderLoop() {
        const animate = (currentTime) => {
            this.frameCount++;
            
            if (currentTime - this.lastTime >= 1000) {
                this.fps = Math.round((this.frameCount * 1000) / (currentTime - this.lastTime));
                this.frameCount = 0;
                this.lastTime = currentTime;
            }
            
            requestAnimationFrame(animate);
        };
        
        requestAnimationFrame(animate);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    window.scanner = new FuturesArbitrageScanner();
    console.log('Futures Arbitrage Scanner initialized');
});
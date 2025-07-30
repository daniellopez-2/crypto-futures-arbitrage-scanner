package exchanges

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

type KrakenFuturesTrade struct {
	Feed      string  `json:"feed"`
	ProductID string  `json:"product_id"`
	Price     float64 `json:"price"`
	Quantity  float64 `json:"qty"`
	Side      string  `json:"side"`
	Timestamp float64 `json:"time"`
	UID       string  `json:"uid"`
	Type      string  `json:"type"`
	Seq       int64   `json:"seq"`
}

type KrakenFuturesTradeSnapshot struct {
	Feed   string `json:"feed"`
	Trades []struct {
		ProductID string  `json:"product_id"`
		Price     float64 `json:"price"`
		Quantity  float64 `json:"qty"`
		Side      string  `json:"side"`
		Timestamp float64 `json:"time"`
		UID       string  `json:"uid"`
		Type      string  `json:"type"`
		Seq       int64   `json:"seq"`
	} `json:"trades"`
}

func processKrakenTrade(productID string, price, quantity float64, side string, timestamp float64, priceChan chan<- PriceData, tradeChan chan<- TradeData) {
	// Convert symbol back to standard format (PI_XBTUSD -> BTCUSDT)
	symbol := convertFromKrakenSymbol(productID)

	// Normalize trade side (Kraken uses "buy" and "sell")
	var normalizedSide string
	if side == "buy" {
		normalizedSide = "buy"
	} else {
		normalizedSide = "sell"
	}

	// Convert timestamp from float64 to int64 milliseconds
	timestampMs := int64(timestamp * 1000)

	priceData := PriceData{
		Symbol:    symbol,
		Exchange:  "kraken_futures",
		Price:     price,
		Timestamp: timestampMs,
	}

	tradeData := TradeData{
		Symbol:    symbol,
		Exchange:  "kraken_futures",
		Price:     price,
		Quantity:  strconv.FormatFloat(quantity, 'f', -1, 64),
		Side:      normalizedSide,
		Timestamp: timestampMs,
	}

	priceChan <- priceData
	tradeChan <- tradeData
}

func ConnectKrakenFutures(symbols []string, priceChan chan<- PriceData, tradeChan chan<- TradeData) {
	wsURL := "wss://futures.kraken.com/ws/v1"

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Kraken connection error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Connected to Kraken futures WebSocket")

		// Subscribe to trades for each symbol
		for _, symbol := range symbols {
			// Convert BTCUSDT to PI_XBTUSD for Kraken
			krakenSymbol := convertToKrakenSymbol(symbol)

			subscribeMsg := map[string]interface{}{
				"event": "subscribe",
				"feed":  "trade",
				"product_ids": []string{krakenSymbol},
			}

			err = conn.WriteJSON(subscribeMsg)
			if err != nil {
				log.Printf("Kraken subscription error for %s: %v", krakenSymbol, err)
				continue
			}
			log.Printf("Kraken subscribed to feed: trade, product_id: %s", krakenSymbol)
		}

		for {
			var rawMessage map[string]interface{}
			err := conn.ReadJSON(&rawMessage)
			if err != nil {
				log.Printf("Kraken read error: %v", err)
				conn.Close()
				break
			}

			// Check if it's a trade_snapshot (array of trades) or individual trade
			if feed, ok := rawMessage["feed"].(string); ok {
				if feed == "trade_snapshot" {
					// Handle trade snapshot (array of trades)
					var snapshot KrakenFuturesTradeSnapshot
					messageBytes, _ := json.Marshal(rawMessage)
					err = json.Unmarshal(messageBytes, &snapshot)
					if err != nil {
						log.Printf("Kraken snapshot unmarshal error: %v", err)
						continue
					}

					log.Printf("Kraken processing trade snapshot with %d trades", len(snapshot.Trades))
					for _, trade := range snapshot.Trades {
						processKrakenTrade(trade.ProductID, trade.Price, trade.Quantity, trade.Side, trade.Timestamp, priceChan, tradeChan)
					}
				} else if feed == "trade" {
					// Handle individual trade
					var trade KrakenFuturesTrade
					messageBytes, _ := json.Marshal(rawMessage)
					err = json.Unmarshal(messageBytes, &trade)
					if err != nil {
						log.Printf("Kraken trade unmarshal error: %v", err)
						continue
					}

					log.Printf("Kraken processing individual trade: %s@%.2f", trade.ProductID, trade.Price)
					processKrakenTrade(trade.ProductID, trade.Price, trade.Quantity, trade.Side, trade.Timestamp, priceChan, tradeChan)
				}
			}
		}

		time.Sleep(2 * time.Second)
	}
}

func convertToKrakenSymbol(symbol string) string {
	// Convert BTCUSDT to PI_XBTUSD (Kraken's format)
	switch symbol {
	case "BTCUSDT":
		return "PI_XBTUSD"
	case "ETHUSDT":
		return "PI_ETHUSD"
	default:
		return symbol
	}
}

func convertFromKrakenSymbol(symbol string) string {
	// Convert PI_XBTUSD to BTCUSDT (standard format)
	switch symbol {
	case "PI_XBTUSD":
		return "BTCUSDT"
	case "PI_ETHUSD":
		return "ETHUSDT"
	default:
		return symbol
	}
}
package exchanges

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

type BitgetTradeMessage struct {
	Action string `json:"action"`
	Arg    struct {
		InstType string `json:"instType"`
		Channel  string `json:"channel"`
		InstID   string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		InstID    string `json:"instId"`
		TradeID   string `json:"tradeId"`
		Price     string `json:"price"`
		Size      string `json:"size"`
		Side      string `json:"side"`
		Timestamp string `json:"ts"`
	} `json:"data"`
}

func ConnectBitgetFutures(symbols []string, priceChan chan<- PriceData, tradeChan chan<- TradeData) {
	wsURL := "wss://ws.bitget.com/mix/v1/stream"

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Bitget connection error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Connected to Bitget futures WebSocket")

		// Subscribe to trades for each symbol
		for _, symbol := range symbols {
			// Convert BTCUSDT to BTCUSDT_UMCBL for Bitget
			bitgetSymbol := convertToBitgetSymbol(symbol)

			subscribeMsg := map[string]interface{}{
				"op": "subscribe",
				"args": []map[string]string{
					{
						"instType": "UMCBL",
						"channel":  "trade",
						"instId":   bitgetSymbol,
					},
				},
			}

			err = conn.WriteJSON(subscribeMsg)
			if err != nil {
				log.Printf("Bitget subscription error for %s: %v", bitgetSymbol, err)
				continue
			}
			log.Printf("Bitget subscribed to trade channel for: %s", bitgetSymbol)
		}

		for {
			var rawMessage map[string]interface{}
			err := conn.ReadJSON(&rawMessage)
			if err != nil {
				log.Printf("Bitget read error: %v", err)
				conn.Close()
				break
			}

			// Check if it's a trade message
			if action, ok := rawMessage["action"].(string); ok && action == "snapshot" {
				if arg, exists := rawMessage["arg"].(map[string]interface{}); exists {
					if channel, channelExists := arg["channel"].(string); channelExists && channel == "trade" {
						var message BitgetTradeMessage
						messageBytes, _ := json.Marshal(rawMessage)
						err = json.Unmarshal(messageBytes, &message)
						if err != nil {
							log.Printf("Bitget trade unmarshal error: %v", err)
							continue
						}

						log.Printf("Bitget processing %d trades for %s", len(message.Data), message.Arg.InstID)
						for _, trade := range message.Data {
							// Convert symbol back to standard format (BTCUSDT_UMCBL -> BTCUSDT)
							symbol := convertFromBitgetSymbol(trade.InstID)

							// Parse price and timestamp
							price, err := strconv.ParseFloat(trade.Price, 64)
							if err != nil {
								log.Printf("Bitget price parse error: %v", err)
								continue
							}

							timestamp, err := strconv.ParseInt(trade.Timestamp, 10, 64)
							if err != nil {
								log.Printf("Bitget timestamp parse error: %v", err)
								continue
							}

							// Normalize trade side (Bitget uses "buy" and "sell")
							var side string
							if trade.Side == "buy" {
								side = "buy"
							} else {
								side = "sell"
							}

							priceData := PriceData{
								Symbol:    symbol,
								Exchange:  "bitget_futures",
								Price:     price,
								Timestamp: timestamp,
							}

							tradeData := TradeData{
								Symbol:    symbol,
								Exchange:  "bitget_futures",
								Price:     price,
								Quantity:  trade.Size,
								Side:      side,
								Timestamp: timestamp,
							}

							priceChan <- priceData
							tradeChan <- tradeData
						}
					}
				}
			}
		}

		time.Sleep(2 * time.Second)
	}
}

func convertToBitgetSymbol(symbol string) string {
	// Convert BTCUSDT to BTCUSDT_UMCBL (Bitget's USDT-M perpetual futures format)
	switch symbol {
	case "BTCUSDT":
		return "BTCUSDT_UMCBL"
	case "ETHUSDT":
		return "ETHUSDT_UMCBL"
	default:
		return symbol + "_UMCBL"
	}
}

func convertFromBitgetSymbol(symbol string) string {
	// Convert BTCUSDT_UMCBL to BTCUSDT (standard format)
	switch symbol {
	case "BTCUSDT_UMCBL":
		return "BTCUSDT"
	case "ETHUSDT_UMCBL":
		return "ETHUSDT"
	default:
		// Remove _UMCBL suffix if present
		if len(symbol) > 6 && symbol[len(symbol)-6:] == "_UMCBL" {
			return symbol[:len(symbol)-6]
		}
		return symbol
	}
}
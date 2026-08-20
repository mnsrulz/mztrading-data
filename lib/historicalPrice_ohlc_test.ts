import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts";
import { mapWorkerOhlcRow, type OptionOhlcRow } from "./historicalPrice.ts";

Deno.test("mapWorkerOhlcRow - maps worker row to option ohlc row", () => {
    const result = mapWorkerOhlcRow({
        date: "2025-05-12",
        open: 16,
        high: 19.55,
        low: 16,
        close: 19.549999237060547,
        volume: 8,
        adjclose: 19.549999237060547,
    });
    const expected: OptionOhlcRow = {
        dt: "2025-05-12",
        open: 16,
        high: 19.55,
        low: 16,
        close: 19.549999237060547,
        volume: 8,
    };
    assertEquals(result, expected);
});

Deno.test("mapWorkerOhlcRow - drops adjclose even when missing", () => {
    const result = mapWorkerOhlcRow({
        date: "2025-05-13",
        open: 1,
        high: 2,
        low: 0.5,
        close: 1.5,
        volume: 0,
    });
    assertEquals(Object.keys(result).sort(), ["close", "dt", "high", "low", "open", "volume"]);
    assertEquals(result.volume, 0);
});
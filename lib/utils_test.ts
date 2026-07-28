import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts";
import { classifyExpirations, type ExpirationFlagsEntry } from "./utils.ts";

function toMap(entries: ExpirationFlagsEntry[]): Map<string, { isWeekly: boolean; isMonthly: boolean }> {
    return new Map(entries.map(e => [e.expiration, { isWeekly: e.isWeekly, isMonthly: e.isMonthly }]));
}

Deno.test("classifyExpirations - empty set", () => {
    assertEquals(classifyExpirations(new Set()), []);
});

Deno.test("classifyExpirations - single date", () => {
    const result = classifyExpirations(new Set(["2025-01-10"]));
    assertEquals(result, [{ expiration: "2025-01-10", isWeekly: true, isMonthly: false }]);
});

Deno.test("classifyExpirations - Friday OPEX is weekly + monthly", () => {
    const result = toMap(classifyExpirations(new Set([
        "2025-01-13",
        "2025-01-14",
        "2025-01-15",
        "2025-01-16",
        "2025-01-17",
    ])));
    assertEquals(result.get("2025-01-13"), { isWeekly: false, isMonthly: false });
    assertEquals(result.get("2025-01-14"), { isWeekly: false, isMonthly: false });
    assertEquals(result.get("2025-01-15"), { isWeekly: false, isMonthly: false });
    assertEquals(result.get("2025-01-16"), { isWeekly: false, isMonthly: false });
    assertEquals(result.get("2025-01-17"), { isWeekly: true, isMonthly: true });
});

Deno.test("classifyExpirations - multiple weeks", () => {
    const result = toMap(classifyExpirations(new Set([
        "2025-01-06",
        "2025-01-10",
        "2025-01-13",
        "2025-01-17",
        "2025-01-20",
        "2025-01-24",
    ])));
    assertEquals(result.get("2025-01-06"), { isWeekly: false, isMonthly: false });
    assertEquals(result.get("2025-01-10"), { isWeekly: true, isMonthly: false });
    assertEquals(result.get("2025-01-13"), { isWeekly: false, isMonthly: false });
    assertEquals(result.get("2025-01-17"), { isWeekly: true, isMonthly: true });
    assertEquals(result.get("2025-01-20"), { isWeekly: false, isMonthly: false });
    assertEquals(result.get("2025-01-24"), { isWeekly: true, isMonthly: false });
});

Deno.test("classifyExpirations - month boundary crossing weeks", () => {
    const result = toMap(classifyExpirations(new Set([
        "2025-01-29",
        "2025-01-31",
        "2025-02-03",
        "2025-02-07",
    ])));
    assertEquals(result.get("2025-01-29"), { isWeekly: false, isMonthly: false });
    assertEquals(result.get("2025-01-31"), { isWeekly: true, isMonthly: false });
    assertEquals(result.get("2025-02-03"), { isWeekly: false, isMonthly: false });
    assertEquals(result.get("2025-02-07"), { isWeekly: true, isMonthly: false });
});

Deno.test("classifyExpirations - weekly but day not in 15-21 is not monthly", () => {
    const result = toMap(classifyExpirations(new Set([
        "2025-02-07",
        "2025-02-10",
    ])));
    assertEquals(result.get("2025-02-07"), { isWeekly: true, isMonthly: false });
});

Deno.test("classifyExpirations - mid-month Friday on day 21 is monthly", () => {
    const result = toMap(classifyExpirations(new Set([
        "2025-03-17",
        "2025-03-21",
    ])));
    assertEquals(result.get("2025-03-21"), { isWeekly: true, isMonthly: true });
});

Deno.test("classifyExpirations - single date mid-month not a Friday", () => {
    const result = classifyExpirations(new Set(["2025-04-16"]));
    assertEquals(result, [{ expiration: "2025-04-16", isWeekly: true, isMonthly: true }]);
});

Deno.test("classifyExpirations - returns sorted chronologically", () => {
    const result = classifyExpirations(new Set([
        "2025-03-21",
        "2025-01-17",
        "2025-02-21",
        "2025-04-18",
    ]));
    assertEquals(result.map(e => e.expiration), ["2025-01-17", "2025-02-21", "2025-03-21", "2025-04-18"]);
});

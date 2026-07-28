export const  getWeekOfMonth = (date: number, month: number, year: number) => {
    const firstDay = new Date(year, month, 1);
    const firstWeekday = firstDay.getDay();
    return Math.ceil((date + firstWeekday) / 7);
}

export type ExpirationFlags = { isWeekly: boolean; isMonthly: boolean };
export type ExpirationFlagsEntry = { expiration: string } & ExpirationFlags;

function getISOWeekStart(dateStr: string): string {
    const d = new Date(dateStr + "T00:00:00Z");
    const day = d.getUTCDay();
    const diff = d.getUTCDate() - day + (day === 0 ? -6 : 1);
    d.setUTCDate(diff);
    return d.toISOString().slice(0, 10);
}

export function classifyExpirations(expirations: Set<string>): ExpirationFlagsEntry[] {
    const lookup: Record<string, ExpirationFlags> = {};
    const sorted = [...expirations].sort();
    for (const exp of sorted) {
        lookup[exp] = { isWeekly: false, isMonthly: false };
    }

    const weekGroups = new Map<string, string[]>();
    for (const exp of sorted) {
        const weekStart = getISOWeekStart(exp);
        const group = weekGroups.get(weekStart);
        if (group) {
            group.push(exp);
        } else {
            weekGroups.set(weekStart, [exp]);
        }
    }

    for (const dates of weekGroups.values()) {
        const maxDate = dates.reduce((a, b) => a > b ? a : b);
        lookup[maxDate].isWeekly = true;
    }

    for (const exp of sorted) {
        if (lookup[exp].isWeekly) {
            const day = parseInt(exp.slice(8, 10), 10);
            if (day >= 15 && day <= 21) {
                lookup[exp].isMonthly = true;
            }
        }
    }

    return sorted.map(exp => ({ expiration: exp, ...lookup[exp] }));
}
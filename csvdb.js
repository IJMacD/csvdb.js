export class CSVDB {
    #headers;
    #rows;
    get rowCount() {
        return this.#rows.length;
    }
    get headers() {
        return this.#headers;
    }
    constructor(csv) {
        const [headerLine, ...restLines] = csv.trim().split("\n");
        this.#headers = parseCSVLine(headerLine);
        const rows = restLines.map(parseCSVLine);
        this.#rows = rows.map((row) => zip(this.#headers, row));
    }
    [Symbol.iterator]() {
        return this.query()[Symbol.iterator]();
    }
    query() {
        return new CSVDBQuery(this.#rows);
    }
    static except(resultsA, resultsB) {
        return new CSVDBQuery(except(resultsA, resultsB));
    }
    static intersect(resultsA, resultsB) {
        return new CSVDBQuery(intersect(resultsA, resultsB));
    }
    static union(resultsA, resultsB) {
        return CSVDB.unionAll(resultsA, resultsB).distinct();
    }
    static unionAll(resultsA, resultsB) {
        return new CSVDBQuery(unionAll(resultsA, resultsB));
    }
}
export class CSVDBQuery {
    #rows;
    #join = [];
    #where = [];
    #groupBy = null;
    #selection = null;
    #sort = null;
    #windowSpecs = new Map();
    #offset = 0;
    #limit = Infinity;
    #distinct = false;
    #internalIterator;
    constructor(rows) {
        this.#rows = rows;
    }
    query() {
        return new CSVDBQuery(this);
    }
    join(joinSpec) {
        this.#join.push(joinSpec);
        return this;
    }
    joinOn(other, on = () => true) {
        let otherCache;
        this.#join.push((rowA) => {
            if (typeof otherCache === "undefined") {
                otherCache = [...other];
            }
            const out = [];
            if (rowA) {
                for (const rowB of otherCache) {
                    if (on(rowA, rowB)) {
                        out.push({ ...rowA, ...rowB });
                    }
                }
            }
            return out;
        });
        return this;
    }
    where(predicate) {
        this.#where.push(predicate);
        return this;
    }
    groupBy(discriminator) {
        if (typeof discriminator === "string") {
            const d = discriminator;
            discriminator = (row) => row[d];
        }
        this.#groupBy = discriminator;
        return this;
    }
    select(selection) {
        if (Array.isArray(selection)) {
            this.#selection = {};
            for (const col of selection) {
                this.#selection[col] = col;
            }
        }
        else {
            this.#selection = selection;
        }
        return this;
    }
    orderBy(comparator) {
        if (typeof comparator === "string") {
            const c = comparator;
            if (c[0] === "-") {
                const f = comparator.substring(1);
                this.#sort = (rowA, rowB) => +rowB[f] - +rowA[f];
            }
            else if (c[0] === "+") {
                const f = comparator.substring(1);
                this.#sort = (rowA, rowB) => +rowA[f] - +rowB[f];
            }
            else {
                this.#sort = (rowA, rowB) => String(rowA[c]).localeCompare(rowB[c]);
            }
        }
        else {
            this.#sort = comparator;
        }
        return this;
    }
    offset(rows) {
        this.#offset = rows;
        return this;
    }
    fetchFirst(rows) {
        this.#limit = rows;
        return this;
    }
    window(name, spec) {
        this.#windowSpecs.set(name, spec);
        return this;
    }
    distinct(distinct = true) {
        this.#distinct = distinct;
        return this;
    }
    toArray() {
        return [...this];
    }
    getNextRow() {
        if (!this.#internalIterator) {
            this.#internalIterator = this[Symbol.iterator]();
        }
        return this.#internalIterator.next().value;
    }
    getNextValue(column = 0) {
        const row = this.getNextRow();
        return row
            ? typeof column === "string"
                ? row[column]
                : Object.values(row)[column]
            : undefined;
    }
    [Symbol.iterator]() {
        return this.#iter();
    }
    *#iter() {
        if (this.#limit === 0) {
            return;
        }
        let rows = this.#rows;
        for (const join of this.#join) {
            const newRows = [];
            for (const row of rows) {
                const joinResult = join(row);
                joinResult && newRows.push(...joinResult);
            }
            const joinResult = join(null);
            joinResult && newRows.push(...joinResult);
            rows = newRows;
        }
        for (const predicate of this.#where) {
            rows = filter(rows, predicate);
        }
        if (this.#sort) {
            rows = [...rows].sort(this.#sort);
        }
        let rowGroups = rows;
        let allRowGroup = undefined;
        const haveWindowFunctions = this.#windowSpecs.size > 0 ||
            (this.#selection &&
                Object.values(this.#selection).some((s) => (typeof s === "string" && s.endsWith(" OVER ()")) ||
                    Array.isArray(s)));
        if (this.#groupBy) {
            rowGroups = groupRows(rows, this.#groupBy);
        }
        else if (this.#hasAggregates()) {
            rowGroups = [[...rows]];
        }
        else if (haveWindowFunctions) {
            allRowGroup = [...rows];
        }
        const distinctCache = [];
        let i = 0;
        for (const rowGroupOrRow of rowGroups) {
            const isArrayOfArrays = Array.isArray(rowGroupOrRow);
            const sourceRow = isArrayOfArrays ? rowGroupOrRow[0] : rowGroupOrRow;
            const rowGroup = isArrayOfArrays
                ? rowGroupOrRow
                : allRowGroup || [sourceRow];
            const result = this.#mapSelectionToRow(sourceRow, this.#selection, i + 1, rowGroup);
            if (this.#distinct) {
                if (!isDistinct(distinctCache, result)) {
                    continue;
                }
                distinctCache.push(result);
            }
            if (i >= this.#offset) {
                if (i - this.#offset >= this.#limit) {
                    return;
                }
                yield result;
            }
            i++;
        }
    }
    #hasAggregates() {
        if (!this.#selection)
            return false;
        return Object.values(this.#selection).some((s) => typeof s === "string" && isAggregate(s));
    }
    #mapSelectionToRow(sourceRow, selection, index, groupRows) {
        const out = {};
        if (!selection) {
            return sourceRow;
        }
        for (const [alias, col] of Object.entries(selection)) {
            let fn;
            let field;
            let fnName;
            let args;
            let windowName;
            let windowSpec;
            const re = /^([A-Z_]+)\(([^)]*)\)(?:\s+OVER\s+([\w\d_]+|\(\)))?$/;
            if (col instanceof Function) {
                out[alias] = col(sourceRow, index, groupRows);
                continue;
            }
            if (Array.isArray(col)) {
                const [fnOrFnName, windowNameOrSpec] = col;
                if (typeof fnOrFnName === "string") {
                    const fnString = fnOrFnName;
                    const aggregateMatch = re.exec(fnString);
                    if (aggregateMatch) {
                        fnName = aggregateMatch[1];
                        args = aggregateMatch[2].split(",");
                        if (aggregateMatch[3]) {
                            throw Error("Unexpected OVER");
                        }
                    }
                    else {
                        throw Error(`Bad Func: ${fnString}`);
                    }
                }
                else {
                    fn = fnOrFnName;
                }
                if (typeof windowNameOrSpec === "string") {
                    windowName = windowNameOrSpec;
                }
                else {
                    windowSpec = windowNameOrSpec;
                }
            }
            else {
                const aggregateMatch = re.exec(col);
                if (aggregateMatch) {
                    fnName = aggregateMatch[1];
                    args = aggregateMatch[2].split(",");
                    windowName = aggregateMatch[3];
                }
                else {
                    field = col;
                }
            }
            if (windowName) {
                windowSpec =
                    windowName === "()" ? {} : this.#windowSpecs.get(windowName);
                if (!windowSpec) {
                    throw Error(`Bad Window: ${windowName}`);
                }
            }
            let rows = groupRows;
            if (windowSpec) {
                rows = applyWindow(rows, windowSpec, sourceRow);
            }
            if (fn) {
                out[alias] = fn(sourceRow, index, rows);
                continue;
            }
            if (fnName && args) {
                let value;
                if (fnName === "ROW_NUMBER") {
                    value = rows.indexOf(sourceRow) + 1;
                }
                else if (fnName in AGGREGATE_FUNCTIONS) {
                    let values = rows.map((row) => row[args[0]]);
                    value = AGGREGATE_FUNCTIONS[fnName](values);
                }
                else if (fnName in WINDOW_FUNCTIONS && windowSpec) {
                    orderByCheck(windowSpec, fnName);
                    value = WINDOW_FUNCTIONS[fnName](sourceRow, rows, args, windowSpec);
                }
                else if (fnName in POSITION_FUNCTIONS && windowSpec) {
                    orderByCheck(windowSpec, fnName);
                    let values = rows.map((row) => row[args[0]]);
                    value = POSITION_FUNCTIONS[fnName](sourceRow, rows, args, values);
                }
                else if (fnName in STAT_FUNCTIONS) {
                    let values = rows.map((row) => row[args[0]]);
                    value = STAT_FUNCTIONS[fnName](values);
                }
                else {
                    throw Error(`Bad Func: ${fnName}`);
                }
                out[alias] = value;
                continue;
            }
            if (sourceRow) {
                if (col === "*") {
                    Object.assign(out, alias === "*"
                        ? sourceRow
                        : Object.fromEntries(Object.entries(sourceRow).map(([key, value]) => [
                            `${alias}${key}`,
                            value,
                        ])));
                }
                else if (field) {
                    out[alias] = sourceRow[field];
                }
            }
        }
        return out;
    }
}
function applyWindow(rows, windowSpec, sourceRow) {
    if (windowSpec.partitionBy) {
        const pb = windowSpec.partitionBy;
        const fn = typeof pb === "string" ? (row) => row[pb] : pb;
        const sympatheticValue = fn(sourceRow);
        rows = rows.filter((row) => fn(row) === sympatheticValue);
    }
    if (windowSpec.orderBy) {
        const orderBy = getOrderBy(windowSpec);
        rows = [...rows].sort(orderBy);
        let framingStart = -Infinity;
        let framingEnd = 0;
        if (windowSpec.framing) {
            const unit = windowSpec.framing[0];
            if (unit !== "ROWS") {
                throw Error(`Window unit ${unit}`);
            }
            const CURRENT_ROW = "CURRENT ROW";
            if (typeof windowSpec.framing[1] === "number") {
                framingStart = windowSpec.framing[1];
            }
            else if (windowSpec.framing[1] === "UNBOUNDED PRECEDING") {
                framingStart = -Infinity;
            }
            else if (windowSpec.framing[1] === CURRENT_ROW) {
                framingStart = 0;
            }
            if (typeof windowSpec.framing[2] === "number") {
                framingEnd = windowSpec.framing[2];
            }
            else if (windowSpec.framing[2] === "UNBOUNDED FOLLOWING") {
                framingEnd = Infinity;
            }
            else if (windowSpec.framing[2] === CURRENT_ROW) {
                framingEnd = 0;
            }
        }
        const index = rows.indexOf(sourceRow);
        const startIndex = Math.max(index + framingStart, 0);
        const endIndex = index + framingEnd + 1;
        rows = rows.slice(startIndex, endIndex);
    }
    return rows;
}
function getOrderBy(windowSpec) {
    if (typeof windowSpec.orderBy === "string") {
        let k = windowSpec.orderBy;
        if (k[0] === "+") {
            k = k.substring(1);
            return (rowA, rowB) => +rowA[k] - +rowB[k];
        }
        return (rowA, rowB) => rowA[k].localeCompare(rowB[k]);
    }
    return windowSpec.orderBy;
}
function orderByCheck(windowSpec, fnName) {
    if (!windowSpec?.orderBy)
        throw Error(`ORDER BY required: ${fnName}`);
}
function zip(keys, values) {
    const out = {};
    for (let i = 0; i < keys.length; i++) {
        const key = keys[i];
        out[key] = values[i];
    }
    return out;
}
function groupRows(rows, discriminator) {
    const resultSet = new Map();
    for (const row of rows) {
        const value = discriminator(row);
        if (!resultSet.has(value)) {
            resultSet.set(value, []);
        }
        resultSet.get(value)?.push(row);
    }
    return [...resultSet.values()];
}
const isAggregate = (col) => typeof col === "string" && /^[A-Z]+\([^)]*\)$/.test(col);
function parseCSVLine(line) {
    line = line.trim();
    const matches = line.matchAll(/([^",]*|"[^"]*")(,|$)/g);
    const m = [...matches];
    if (m[m.length - 1][0].length === 0) {
        m.length--;
    }
    return m.map((match) => match[1].trim().replace(/^"|"$/g, ""));
}
function* filter(iterable, predicate) {
    let i = 0;
    for (const item of iterable) {
        if (predicate(item, i++)) {
            yield item;
        }
    }
}
function isDistinct(rows, row) {
    return rows.every((rowB) => !isSame(row, rowB));
}
function isSame(rowA, rowB) {
    const keysA = Object.keys(rowA);
    const keysB = Object.keys(rowB);
    if (keysA.length !== keysB.length)
        return false;
    return keysA.every((key) => rowA[key] === rowB[key]);
}
function* except(resultsA, resultsB) {
    const cache = [...resultsB];
    for (const result of resultsA) {
        if (!isDistinct(cache, result)) {
            continue;
        }
        cache.push(result);
        yield result;
    }
}
function* intersect(resultsA, resultsB) {
    const cache = [...resultsB];
    for (const result of resultsA) {
        if (isDistinct(cache, result)) {
            continue;
        }
        yield result;
    }
}
function* unionAll(resultsA, resultsB) {
    for (const result of resultsA) {
        yield result;
    }
    for (const result of resultsB) {
        yield result;
    }
}
const SUM = (values) => values.reduce((total, v) => total + +v, 0);
const AGGREGATE_FUNCTIONS = {
    SUM,
    AVG: (values) => SUM(values) / values.length,
    MAX: (values) => Math.max(...values),
    MIN: (values) => Math.min(...values),
    COUNT: (values) => values.length,
    LISTAGG: (values) => values.join(),
    ARRAY: (values) => values,
    JSON: (values) => JSON.stringify(values),
    ANY: (values) => values[0],
    RANDOM: (values) => values[Math.floor(Math.random() * values.length)],
};
const WINDOW_FUNCTIONS = {
    RANK: (sourceRow, rows, args, windowSpec) => {
        const orderBy = getOrderBy(windowSpec);
        const index = rows.indexOf(sourceRow);
        let i = index;
        for (; i >= 0; i--) {
            const order = orderBy(rows[i], sourceRow);
            if (order !== 0)
                break;
        }
        return i + 2;
    },
    DENSE_RANK: (sourceRow, rows, args, windowSpec) => {
        const orderBy = getOrderBy(windowSpec);
        const index = rows.indexOf(sourceRow);
        let count = 0;
        for (let i = 1; i <= index; i++) {
            const order = orderBy(rows[i - 1], rows[i]);
            if (order === 0)
                count++;
        }
        return index - count + 1;
    },
    NTILE: (sourceRow, rows, args, windowSpec) => {
        const index = rows.indexOf(sourceRow);
        return Math.floor((+args[0] * index) / rows.length) + 1;
    },
    PERCENT_RANK: (sourceRow, rows, args, windowSpec) => {
        if (rows.length === 1) {
            return 0;
        }
        const orderBy = getOrderBy(windowSpec);
        const index = rows.indexOf(sourceRow);
        let i = index;
        for (; i >= 0; i--) {
            const order = orderBy(rows[i], sourceRow);
            if (order !== 0)
                break;
        }
        return (i + 1) / (rows.length - 1);
    },
    CUME_DIST: (sourceRow, rows, args, windowSpec) => {
        const orderBy = getOrderBy(windowSpec);
        const index = rows.indexOf(sourceRow);
        let i = index + 1;
        for (; i < rows.length; i++) {
            const order = orderBy(rows[i], sourceRow);
            if (order !== 0)
                break;
        }
        return i / rows.length;
    },
    PERCENTILE_DIST: (sourceRow, rows, args, windowSpec) => {
        const result = findPercentile(rows, +args[0], windowSpec);
        if (result) {
            const [index, key] = result;
            return rows[index][key];
        }
        return null;
    },
    PERCENTILE_CONT: (sourceRow, rows, args, windowSpec) => {
        const result = findPercentile(rows, +args[0], windowSpec);
        if (result) {
            const [index, key, x] = result;
            const a = +rows[index - 1][key];
            const b = +rows[index][key];
            return x * (b - a) + a;
        }
        return null;
    },
};
function findPercentile(rows, percentile, windowSpec) {
    if (typeof windowSpec?.orderBy !== "string") {
        throw Error(`ORDER BY must be string`);
    }
    let k = windowSpec.orderBy;
    if (k[0] === "+") {
        k = k.substring(1);
    }
    let prevP = 0;
    for (let i = 0; i < rows.length; i++) {
        let j = i + 1;
        for (; j < rows.length; j++) {
            if (rows[i][k] !== rows[j][k])
                break;
        }
        const p = j / rows.length;
        if (p >= percentile) {
            const x = (percentile - prevP) / (p - prevP);
            return [i, k, x];
        }
        prevP = p;
    }
    return null;
}
const POSITION_FUNCTIONS = {
    LEAD: (sourceRow, rows, args, values) => {
        const index = rows.indexOf(sourceRow);
        let delta = 1;
        if (args.length > 1)
            delta = +args[1];
        return values[index + delta] || null;
    },
    LAG: (sourceRow, rows, args, values) => {
        let delta = 1;
        if (args.length > 1)
            delta = +args[1];
        const index = rows.indexOf(sourceRow);
        return values[index - delta] || null;
    },
    FIRST_VALUE: (sourceRow, rows, args, values) => {
        return values[0];
    },
    LAST_VALUE: (sourceRow, rows, args, values) => {
        return values[values.length - 1];
    },
    NTH_VALUE: (sourceRow, rows, args, values) => {
        return values[+args[1] - 1] || null;
    },
};
const VARIANCE_SUM = (values) => {
    const n = values.length;
    const mean = SUM(values) / n;
    const sum = values.reduce((total, v) => total + Math.pow(+v - mean, 2), 0);
    return Math.sqrt(sum / n);
};
const STAT_FUNCTIONS = {
    STDDEV_POP: (values) => Math.sqrt(VARIANCE_SUM(values) / values.length),
    STDDEV_SAMP: (values) => Math.sqrt(VARIANCE_SUM(values) / (values.length - 1)),
    VAR_POP: (values) => VARIANCE_SUM(values) / values.length,
    VAR_SAMP: (values) => VARIANCE_SUM(values) / (values.length - 1),
};

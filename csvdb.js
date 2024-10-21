var __classPrivateFieldGet = (this && this.__classPrivateFieldGet) || function (receiver, state, kind, f) {
    if (kind === "a" && !f) throw new TypeError("Private accessor was defined without a getter");
    if (typeof state === "function" ? receiver !== state || !f : !state.has(receiver)) throw new TypeError("Cannot read private member from an object whose class did not declare it");
    return kind === "m" ? f : kind === "a" ? f.call(receiver) : f ? f.value : state.get(receiver);
};
var __classPrivateFieldSet = (this && this.__classPrivateFieldSet) || function (receiver, state, value, kind, f) {
    if (kind === "m") throw new TypeError("Private method is not writable");
    if (kind === "a" && !f) throw new TypeError("Private accessor was defined without a setter");
    if (typeof state === "function" ? receiver !== state || !f : !state.has(receiver)) throw new TypeError("Cannot write private member to an object whose class did not declare it");
    return (kind === "a" ? f.call(receiver, value) : f ? f.value = value : state.set(receiver, value)), value;
};
var _CSVDB_headers, _CSVDB_rows, _CSVDBQuery_instances, _CSVDBQuery_rows, _CSVDBQuery_join, _CSVDBQuery_where, _CSVDBQuery_groupBy, _CSVDBQuery_selection, _CSVDBQuery_sort, _CSVDBQuery_windowSpecs, _CSVDBQuery_limit, _CSVDBQuery_distinct, _CSVDBQuery_internalIterator, _CSVDBQuery_hasAggregates, _CSVDBQuery_mapSelectionToRow;
export class CSVDB {
    get rowCount() {
        return __classPrivateFieldGet(this, _CSVDB_rows, "f").length;
    }
    get headers() {
        return __classPrivateFieldGet(this, _CSVDB_headers, "f");
    }
    constructor(csv) {
        _CSVDB_headers.set(this, void 0);
        _CSVDB_rows.set(this, void 0);
        const [headerLine, ...restLines] = csv.trim().split("\n");
        __classPrivateFieldSet(this, _CSVDB_headers, parseCSVLine(headerLine), "f");
        const rows = restLines.map(parseCSVLine);
        __classPrivateFieldSet(this, _CSVDB_rows, rows.map(row => zip(__classPrivateFieldGet(this, _CSVDB_headers, "f"), row)), "f");
    }
    [(_CSVDB_headers = new WeakMap(), _CSVDB_rows = new WeakMap(), Symbol.iterator)]() {
        return this.query()[Symbol.iterator]();
    }
    query() {
        return new CSVDBQuery(__classPrivateFieldGet(this, _CSVDB_rows, "f"));
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
class CSVDBQuery {
    constructor(rows) {
        _CSVDBQuery_instances.add(this);
        _CSVDBQuery_rows.set(this, void 0);
        _CSVDBQuery_join.set(this, []);
        _CSVDBQuery_where.set(this, []);
        _CSVDBQuery_groupBy.set(this, null);
        _CSVDBQuery_selection.set(this, null);
        _CSVDBQuery_sort.set(this, null);
        _CSVDBQuery_windowSpecs.set(this, new Map());
        _CSVDBQuery_limit.set(this, Infinity);
        _CSVDBQuery_distinct.set(this, false);
        _CSVDBQuery_internalIterator.set(this, null);
        __classPrivateFieldSet(this, _CSVDBQuery_rows, rows, "f");
    }
    /**
     * Materialise rows and create a new query object from them
     */
    query() {
        return new CSVDBQuery(this);
    }
    /**
     * Callback `join` is called once per row of the current set.
     * The callback should return zero or more rows as appropriate to implement
     * the join.
     * The `RowObject`s returned from the callback conceptually should include all
     * fields from existing the row, but they don't have to.
     * Multiple joins can be added with multiple calls to the `join()` method
     * and will be executed in sequence.
     * Note: the `join()` method will be called one extra time with the row set
     * to `null`. This allows RIGHT JOIN and FULL OUTER JOIN to be implemented.
     */
    join(join) {
        __classPrivateFieldGet(this, _CSVDBQuery_join, "f").push(join);
        return this;
    }
    /**
     * Helper method to join two Queries.
     * `on` is a Callback which is given two rows (one from each side of the
     * join) and returns a boolean to indicate whether or not this match should
     * be included in the result set.
     * If `on` is not provided then the result is a cartesian join.
     */
    joinOn(other, on = () => true) {
        let otherCache;
        __classPrivateFieldGet(this, _CSVDBQuery_join, "f").push(rowA => {
            // Materialise `other` just once
            if (typeof otherCache === "undefined") {
                // @ts-ignore
                otherCache = [...other];
            }
            const out = [];
            if (rowA) {
                for (const rowB of otherCache) {
                    if (on(rowA, rowB)) {
                        out.push(Object.assign(Object.assign({}, rowA), rowB));
                    }
                }
            }
            return out;
        });
        return this;
    }
    /**
     * Multiple calls will be AND'd together
     */
    where(predicate) {
        __classPrivateFieldGet(this, _CSVDBQuery_where, "f").push(predicate);
        return this;
    }
    groupBy(discriminator) {
        if (typeof discriminator === "string") {
            const d = discriminator;
            discriminator = row => row[d];
        }
        __classPrivateFieldSet(this, _CSVDBQuery_groupBy, discriminator, "f");
        return this;
    }
    select(selection) {
        if (Array.isArray(selection)) {
            __classPrivateFieldSet(this, _CSVDBQuery_selection, {}, "f");
            for (const col of selection) {
                __classPrivateFieldGet(this, _CSVDBQuery_selection, "f")[col] = col;
            }
        }
        else {
            __classPrivateFieldSet(this, _CSVDBQuery_selection, selection, "f");
        }
        return this;
    }
    /**
     * Sorts based on input rows.
     * To sort based on output use `.toArray().sort()`
     */
    orderBy(comparator) {
        __classPrivateFieldSet(this, _CSVDBQuery_sort, comparator, "f");
        return this;
    }
    fetchFirst(limit) {
        __classPrivateFieldSet(this, _CSVDBQuery_limit, limit, "f");
        return this;
    }
    window(name, spec) {
        __classPrivateFieldGet(this, _CSVDBQuery_windowSpecs, "f").set(name, spec);
        return this;
    }
    distinct(distinct = true) {
        __classPrivateFieldSet(this, _CSVDBQuery_distinct, distinct, "f");
        return this;
    }
    /**
     * Materialise result rows
     */
    toArray() {
        return [...this];
    }
    getNextRow() {
        if (!__classPrivateFieldGet(this, _CSVDBQuery_internalIterator, "f")) {
            __classPrivateFieldSet(this, _CSVDBQuery_internalIterator, this[Symbol.iterator](), "f");
        }
        return __classPrivateFieldGet(this, _CSVDBQuery_internalIterator, "f").next().value;
    }
    getNextValue(column = 0) {
        const row = this.getNextRow();
        return row ? Object.values(row)[column] : undefined;
    }
    [(_CSVDBQuery_rows = new WeakMap(), _CSVDBQuery_join = new WeakMap(), _CSVDBQuery_where = new WeakMap(), _CSVDBQuery_groupBy = new WeakMap(), _CSVDBQuery_selection = new WeakMap(), _CSVDBQuery_sort = new WeakMap(), _CSVDBQuery_windowSpecs = new WeakMap(), _CSVDBQuery_limit = new WeakMap(), _CSVDBQuery_distinct = new WeakMap(), _CSVDBQuery_internalIterator = new WeakMap(), _CSVDBQuery_instances = new WeakSet(), Symbol.iterator)]() {
        const self = this;
        function* iter() {
            if (__classPrivateFieldGet(self, _CSVDBQuery_limit, "f") === 0) {
                return;
            }
            let rows = __classPrivateFieldGet(self, _CSVDBQuery_rows, "f");
            for (const join of __classPrivateFieldGet(self, _CSVDBQuery_join, "f")) {
                const newRows = [];
                for (const row of rows) {
                    newRows.push(...join(row));
                }
                newRows.push(...join(null));
                rows = newRows;
            }
            // WHERE
            for (const predicate of __classPrivateFieldGet(self, _CSVDBQuery_where, "f")) {
                rows = filter(rows, predicate);
            }
            // ORDER BY
            if (__classPrivateFieldGet(self, _CSVDBQuery_sort, "f")) {
                // Need to materialise the rows in order to sort
                rows = [...rows].sort(__classPrivateFieldGet(self, _CSVDBQuery_sort, "f"));
            }
            // GROUP BY
            let rowGroups = rows;
            const haveWindowFunctions = __classPrivateFieldGet(self, _CSVDBQuery_windowSpecs, "f").size > 0 ||
                (__classPrivateFieldGet(self, _CSVDBQuery_selection, "f") &&
                    Object.values(__classPrivateFieldGet(self, _CSVDBQuery_selection, "f"))
                        .some(s => (typeof s === "string" && s.endsWith(" OVER ()")) || Array.isArray(s)));
            if (__classPrivateFieldGet(self, _CSVDBQuery_groupBy, "f")) {
                // groupRows() will materialise the rows
                rowGroups = groupRows(rows, __classPrivateFieldGet(self, _CSVDBQuery_groupBy, "f"));
            }
            else if (__classPrivateFieldGet(self, _CSVDBQuery_instances, "m", _CSVDBQuery_hasAggregates).call(self)) {
                // We're going to have to materialise the rows anyway so do it now
                rowGroups = [[...rows]];
            }
            else if (haveWindowFunctions) {
                // Unfortunately we need to materialise the rows once to pass as the
                // 4th argument to mapSelectionToRow()
                rowGroups = [[...rows]];
            }
            const distinctCache = [];
            // SELECT
            let i = 1;
            for (const rowGroupOrRow of rowGroups) {
                // Two modes: array of arrays or single iterable list
                const isIterator = !Array.isArray(rowGroupOrRow);
                const sourceRow = isIterator ? rowGroupOrRow : rowGroupOrRow[0];
                const rowGroup = isIterator ? [sourceRow] : rowGroupOrRow;
                const result = __classPrivateFieldGet(self, _CSVDBQuery_instances, "m", _CSVDBQuery_mapSelectionToRow).call(self, sourceRow, __classPrivateFieldGet(self, _CSVDBQuery_selection, "f"), i, rowGroup);
                if (__classPrivateFieldGet(self, _CSVDBQuery_distinct, "f")) {
                    if (!isDistinct(distinctCache, result)) {
                        continue;
                    }
                    distinctCache.push(result);
                }
                yield result;
                // FETCH FIRST
                if (++i > __classPrivateFieldGet(self, _CSVDBQuery_limit, "f")) {
                    return;
                }
            }
        }
        return iter();
    }
}
_CSVDBQuery_hasAggregates = function _CSVDBQuery_hasAggregates() {
    if (!__classPrivateFieldGet(this, _CSVDBQuery_selection, "f"))
        return false;
    return Object.values(__classPrivateFieldGet(this, _CSVDBQuery_selection, "f")).some(s => typeof s === "string" && isAggregate(s));
}, _CSVDBQuery_mapSelectionToRow = function _CSVDBQuery_mapSelectionToRow(sourceRow, selection, index, groupRows) {
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
        // Easily dealt with
        if (col instanceof Function) {
            out[alias] = col(sourceRow, index, groupRows);
            continue;
        }
        // Now check for array syntax
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
        // It must be a string
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
            windowSpec = windowName === "()" ?
                {} :
                __classPrivateFieldGet(this, _CSVDBQuery_windowSpecs, "f").get(windowName);
            if (!windowSpec) {
                throw Error(`Bad Window: ${windowName}`);
            }
        }
        let rows = groupRows;
        if (windowSpec) {
            rows = applyWindow(rows, windowSpec, sourceRow);
        }
        // If we have a function we can apply it now and continue
        if (fn) {
            out[alias] = fn(sourceRow, index, rows);
            continue;
        }
        // If we have a builtin function name then evaluate it and continue
        if (fnName && args) {
            let value;
            if (fnName === "ROW_NUMBER") {
                value = rows.indexOf(sourceRow) + 1;
            }
            else if (fnName in AGGREGATE_FUNCTIONS) {
                let values = rows.map(row => row[args[0]]);
                value = AGGREGATE_FUNCTIONS[fnName](values);
            }
            else if (fnName in WINDOW_FUNCTIONS && windowSpec) {
                orderByCheck(windowSpec, fnName);
                value = WINDOW_FUNCTIONS[fnName](sourceRow, rows, args, windowSpec);
            }
            else if (fnName in POSITION_FUNCTIONS && windowSpec) {
                orderByCheck(windowSpec, fnName);
                let values = rows.map(row => row[args[0]]);
                value = POSITION_FUNCTIONS[fnName](sourceRow, rows, args, values);
            }
            else if (fnName in STAT_FUNCTIONS) {
                let values = rows.map(row => row[args[0]]);
                value = STAT_FUNCTIONS[fnName](values);
            }
            else {
                throw Error(`Bad Func: ${fnName}`);
            }
            out[alias] = value;
            continue;
        }
        // As long as the source row isn't null we can just copy the properties
        if (sourceRow) {
            if (col === "*") {
                Object.assign(out, (alias === "*") ?
                    sourceRow :
                    Object.fromEntries(Object.entries(sourceRow)
                        .map(([key, value]) => [
                        `${alias}${key}`,
                        value
                    ])));
            }
            else if (field) {
                out[alias] = sourceRow[field];
            }
        }
    }
    return out;
};
function applyWindow(rows, windowSpec, sourceRow) {
    if (windowSpec.partitionBy) {
        const pb = windowSpec.partitionBy;
        const fn = typeof pb === "string" ? (row) => row[pb] : pb;
        const sympatheticValue = fn(sourceRow);
        rows = rows.filter(row => fn(row) === sympatheticValue);
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
    // @ts-ignore
    return windowSpec.orderBy;
}
function orderByCheck(windowSpec, fnName) {
    if (!(windowSpec === null || windowSpec === void 0 ? void 0 : windowSpec.orderBy))
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
    var _a;
    const resultSet = new Map();
    for (const row of rows) {
        const value = discriminator(row);
        if (!resultSet.has(value)) {
            resultSet.set(value, []);
        }
        (_a = resultSet.get(value)) === null || _a === void 0 ? void 0 : _a.push(row);
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
    return m.map(match => match[1].trim().replace(/^"|"$/g, ""));
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
    return rows.every(rowB => !isSame(row, rowB));
}
function isSame(rowA, rowB) {
    const keysA = Object.keys(rowA);
    const keysB = Object.keys(rowB);
    if (keysA.length !== keysB.length)
        return false;
    return keysA.every(key => rowA[key] === rowB[key]);
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
    AVG: values => SUM(values) / values.length,
    MAX: values => Math.max(...values),
    MIN: values => Math.min(...values),
    COUNT: values => values.length,
    LISTAGG: values => values.join(),
    ARRAY: values => values,
    JSON: values => JSON.stringify(values),
    ANY: values => values[0],
    RANDOM: values => values[Math.floor(Math.random() * values.length)],
};
const WINDOW_FUNCTIONS = {
    RANK: (sourceRow, rows, args, windowSpec) => {
        const orderBy = getOrderBy(windowSpec);
        const index = rows.indexOf(sourceRow);
        let i = index;
        for (; i >= 0; i--) {
            // @ts-ignore
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
            // @ts-ignore
            const order = orderBy(rows[i - 1], rows[i]);
            if (order === 0)
                count++;
        }
        return index - count + 1;
    },
    NTILE: (sourceRow, rows, args, windowSpec) => {
        const index = rows.indexOf(sourceRow);
        return Math.floor(+args[0] * index / rows.length) + 1;
    },
    PERCENT_RANK: (sourceRow, rows, args, windowSpec) => {
        if (rows.length === 1) {
            return 0;
        }
        const orderBy = getOrderBy(windowSpec);
        const index = rows.indexOf(sourceRow);
        let i = index;
        for (; i >= 0; i--) {
            // @ts-ignore
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
            // @ts-ignore
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
    if (typeof (windowSpec === null || windowSpec === void 0 ? void 0 : windowSpec.orderBy) !== "string") {
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
            return [
                i,
                k,
                x,
            ];
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
    STDDEV_POP: values => Math.sqrt(VARIANCE_SUM(values) / values.length),
    STDDEV_SAMP: values => Math.sqrt(VARIANCE_SUM(values) / (values.length - 1)),
    VAR_POP: values => VARIANCE_SUM(values) / values.length,
    VAR_SAMP: values => VARIANCE_SUM(values) / (values.length - 1),
};

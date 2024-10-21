import { RowObject, SelectFunction, SelectObject, WindowSpec } from "./types";

export class CSVDB
{
    #headers: string[];
    #rows: RowObject[];

    get rowCount () {
        return this.#rows.length;
    }

    get headers () {
        return this.#headers;
    }

    constructor (csv: string) {
        const [ headerLine, ...restLines ] = csv.trim().split("\n");

        this.#headers = parseCSVLine(headerLine);

        const rows = restLines.map(parseCSVLine);

        this.#rows = rows.map(row => zip(this.#headers, row));
    }

    [Symbol.iterator] () {
        return this.query()[Symbol.iterator]();
    }

    query () {
        return new CSVDBQuery(this.#rows);
    }

    static except (resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>) {
        return new CSVDBQuery(except(resultsA, resultsB));
    }

    static intersect (resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>) {
        return new CSVDBQuery(intersect(resultsA, resultsB));
    }

    static union (resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>) {
        return CSVDB.unionAll(resultsA, resultsB).distinct();
    }

    static unionAll (resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>) {
        return new CSVDBQuery(unionAll(resultsA, resultsB));
    }
}

class CSVDBQuery {
    #rows: Iterable<RowObject>;

    #join: ((row: RowObject | null) => RowObject[])[] = [];
    #where: ((row: RowObject, index: number) => boolean)[] = [];
    #groupBy: ((row: RowObject) => any) | null = null;
    #selection: SelectObject | null = null;
    #sort: ((rowA: RowObject, rowB: RowObject) => number) | null = null;
    #windowSpecs: Map<string, WindowSpec> = new Map();

    #limit = Infinity;

    #distinct = false;

    #internalIterator: Generator<RowObject> | null = null;

    constructor (rows: Iterable<RowObject>) {
        this.#rows = rows;
    }

    /**
     * Materialise rows and create a new query object from them
     */
    query () {
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
    join (join: (row: RowObject | null) => RowObject[]) {
        this.#join.push(join);
        return this;
    }

    /**
     * Helper method to join two Queries.
     * `on` is a Callback which is given two rows (one from each side of the
     * join) and returns a boolean to indicate whether or not this match should
     * be included in the result set.
     * If `on` is not provided then the result is a cartesian join.
     */
    joinOn (other: CSVDBQuery, on: (rowA: RowObject, rowB: RowObject) => boolean = () => true) {
        let otherCache: RowObject[];

        this.#join.push(rowA => {
            // Materialise `other` just once
            if (typeof otherCache === "undefined") {
                // @ts-ignore
                otherCache = [...other];
            }

            const out = [];

            if (rowA){
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

    /**
     * Multiple calls will be AND'd together
     */
    where (predicate: (row: RowObject, index: number) => boolean) {
        this.#where.push(predicate);
        return this;
    }

    groupBy (discriminator: ((row: RowObject) => any) | string) {
        if (typeof discriminator === "string") {
            const d = discriminator;
            discriminator = row => row[d];
        }
        this.#groupBy = discriminator;
        return this;
    }

    select (selection: { [alias: string]: string | ((row: RowObject) => any); } | string[]) {
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

    /**
     * Sorts based on input rows.
     * To sort based on output use `.toArray().sort()`
     */
    orderBy (comparator: (rowA: RowObject, rowB: RowObject) => number) {
        this.#sort = comparator;
        return this;
    }

    fetchFirst (limit: number) {
        this.#limit = limit;
        return this;
    }

    window (name: string, spec: WindowSpec) {
        this.#windowSpecs.set(name, spec);
        return this;
    }

    distinct (distinct = true) {
        this.#distinct = distinct;
        return this;
    }

    /**
     * Materialise result rows
     */
    toArray () {
        return [...this];
    }

    getNextRow () {
        if (!this.#internalIterator) {
            this.#internalIterator = this[Symbol.iterator]();
        }

        return this.#internalIterator.next().value;
    }

    getNextValue (column = 0) {
        const row = this.getNextRow();
        return row ? Object.values(row)[column] : undefined;
    }

    [Symbol.iterator] () {
        const self: CSVDBQuery = this;
        function *iter () {
            if (self.#limit === 0) {
                return;
            }

            let rows: RowObject[] | Iterable<RowObject> = self.#rows;

            for (const join of self.#join) {
                const newRows = [];

                for (const row of rows) {
                    newRows.push(...join(row))
                }

                newRows.push(...join(null));

                rows = newRows;
            }

            // WHERE
            for (const predicate of self.#where) {
                rows = filter(rows, predicate);
            }

            // ORDER BY
            if (self.#sort) {
                // Need to materialise the rows in order to sort
                rows = [...rows].sort(self.#sort);
            }

            // GROUP BY
            let rowGroups: RowObject[][]|Iterable<RowObject> = rows;

            const haveWindowFunctions =
                self.#windowSpecs.size > 0 ||
                (
                    self.#selection &&
                    Object.values(self.#selection)
                        .some(s => (typeof s === "string" && s.endsWith(" OVER ()")) || Array.isArray(s))
                );

            if(self.#groupBy) {
                // groupRows() will materialise the rows
                rowGroups = groupRows(rows, self.#groupBy);
            }
            else if (self.#hasAggregates()) {
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

                const result = self.#mapSelectionToRow(sourceRow, self.#selection, i, rowGroup);

                if (self.#distinct) {
                    if (!isDistinct(distinctCache, result)) {
                        continue;
                    }

                    distinctCache.push(result);
                }

                yield result;

                // FETCH FIRST
                if (++i > self.#limit) {
                    return;
                }
            }
        }

        return iter();
    }

    #hasAggregates () {
        if (!this.#selection) return false;

        return Object.values(this.#selection).some(s => typeof s === "string" && isAggregate(s));
    }

    #mapSelectionToRow (sourceRow: RowObject, selection: SelectObject|null, index: number, groupRows: RowObject[]) {
        const out = {};

        if (!selection) {
            return sourceRow;
        }

        for (const [alias, col] of Object.entries(selection)) {
            let fn: SelectFunction;
            let field: string;
            let fnName: string;
            let args: string[];
            let windowName: string;
            let windowSpec: WindowSpec;

            const re = /^([A-Z_]+)\(([^)]*)\)(?:\s+OVER\s+([\w\d_]+|\(\)))?$/;

            // Easily dealt with
            if (col instanceof Function) {
                out[alias] = col(sourceRow, index, groupRows);
                continue;
            }

            // Now check for array syntax
            if (Array.isArray(col)) {
                const [ fnOrFnName, windowNameOrSpec ] = col;

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
                    this.#windowSpecs.get(windowName);

                if (!windowSpec) {
                    throw Error (`Bad Window: ${windowName}`);
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
                let value: number;

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
                    Object.assign(out,
                        (alias === "*") ?
                            sourceRow :
                            Object.fromEntries(
                                Object.entries(sourceRow)
                                    .map(([key,value]) => [
                                        `${alias}${key}`,
                                        value
                                    ])
                            )
                    );
                }
                else if (field) {
                    out[alias] = sourceRow[field];
                }
            }
        }

        return out;
    }
}

function applyWindow(rows: RowObject[], windowSpec: WindowSpec, sourceRow: RowObject) {
    if (windowSpec.partitionBy) {
        const pb = windowSpec.partitionBy;
        const fn = typeof pb === "string" ? (row: RowObject) => row[pb] : pb;
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

function getOrderBy (windowSpec: WindowSpec): (rowA: RowObject, rowB: RowObject) => number {
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

function orderByCheck (windowSpec: WindowSpec, fnName: string) {
    if (!windowSpec?.orderBy)
        throw Error(`ORDER BY required: ${fnName}`);
}

function zip (keys: string[], values: string[]) {
    const out = {};
    for (let i = 0; i < keys.length; i++) {
        const key = keys[i];
        out[key] = values[i];
    }
    return out;
}

function groupRows <T>(rows: Iterable<RowObject>, discriminator: (row: RowObject) => T): RowObject[][] {
    const resultSet: Map<T, RowObject[]> = new Map();

    for (const row of rows) {
        const value = discriminator(row);

        if (!resultSet.has(value)) {
            resultSet.set(value, []);
        }

        resultSet.get(value)?.push(row);
    }

    return [...resultSet.values()];
}

const isAggregate = (col: string | ((row: {}) => any)) => typeof col === "string" && /^[A-Z]+\([^)]*\)$/.test(col);

function parseCSVLine (line: string) {
    line = line.trim();
    const matches = line.matchAll(/([^",]*|"[^"]*")(,|$)/g);

    const m = [...matches];

    if (m[m.length-1][0].length === 0) {
        m.length--;
    }

    return m.map(match => match[1].trim().replace(/^"|"$/g, ""));
}

function *filter <T>(iterable: Iterable<T>, predicate: (item: T, index: number) => boolean): Iterable<T> {
    let i = 0;
    for (const item of iterable) {
        if (predicate(item, i++)) {
            yield item;
        }
    }
}

function isDistinct (rows: RowObject[], row: RowObject) {
    return rows.every(rowB => !isSame(row, rowB));
}

function isSame (rowA: RowObject, rowB: RowObject) {
    const keysA = Object.keys(rowA);
    const keysB = Object.keys(rowB);

    if (keysA.length !== keysB.length) return false;

    return keysA.every(key => rowA[key] === rowB[key]);
}

function *except (resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>) {
    const cache = [...resultsB];

    for (const result of resultsA) {
        if (!isDistinct(cache, result)) {
            continue;
        }

        cache.push(result);

        yield result;
    }
}

function *intersect (resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>) {
    const cache = [...resultsB];

    for (const result of resultsA) {
        if (isDistinct(cache, result)) {
            continue;
        }

        yield result;
    }
}

function *unionAll (resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>) {
    for (const result of resultsA) {
        yield result;
    }

    for (const result of resultsB) {
        yield result;
    }
}

const SUM: (value: any[]) => any = (values): any => values.reduce((total, v) => total + +v, 0);

const AGGREGATE_FUNCTIONS: { [name: string]: (value: any[]) => any; } = {
    SUM,
    AVG: values => SUM(values) / values.length,
    MAX: values => Math.max(...values),
    MIN: values => Math.min(...values),
    COUNT: values => values.length,
    LISTAGG: values => values.join(),
    ARRAY: values => values,
    JSON: values => JSON.stringify(values),
    ANY: values => values[0],
    RANDOM: values => values[Math.floor(Math.random()*values.length)],
};

const WINDOW_FUNCTIONS: { [name: string]: ((sourceRow: RowObject, rows: RowObject[], args: string[], windowSpec: WindowSpec) => any); } = {
    RANK: (sourceRow, rows, args, windowSpec) => {
        const orderBy = getOrderBy(windowSpec);

        const index = rows.indexOf(sourceRow);
        let i = index;
        for (; i >= 0; i--) {
            // @ts-ignore
            const order = orderBy(rows[i], sourceRow);
            if (order !== 0) break;
        }
        return i + 2;
    },
    DENSE_RANK: (sourceRow, rows, args, windowSpec) => {
        const orderBy = getOrderBy(windowSpec);

        const index = rows.indexOf(sourceRow);
        let count = 0;
        for (let i = 1; i <= index; i++) {
            // @ts-ignore
            const order = orderBy(rows[i-1], rows[i]);
            if (order === 0) count++;
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
            if (order !== 0) break;
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
            if (order !== 0) break;
        }
        return i / rows.length;
    },
    PERCENTILE_DIST: (sourceRow, rows, args, windowSpec) => {
        const result = findPercentile(rows, +args[0], windowSpec);
        if (result) {
            const [ index, key ] = result;

            return rows[index][key];
        }

        return null;
    },
    PERCENTILE_CONT: (sourceRow, rows, args, windowSpec) => {
        const result = findPercentile(rows, +args[0], windowSpec);
        if (result) {
            const [ index, key, x ] = result;

            const a = +rows[index-1][key];
            const b = +rows[index][key];

            return x * (b - a) + a;
        }
        return null;
    },
};

function findPercentile (rows: RowObject[], percentile: number, windowSpec: WindowSpec): [index: number, key: string, linear: number] | null {
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
            if (rows[i][k] !== rows[j][k]) break;
        }
        const p = j / rows.length;

        if (p >= percentile) {
            const x = (percentile - prevP)/(p - prevP);

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

const POSITION_FUNCTIONS: { [name: string]: ((sourceRow: RowObject, rows: RowObject[], args: string[], values: any[]) => any); } = {
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

const VARIANCE_SUM = (values: string[]) => {
    const n = values.length;
    const mean = SUM(values) / n;
    const sum = values.reduce((total, v) => total + Math.pow(+v - mean, 2), 0);
    return Math.sqrt(sum / n);
};

const STAT_FUNCTIONS: { [name: string]: (value: any[]) => any; } = {
    STDDEV_POP: values => Math.sqrt(VARIANCE_SUM(values) / values.length),
    STDDEV_SAMP: values => Math.sqrt(VARIANCE_SUM(values) / (values.length - 1)),
    VAR_POP: values => VARIANCE_SUM(values) / values.length,
    VAR_SAMP: values => VARIANCE_SUM(values) / (values.length - 1),
};
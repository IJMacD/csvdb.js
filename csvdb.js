/**
 * @typedef {import("./csvdb").RowObject} RowObject
 */

/**
 * @typedef {import("./csvdb").SelectObject} SelectObject
 */

/**
 * @typedef {import("./csvdb").WindowSpec} WindowSpec
 */

export class CSVDB
{
    /** @type {string[]} */
    #headers;
    /** @type {RowObject[]} */
    #rows;

    get rowCount () {
        return this.#rows.length;
    }

    get headers () {
        return this.#headers;
    }

    /**
     * @param {string} csv
     */
    constructor (csv) {
        const [ headerLine, ...restLines ] = csv.trim().split("\n");

        this.#headers = parseCSVLine(headerLine);

        const rows = restLines.map(parseCSVLine);

        this.#rows = /** @type {RowObject[]} */(rows.map(row => zip(this.#headers, row)));
    }

    [Symbol.iterator] () {
        return this.query()[Symbol.iterator]();
    }

    query () {
        return new CSVDBQuery(this.#rows);
    }

    /**
     * @param {Iterable<RowObject>} resultsA
     * @param {Iterable<RowObject>} resultsB
     */
    static except (resultsA, resultsB) {
        return new CSVDBQuery(except(resultsA, resultsB));
    }

    /**
     * @param {Iterable<RowObject>} resultsA
     * @param {Iterable<RowObject>} resultsB
     */
    static intersect (resultsA, resultsB) {
        return new CSVDBQuery(intersect(resultsA, resultsB));
    }

    /**
     * @param {Iterable<RowObject>} resultsA
     * @param {Iterable<RowObject>} resultsB
     */
    static union (resultsA, resultsB) {
        return CSVDB.unionAll(resultsA, resultsB).distinct();
    }

    /**
     * @param {Iterable<RowObject>} resultsA
     * @param {Iterable<RowObject>} resultsB
     */
    static unionAll (resultsA, resultsB) {
        return new CSVDBQuery(unionAll(resultsA, resultsB));
    }
}

class CSVDBQuery {
    /** @type {Iterable<RowObject>} */
    #rows;

    /** @type {((row: RowObject?) => RowObject[])[]} */
    #join = [];
    /** @type {((row: RowObject, index: number) => boolean)[]} */
    #where = [];
    /** @type {((row: RowObject) => any)?} */
    #groupBy = null;
    /** @type {SelectObject?} */
    #selection = null;
    /** @type {((rowA: RowObject, rowB: RowObject) => number)?} */
    #sort = null;
    /** @type {Map<string, WindowSpec>} */
    #windowSpecs = new Map();

    #limit = Number.POSITIVE_INFINITY;

    #distinct = false;

    /** @type {Generator<RowObject>?} */
    #internalIterator = null;

    /**
     * @param {Iterable<RowObject>} rows
     */
    constructor (rows) {
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
     * @param {(row: RowObject?) => RowObject[]} join
     */
    join (join) {
        this.#join.push(join);
        return this;
    }

    /**
     * Helper method to join two Queries.
     * `on` is a Callback which is given two rows (one from each side of the
     * join) and returns a boolean to indicate whether or not this match should
     * be included in the result set.
     * If `on` is not provided then the result is a cartesian join.
     * @param {CSVDBQuery} other
     * @param {(rowA: RowObject, rowB: RowObject) => boolean} [on]
     */
    joinOn (other, on = () => true) {
        let otherCache;
        this.#join.push(rowA => {
            // Materialise `other` just once
            if (typeof otherCache === "undefined") {
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
     * @param {(row: RowObject, index: number) => boolean} predicate
     */
    where (predicate) {
        this.#where.push(predicate);
        return this;
    }

    /**
     * @param {((row: RowObject) => any)|string} discriminator
     */
    groupBy (discriminator) {
        if (typeof discriminator === "string") {
            const d = discriminator;
            discriminator = row => row[d];
        }
        this.#groupBy = discriminator;
        return this;
    }

    /**
     * @param {{ [alias: string]: string|((row: RowObject) => any) }|string[]} selection
     */
    select (selection) {
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
     * @param {(rowA: RowObject, rowB: RowObject) => number} comparator
     */
    orderBy (comparator) {
        this.#sort = comparator;
        return this;
    }

    /**
     * @param {number} limit
     */
    fetchFirst (limit) {
        this.#limit = limit;
        return this;
    }

    /**
     * @param {string} name
     * @param {WindowSpec} spec
     */
    window (name, spec) {
        this.#windowSpecs.set(name, spec);
        return this;
    }

    distinct (distinct = true) {
        this.#distinct = distinct;
        return this;
    }

    /**
     * Materialise result rows
     * @returns
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

    *[Symbol.iterator] () {
        if (this.#limit === 0) {
            return;
        }

        /** @type {RowObject[]|Iterable<RowObject>} */
        let rows = this.#rows;

        for (const join of this.#join) {
            const newRows = [];

            for (const row of rows) {
                newRows.push(...join(row))
            }

            newRows.push(...join(null));

            rows = newRows;
        }

        // WHERE
        for (const predicate of this.#where) {
            rows = filter(rows, predicate);
        }

        // ORDER BY
        if (this.#sort) {
            // Need to materialise the rows in order to sort
            rows = [...rows].sort(this.#sort);
        }

        // GROUP BY
        /** @type {Iterable<RowObject>|RowObject[][]} */
        let rowIterator = rows;

        const haveWindowFunctions =
            this.#windowSpecs.size > 0 ||
            (
                this.#selection &&
                Object.values(this.#selection)
                    .some(s => (typeof s === "string" && s.endsWith(" OVER ()")) || Array.isArray(s))
            );

        if(this.#groupBy) {
            // groupRows() will materialise the rows
            rowIterator = groupRows(rows, this.#groupBy);
        }
        else if (this.#hasAggregates()) {
            // We're going to have to materialise the rows anyway so do it now
            rowIterator = [[...rows]];
        }
        else if (haveWindowFunctions) {
            // Unfortunately we need to materialise the rows once to pass as the
            // 4th argument to mapSelectionToRow()
            rowIterator = [...rows];
        }

        const distinctCache = [];

        // SELECT
        let i = 1;
        for (const row of rowIterator) {
            /** @type {RowObject[]} */
            const rowGroup = Array.isArray(row) ? row :
                (haveWindowFunctions ? /** @type {RowObject[]} */(rowIterator) :  [row]);
            const sourceRow = Array.isArray(row) ? rowGroup[0] : row;

            const result = this.#mapSelectionToRow(sourceRow, this.#selection, i, rowGroup);

            if (this.#distinct) {
                if (!isDistinct(distinctCache, result)) {
                    continue;
                }

                distinctCache.push(result);
            }

            yield result;

            // FETCH FIRST
            if (++i > this.#limit) {
                return;
            }
        }
    }

    #hasAggregates () {
        if (!this.#selection) return false;

        return Object.values(this.#selection).some(s => typeof s === "string" && isAggregate(s));
    }

    /**
     * @param {RowObject} sourceRow
     * @param {SelectObject?} selection
     * @param {number} index
     * @param {RowObject[]} groupRows
     */
    #mapSelectionToRow (sourceRow, selection, index, groupRows) {
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
                const [ fnOrFnName, windowNameOrSpec ] = col;

                if (typeof fnOrFnName === "string") {
                    const fnString = fnOrFnName;

                    const aggregateMatch = re.exec(fnString);
                    if (aggregateMatch) {
                        fnName = aggregateMatch[1];
                        args = aggregateMatch[2].split(",");

                        if (aggregateMatch[3]) {
                            throw Error("Cannot specify OVER clause in string");
                        }
                    }
                    else {
                        throw Error(`Expected function but got: ${fnString}`);
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
                    throw Error (`Window "${windowName}" not specified`);
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
                    throw Error(`Function '${fnName} not recognised`);
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

/**
 * @param {RowObject[]} rows
 * @param {WindowSpec} windowSpec
 * @param {RowObject} sourceRow
 */
function applyWindow(rows, windowSpec, sourceRow) {
    if (windowSpec.partitionBy) {
        const pb = windowSpec.partitionBy;
        const fn = typeof pb === "string" ? (/** @type {RowObject} */ row) => row[pb] : pb;
        const sympatheticValue = fn(sourceRow);
        rows = rows.filter(row => fn(row) === sympatheticValue);
    }

    if (windowSpec.orderBy) {
        const orderBy = getOrderBy(windowSpec);

        rows = [...rows].sort(orderBy);

        let framingStart = Number.NEGATIVE_INFINITY;
        let framingEnd = 0;

        if (windowSpec.framing) {
            const unit = windowSpec.framing[0];
            if (unit !== "ROWS") {
                throw Error(`Not Implemented: Window unit ${unit}`);
            }

            if (typeof windowSpec.framing[1] === "number") {
                framingStart = windowSpec.framing[1];
            }
            else if (windowSpec.framing[1] === "UNBOUNDED PRECEDING") {
                framingStart = Number.NEGATIVE_INFINITY;
            }
            else if (windowSpec.framing[1] === "CURRENT ROW") {
                framingStart = 0;
            }

            if (typeof windowSpec.framing[2] === "number") {
                framingEnd = windowSpec.framing[2];
            }
            else if (windowSpec.framing[2] === "UNBOUNDED FOLLOWING") {
                framingEnd = Number.POSITIVE_INFINITY;
            }
            else if (windowSpec.framing[2] === "CURRENT ROW") {
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

/**
 * @param {WindowSpec} windowSpec
 * @returns {(rowA: RowObject, rowB: RowObject) => number}
 */
function getOrderBy (windowSpec) {
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

/**
 * @param {WindowSpec} windowSpec
 * @param {string} fnName
 */
function orderByCheck (windowSpec, fnName) {
    if (!windowSpec?.orderBy)
        throw Error(`ORDER BY clause required in windows spec for ${fnName}`);
}

/**
 * @param {string[]} keys
 * @param {string[]} values
 */
function zip (keys, values) {
    const out = {};
    for (let i = 0; i < keys.length; i++) {
        const key = keys[i];
        out[key] = values[i];
    }
    return out;
}

/**
 * @param {Iterable<RowObject>} rows
 * @param {(row: RowObject) => T} discriminator
 * @template T
 * @returns {RowObject[][]}
 */
function groupRows (rows, discriminator) {
    /** @type {Map<T, RowObject[]>} */
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

const isAggregate = (/** @type {string|((row: {}) => any)} */ col) => typeof col === "string" && /^[A-Z]+\([^)]*\)$/.test(col);

/**
 * @param {string} line
 */
function parseCSVLine (line) {
    line = line.trim();
    const matches = line.matchAll(/([^",]*|"[^"]*")(,|$)/g);

    const m = [...matches];

    if (m[m.length-1][0].length === 0) {
        m.length--;
    }

    return m.map(match => match[1].trim().replace(/^"|"$/g, ""));
}

/**
 * @param {Iterable<T>} iterable
 * @param {(item: T, index: number) => boolean} predicate
 * @template T
 * @returns {Iterable<T>}
 */
function *filter (iterable, predicate) {
    let i = 0;
    for (const item of iterable) {
        if (predicate(item, i++)) {
            yield item;
        }
    }
}

/**
 * @param {RowObject[]} rows
 * @param {RowObject} row
 */
function isDistinct (rows, row) {
    return rows.every(rowB => !isSame(row, rowB));
}

/**
 * @param {RowObject} rowA
 * @param {RowObject} rowB
 */
function isSame (rowA, rowB) {
    const keysA = Object.keys(rowA);
    const keysB = Object.keys(rowB);

    if (keysA.length !== keysB.length) return false;

    return keysA.every(key => rowA[key] === rowB[key]);
}

/**
 * @param {Iterable<RowObject>} resultsA
 * @param {Iterable<RowObject>} resultsB
 */
function *except (resultsA, resultsB) {
    const cache = [...resultsB];

    for (const result of resultsA) {
        if (!isDistinct(cache, result)) {
            continue;
        }

        cache.push(result);

        yield result;
    }
}

/**
 * @param {Iterable<RowObject>} resultsA
 * @param {Iterable<RowObject>} resultsB
 */
function *intersect (resultsA, resultsB) {
    const cache = [...resultsB];

    for (const result of resultsA) {
        if (isDistinct(cache, result)) {
            continue;
        }

        yield result;
    }
}

/**
 * @param {Iterable<RowObject>} resultsA
 * @param {Iterable<RowObject>} resultsB
 */
function *unionAll (resultsA, resultsB) {
    for (const result of resultsA) {
        yield result;
    }

    for (const result of resultsB) {
        yield result;
    }
}

/** @type {(value: any[]) => any} */
const SUM = values => values.reduce((total, v) => total + +v, 0);

/** @type {{ [name: string]: (value: any[]) => any }} */
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
    RANDOM: values => values[Math.floor(Math.random()*values.length)],
};

/**
 * @type {{ [name: string]: ((sourceRow: RowObject, rows: RowObject[], args: string[], windowSpec: WindowSpec) => any) }}
 */
const WINDOW_FUNCTIONS = {
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
        if (typeof windowSpec?.orderBy !== "string") {
            throw Error(`PERCENTILE_DIST requires orderBy to be specified as a string`);
        }

        let k = windowSpec.orderBy;
        if (k[0] === "+") {
            k = k.substring(1);
        }

        const percentile = +args[0];

        for (let i = 0; i < rows.length; i++) {
            for (let j = i + 1; j < rows.length; j++) {
                if (rows[i][k] !== rows[j][k]) break;
            }
            const p = i / rows.length;

            if (p >= percentile) {
                return rows[i][k];
            }
        }

        return null;
    },
    PERCENTILE_CONT: (sourceRow, rows, args, windowSpec) => {
        if (typeof windowSpec?.orderBy !== "string") {
            throw Error(`PERCENTILE_CONT requires orderBy to be specified as a string`);
        }

        let k = windowSpec.orderBy;
        if (k[0] === "+") {
            k = k.substring(1);
        }

        const percentile = +args[0];

        let prevP = 0;

        for (let i = 0; i < rows.length; i++) {
            let j = i + 1;
            for (; j < rows.length; j++) {
                if (rows[i][k] !== rows[j][k]) break;
            }
            const p = j / rows.length;

            if (p >= percentile) {
                const x = (percentile - prevP)/(p - prevP);
                const a = +rows[i-1][k];
                const b = +rows[i][k];
                return x * (b - a) + a;
            }

            prevP = p;
        }

        return null;
    },
}

/**
 * @type {{ [name: string]: ((sourceRow: RowObject, rows: RowObject[], args: string[], values: any[]) => any) }}
 */
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

const VARIANCE_SUM = (/** @type {any[]} */ values) => {
    const n = values.length;
    const mean = SUM(values) / n;
    const sum = values.reduce((total, v) => total + Math.pow(+v - mean, 2), 0);
    return Math.sqrt(sum / n);
};

/** @type {{ [name: string]: (value: any[]) => any }} */
const STAT_FUNCTIONS = {
    STDDEV_POP: values => Math.sqrt(VARIANCE_SUM(values) / values.length),
    STDDEV_SAMP: values => Math.sqrt(VARIANCE_SUM(values) / (values.length - 1)),
    VAR_POP: values => VARIANCE_SUM(values) / values.length,
    VAR_SAMP: values => VARIANCE_SUM(values) / (values.length - 1),
};
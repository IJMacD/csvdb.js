/** @typedef {{}} RowObject */

export class CSVDB
{
    /** @type {string[]} */
    #headers;
    /** @type {RowObject[]} */
    #rows;

    get rowCount () {
        return this.#rows.length;
    }

    /**
     * @param {string} csv
     */
    constructor (csv) {
        const [ headerLine, ...restLines ] = csv.trim().split("\n");

        this.#headers = headerLine.trim().split(",");

        const rows = restLines.map(parseCSVLine);

        this.#rows = /** @type {RowObject[]} */(rows.map(row => zip(this.#headers, row)));
    }

    [Symbol.iterator] () {
        return this.query()[Symbol.iterator]();
    }

    query () {
        return new CSVDBQuery(this.#rows);
    }
}

class CSVDBQuery {
    /** @type {Iterable<RowObject>} */
    #rows;

    /** @type {((row: RowObject) => RowObject[])[]} */
    #join = [];
    /** @type {((row: RowObject, index: number) => boolean)[]} */
    #where = [];
    /** @type {((row: RowObject) => any)?} */
    #groupBy = null;
    /** @type {{ [alias: string]: string|((row: RowObject) => any) }?} */
    #selection = null;
    /** @type {((rowA: RowObject, rowB: RowObject) => number)?} */
    #sort = null;

    #limit = Number.POSITIVE_INFINITY;

    #distinct = false;

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
     * @param {(row: RowObject) => RowObject[]} join
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

            for (const rowB of otherCache) {
                if (on(rowA, rowB)) {
                    out.push({ ...rowA, ...rowB });
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
        let iterator = rows;

        if(this.#groupBy) {
            // groupRows() will materialise the rows
            iterator = groupRows(rows, this.#groupBy);
        }
        else if (this.#hasAggregates()) {
            // We're going to have to materialise the rows anyway so do it now
            iterator = [[...rows]];
        }

        const distinctCache = [];

        // SELECT
        let i = 1;
        for (const row of iterator) {
            /** @type {RowObject[]} */
            const rowGroup = Array.isArray(row) ? row : [row];

            const result = this.#mapSelectionToRow(rowGroup, this.#selection, i);

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

        return Object.values(this.#selection).some(isAggregate);
    }

    /**
     * @param {RowObject[]} sourceRows
     * @param {{ [alias: string]: string|((row: RowObject, index?: number) => any) }?} selection
     * @param {number} index
     */
    #mapSelectionToRow (sourceRows, selection, index) {
        const out = {};

        const firstRow = sourceRows[0];

        if (!selection) {
            return firstRow;
        }

        for (const [alias, col] of Object.entries(selection)) {

            if (col instanceof Function) {
                out[alias] = col(firstRow, index);
            }
            else {
                const aggregateMatch = /^([A-Z]{3,5})\((.*)\)$/.exec(col);
                if (aggregateMatch) {
                    const fnName = aggregateMatch[1];
                    const colName = aggregateMatch[2];

                    const values = sourceRows.map(row => row[colName]);

                    let value;

                    if (fnName === "SUM") {
                        value = values.reduce((total, v) => total + +v, 0);
                    }
                    else if (fnName === "AVG") {
                        value = values.reduce((total, v) => total + +v, 0) / values.length;
                    }
                    else if (fnName === "MAX") {
                        value = Math.max(...values);
                    }
                    else if (fnName === "MIN") {
                        value = Math.min(...values);
                    }
                    else if (fnName === "COUNT") {
                        value = values.length;
                    }
                    else if (fnName === "AGG") {
                        value = values.join();
                    }
                    else if (fnName === "ARRAY") {
                        value = values;
                    }
                    else if (fnName === "JSON") {
                        value = JSON.stringify(values);
                    }
                    else if (fnName === "ANY") {
                        value = values[0];
                    }

                    out[alias] = value;
                }
                else if (firstRow) {
                    if (col === "*") {
                        Object.assign(out, firstRow);
                    }
                    else {
                        out[alias] = firstRow[col];
                    }
                }
            }
        }

        return out;
    }
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

const isAggregate = (/** @type {string|((row: {}) => any)} */ col) => typeof col === "string" && /^[A-Z]{3,5}\(.*\)/.test(col);

/**
 * @param {string} line
 */
function parseCSVLine (line) {
    // line => line.trim().split(",").map(cell => cell.replace(/^"|"$/g,""))
    line = line.trim();
    const matches = line.matchAll(/([^",]*|"[^"]*")(,|$)/g);

    const m = [...matches];

    if (m[m.length-1][0].length === 0) {
        m.length--;
    }

    return m.map(match => match[1].replace(/^"|"$/g, ""));
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
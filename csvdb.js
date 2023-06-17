/** @typedef {{}} RowObject */

export class CSVDB
{
    /** @type {string[]} */
    #headers;
    /** @type {RowObject[]} */
    #rows;

    /**
     * @param {string} csv
     */
    constructor (csv) {
        const [ headerLine, ...restLines ] = csv.trim().split("\n");

        this.#headers = headerLine.trim().split(",");

        const rows = restLines.map(parseCSVLine);

        this.#rows = /** @type {RowObject[]} */(rows.map(row => zip(this.#headers, row)));
    }

    query () {
        return new CSVDBQuery(this.#rows);
    }
}

class CSVDBQuery {
    #rows;

    /** @type {((row: RowObject) => boolean)[]} */
    #where = [];
    /** @type {((row: RowObject) => any)?} */
    #groupBy = null;
    /** @type {{ [alias: string]: string|((row: RowObject) => any) }?} */
    #selection = null;
    /** @type {((rowA: RowObject, rowB: RowObject) => number)?} */
    #sort = null;

    #limit = NaN;

    /**
     * @param {RowObject[]} rows
     */
    constructor (rows) {
        this.#rows = rows;
    }

    /**
     * Materialise rows and create a new query object from them
     */
    query () {
        return new CSVDBQuery(this.toArray());
    }

    /**
     * Multiple calls will be AND'd together
     * @param {(row: RowObject) => boolean} predicate
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
     * Materialise result rows
     * @returns
     */
    toArray () {
        return [...this];
    }

    *[Symbol.iterator] () {
        let rows = this.#rows;

        // WHERE
        for (const predicate of this.#where) {
            rows = rows.filter(predicate);
        }

        // ORDER BY
        if (this.#sort) {
            rows.sort(this.#sort);
        }

        // GROUP BY
        let groupedRows = null;

        if(this.#groupBy) {
            groupedRows = groupRows(rows, this.#groupBy);
        }
        else if (this.#hasAggregates()) {
            groupedRows = [rows];
        }
        else {
            groupedRows = rows.map(row => [row]);
        }

        // FETCH FIRST
        if (!isNaN(this.#limit)) {
            groupedRows.length = Math.min(groupedRows.length, this.#limit);
        }

        // SELECT
        for (const rows of groupedRows) {
            yield mapSelectionToRow(rows, this.#selection);
        }
    }

    #hasAggregates () {
        if (!this.#selection) return false;

        return Object.values(this.#selection).some(isAggregate);
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
 * @param {{}[]} rows
 * @param {(row: object) => T} discriminator
 * @template T
 */
function groupRows (rows, discriminator) {
    /** @type {Map<T, object[]>} */
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

const isAggregate = (/** @type {string|((row: {}) => any)} */ col) => typeof col === "string" && /^[A-Z]{3,5}\(.*\)$/.test(col);

/**
 * @param {any[]} sourceRows
 * @param {{ [alias: string]: string|((row: {}) => any) }?} selection
 */
function mapSelectionToRow (sourceRows, selection) {
    const out = {};

    if (!selection) {
        return sourceRows[0];
    }

    for (const [ alias, col] of Object.entries(selection)) {

        if (col instanceof Function) {
            out[alias] = col(sourceRows[0]);
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

                out[alias] = value;
            }
            else if (sourceRows.length > 0) {
                out[alias] = sourceRows[0][col];
            }
        }
    }

    return out;
}

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
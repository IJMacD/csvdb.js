export class CSVDB
{
    /** @type {string[]} */
    #headers;
    /** @type {{}[]} */
    #rows;

    /**
     * @param {string} csv
     */
    constructor (csv) {
        const [ headerLine, ...restLines ] = csv.trim().split("\n");

        this.#headers = headerLine.split(",");

        const rows = restLines.map(line => line.split(",").map(cell => cell.replace(/^"|"$/g,"")));

        this.#rows = rows.map(row => zip(this.#headers, row));
    }

    query () {
        return new CSVDBQuery(this.#rows);
    }
}

/** @typedef {{ [field: string]: string }} RowObject */

class CSVDBQuery {
    #rows;

    /** @type {((row: RowObject) => boolean)[]} */
    #where = [];
    /** @type {((row: RowObject) => any)?} */
    #groupBy = null;
    /** @type {{ [alias: string]: string|((row: RowObject) => any) }?} */
    #selection = null;

    constructor (rows) {
        this.#rows = rows;
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
     * @param {(row: RowObject) => T} discriminator
     * @template T
     */
    groupBy (discriminator) {
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

    toArray () {
        let rows = this.#rows;

        for (const predicate of this.#where) {
            rows = rows.filter(predicate);
        }

        let groupedRows = null;

        if(this.#groupBy) {
            groupedRows = groupRows(rows, this.#groupBy);
        }
        else if (this.#hasAggregates()) {
            groupedRows = [rows];
        }

        if (!this.#selection) {
            if (groupedRows) {
                return groupedRows.map(rows => rows[0]);
            }

            return rows;
        }

        if (groupedRows) {
            return groupedRows.map(rows => mapSelectionToRow(rows, this.#selection));
        }

        return rows.map(row => mapSelectionToRow([row], this.#selection));
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

const isAggregate = (/** @type {string|((row: {}) => any)} */ col) => typeof col === "string" && /^[A-Z]{3}\(.*\)$/.test(col);

/**
 * @param {any[]} sourceRows
 * @param {{ [alias: string]: string|((row: {}) => any) }} selection
 */
function mapSelectionToRow (sourceRows, selection) {
    const out = {};

    for (const [ alias, col] of Object.entries(selection)) {
        if (col instanceof Function) {
            out[alias] = col(sourceRows[0]);
        }
        else if (isAggregate(col)) {
            const fnName = col.substring(0, 3);
            const colName = col.substring(4, col.length - 1);
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

            out[alias] = value;
        }
        else if (sourceRows.length > 0) {
            out[alias] = sourceRows[0][col];
        }
    }

    return out;
}
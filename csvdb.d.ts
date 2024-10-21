import { RowObject, WindowSpec } from "./types";
export declare class CSVDB {
    #private;
    get rowCount(): number;
    get headers(): string[];
    constructor(csv: string);
    [Symbol.iterator](): Generator<{}, void, unknown>;
    query(): CSVDBQuery;
    static except(resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>): CSVDBQuery;
    static intersect(resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>): CSVDBQuery;
    static union(resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>): CSVDBQuery;
    static unionAll(resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>): CSVDBQuery;
}
declare class CSVDBQuery {
    #private;
    constructor(rows: Iterable<RowObject>);
    /**
     * Materialise rows and create a new query object from them
     */
    query(): CSVDBQuery;
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
    join(join: (row: RowObject | null) => RowObject[]): this;
    /**
     * Helper method to join two Queries.
     * `on` is a Callback which is given two rows (one from each side of the
     * join) and returns a boolean to indicate whether or not this match should
     * be included in the result set.
     * If `on` is not provided then the result is a cartesian join.
     */
    joinOn(other: CSVDBQuery, on?: (rowA: RowObject, rowB: RowObject) => boolean): this;
    /**
     * Multiple calls will be AND'd together
     */
    where(predicate: (row: RowObject, index: number) => boolean): this;
    groupBy(discriminator: ((row: RowObject) => any) | string): this;
    select(selection: {
        [alias: string]: string | ((row: RowObject) => any);
    } | string[]): this;
    /**
     * Sorts based on input rows.
     * To sort based on output use `.toArray().sort()`
     */
    orderBy(comparator: (rowA: RowObject, rowB: RowObject) => number): this;
    fetchFirst(limit: number): this;
    window(name: string, spec: WindowSpec): this;
    distinct(distinct?: boolean): this;
    /**
     * Materialise result rows
     */
    toArray(): {}[];
    getNextRow(): any;
    getNextValue(column?: number): unknown;
    [Symbol.iterator](): Generator<{}, void, unknown>;
}
export {};

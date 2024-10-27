import { ColumnSpec, RowObject, SelectFunction, SelectObject, WindowSpec } from "./types";
export { RowObject, SelectFunction, SelectObject, WindowSpec, ColumnSpec };
export declare class CSVDB {
    #private;
    get rowCount(): number;
    get headers(): string[];
    constructor(csv: string);
    [Symbol.iterator](): Iterator<RowObject, any, any>;
    query(): CSVDBQuery;
    static except(resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>): CSVDBQuery;
    static intersect(resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>): CSVDBQuery;
    static union(resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>): CSVDBQuery;
    static unionAll(resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>): CSVDBQuery;
}
export declare class CSVDBQuery {
    #private;
    constructor(rows: Iterable<RowObject>);
    query(): CSVDBQuery;
    join(joinSpec: (row: RowObject | null) => RowObject[]): this;
    joinOn(other: Iterable<CSVDB | CSVDBQuery | any>, on?: (rowA: RowObject, rowB: RowObject) => boolean): this;
    where(predicate: (row: RowObject, index: number) => boolean): this;
    groupBy(discriminator: ((row: RowObject) => any) | string): this;
    select(selection: {
        [alias: string]: ColumnSpec | [ColumnSpec, string | WindowSpec];
    } | string[]): this;
    orderBy(comparator: ((rowA: RowObject, rowB: RowObject) => number) | string): this;
    offset(rows: number): this;
    fetchFirst(rows: number): this;
    window(name: string, spec: WindowSpec): this;
    distinct(distinct?: boolean): this;
    toArray(): RowObject[];
    getNextRow(): RowObject;
    getNextValue(column?: string | number): any;
    [Symbol.iterator](): Iterator<RowObject>;
}

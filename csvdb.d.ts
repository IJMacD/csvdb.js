export class CSVDB {
    constructor (csvText: string);
    get rowCount(): number;
    get headers(): string[];
    [Symbol.iterator](): Generator<RowObject>;
    query(): CSVDBQuery;
    static except (resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>): Iterable<RowObject>;
    static intersect (resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>): Iterable<RowObject>;
    static union (resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>): Iterable<RowObject>;
    static unionAll (resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>): Iterable<RowObject>;
}

declare class CSVDBQuery {
    constructor (rows: Iterable<RowObject>);
    query (): CSVDBQuery;
    join (join: (row: RowObject?) => RowObject[]): CSVDBQuery;
    joinOn (other: Iterable<RowObject>, on: ((rowA: RowObject, rowB: RowObject) => boolean)): CSVDBQuery;
    where (predicate: (row: RowObject) => boolean): CSVDBQuery;
    groupBy (discriminator: (row: RowObject) => any): CSVDBQuery;
    select (selection: SelectObject|string[]): CSVDBQuery;
    orderBy (comparator: (rowA: RowObject, rowB: RowObject) => number): CSVDBQuery;
    fetchFirst (limit: number): CSVDBQuery;
    window (name: string, spec: WindowSpec): CSVDBQuery;
    distinct (distinct: boolean): CSVDBQuery;
    toArray (): RowObject[];
    [Symbol.iterator] (): Generator<RowObject>;
}

interface RowObject extends Object {
    [field: string]: any;
}

interface SelectObject {
    [alias: string]: string|((row: RowObject) => any);
}

interface WindowSpec {
    partitionBy?: (row: RowObject) => any;
    orderBy?: (rowA: RowObject, rowB: RowObject) => number;
    framing?: [unit:"ROWS"|"RANGE",start:string|number,end:string|number];
}
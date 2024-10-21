export interface RowObject {
    [field: string]: string;
}

export type SelectFunction = (row: RowObject, i: number, rows: RowObject[]) => any;

export interface SelectObject {
    [alias: string]: string|SelectFunction|[string|SelectFunction,string|WindowSpec];
}

export interface WindowSpec {
    partitionBy?: string|((row: RowObject) => any);
    orderBy?: string|((rowA: RowObject, rowB: RowObject) => number);
    framing?: [unit:"ROWS"|"RANGE",start:string|number,end:string|number];
}
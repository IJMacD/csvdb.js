export interface RowObject {
  [field: string]: any;
}

export interface StringRowObject {
  [field: string]: string;
}

export type SelectFunction = (
  row: RowObject,
  i: number,
  rows: RowObject[]
) => any;

export interface SelectObject {
  [alias: string]:
    | string
    | SelectFunction
    | [string | SelectFunction, string | WindowSpec];
}

export interface WindowSpec {
  partitionBy?: string | ((row: RowObject) => any);
  orderBy?: string | ((rowA: RowObject, rowB: RowObject) => number);
  framing?: [
    unit: "ROWS" | "RANGE",
    start: "UNBOUNDED PRECEDING" | "CURRENT ROW" | number,
    end: "UNBOUNDED FOLLOWING" | "CURRENT ROW" | number
  ];
}

export type ColumnSpec =
  | string
  | ((row: RowObject, i: number, rowGroup: RowObject[]) => any);

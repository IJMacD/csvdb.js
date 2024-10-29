import {
  ColumnSpec,
  RowObject,
  SelectFunction,
  SelectObject,
  StringRowObject,
  WindowSpec,
} from "./types";
export { RowObject, SelectFunction, SelectObject, WindowSpec, ColumnSpec };

export class CSVDB {
  #headers: string[];
  #rawLines: string[];
  #parsedRows: RowObject[] = [];

  /**
   * Returns the number of rows in the csv file
   * @example
   * ```js
   * console.log(db.rowCount)
   * ```
   * Output:
   * ```
   * 2
   * ```
   */
  get rowCount() {
    return this.#rawLines.length;
  }

  /**
   * Returns an array of the csv column headers
   * @example
   * ```js
   * console.log(db.headers)
   * ```
   * Output:
   * ```
   * ["a","b","c"]
   * ```
   */
  get headers() {
    return this.#headers;
  }

  /**
   * Create a CSVDB by providing a CSV string to the constructor.
   * @example Create a database object
   * ```js
   * const db = new CSVDB("a,b,c\n,1,2,3\n4,5,6")
   * ```
   * @param csv A raw string containing CSV data. The headers should be the
   * first row. Rows should be separated by a single `\n` character.
   */
  constructor(csv: string) {
    const [headerLine, ...restLines] = csv.trim().split("\n");

    this.#headers = parseCSVLine(headerLine);

    this.#rawLines = restLines;
  }

  /**
   * Iterating on the db returns all rows as RowObjects
   * @example
   * ```js
   * for (const row of db) {
   *  console.log(row)
   * }
   * ```
   * Output:
   * ```
   * {a: "1", b: "2", c: "3"}
   * {a: "4", b: "5", c: "6"}
   * ```
   */
  [Symbol.iterator](): Iterator<RowObject> {
    return this.#iter();
  }

  *#iter(): Generator<RowObject> {
    // It's important to keep object identity over multiple calls to #iter()

    for (const row of this.#parsedRows) {
      yield row;
    }

    for (let i = this.#parsedRows.length; i < this.rowCount; i++) {
      const line = this.#rawLines[i];
      const parsed = parseCSVLine(line);
      const row = zip(this.#headers, parsed);
      this.#parsedRows.push(row);
      yield row;
    }
  }

  /**
   * The db can of course be queried via the `query()` method. Calling this
   * method returns a `CSVDBQuery` object which has the actual methods used to
   * specify the desired query.
   *
   * @example
   * ```js
   * const query = db.query()
   *
   * query.where(r => r.a === "1")
   *
   * console.log(query.getNextRow())
   * ```
   * Output:
   * ```
   * {a: "1", b: "2", c: "3"}
   * ```
   */
  query() {
    return new CSVDBQuery(this);
  }

  /**
   * Takes two iterables of RowObjects and produces the mathematical set of the
   * first iterable subtract the second.
   * @param resultsA
   * @param resultsB
   * @returns A new CSVDBQuery object
   * @example
   * ```
   * const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
   * const db2 = new CSVDB("a,b,c\n1,2,3\n1,3,5");
   * const results = CSVDB.except(db, db2);
   * console.log(results.toArray());
   * ```
   * Output
   * ```
   * [ { a: '4', b: '5', c: '6' } ]
   * ```
   */
  static except(resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>) {
    return new CSVDBQuery(except(resultsA, resultsB));
  }

  /**
   * Takes two iterables of RowObjects and produces the mathematical set of the
   * intersection of both.
   * @param resultsA
   * @param resultsB
   * @returns A new CSVDBQuery object
   * @example
   * ```
   * const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
   * const db2 = new CSVDB("a,b,c\n1,2,3\n1,3,5");
   * const results = CSVDB.intersect(db, db2);
   * console.log(results.toArray());
   * ```
   * Output
   * ```
   * [ { a: '1', b: '2', c: '3' } ]
   * ```
   */
  static intersect(
    resultsA: Iterable<RowObject>,
    resultsB: Iterable<RowObject>
  ) {
    return new CSVDBQuery(intersect(resultsA, resultsB));
  }

  /**
   * Takes two iterables of RowObjects and produces the mathematical set of the
   * union of both.
   * @param resultsA
   * @param resultsB
   * @returns A new CSVDBQuery object
   * @example
   * ```
   * const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
   * const db2 = new CSVDB("a,b,c\n1,2,3\n1,3,5");
   * const results = CSVDB.union(db, db2);
   * console.log(results.toArray());
   * ```
   * Output
   * ```
   * [
   *   { a: '1', b: '2', c: '3' },
   *   { a: '4', b: '5', c: '6' },
   *   { a: '1', b: '3', c: '5' }
   * ]
   * ```
   */
  static union(resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>) {
    return CSVDB.unionAll(resultsA, resultsB).distinct();
  }

  /**
   * Takes two iterables of RowObjects and appends the second to the first
   * un-mathematically.
   * @param resultsA
   * @param resultsB
   * @returns A new CSVDBQuery object
   * @example
   * ```
   * const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
   * const db2 = new CSVDB("a,b,c\n1,2,3\n1,3,5");
   * const results = CSVDB.unionAll(db, db2);
   * console.log(results.toArray());
   * ```
   * Output
   * ```
   * [
   *   { a: '1', b: '2', c: '3' },
   *   { a: '4', b: '5', c: '6' },
   *   { a: '1', b: '2', c: '3' },
   *   { a: '1', b: '3', c: '5' }
   * ]
   * ```
   */
  static unionAll(
    resultsA: Iterable<RowObject>,
    resultsB: Iterable<RowObject>
  ) {
    return new CSVDBQuery(unionAll(resultsA, resultsB));
  }
}

export class CSVDBQuery {
  #rows: Iterable<RowObject>;

  #join: ((row: RowObject | null) => RowObject[] | null | undefined)[] = [];
  #where: ((row: RowObject, index: number) => boolean)[] = [];
  #groupBy: ((row: RowObject) => any) | null = null;
  #selection: SelectObject | null = null;
  #sort: ((rowA: RowObject, rowB: RowObject) => number) | null = null;
  #windowSpecs: Map<string, WindowSpec> = new Map();

  #offset = 0;
  #limit = Infinity;

  #distinct = false;

  #internalIterator: Iterator<RowObject> | undefined;

  constructor(rows: Iterable<RowObject>) {
    this.#rows = rows;
  }

  /**
   * Create a new query object from the current result set.
   *
   * Allows combining queries in complex logic.
   *
   * @example Select the largest 3 values presented in ascending order
   * ```
   * const db = new CSVDB("a\n5\n2\n9\n1\n8\n4\n6\n3\n7");
   *
   * const query = db.query()
   *  .orderBy("-a")      // Results in descending order
   *  .fetchFirst(3)      // Take the top 3
   *  .query()            // Create a new query from the current result set
   *  .orderBy("+a");     // Order results in ascending order
   *
   * console.log(query.toArray());
   * ```
   * Output
   * ```
   * [ { a: '7' }, { a: '8' }, { a: '9' } ]
   * ```
   * In this case the results could be sorted using the native
   * Array.prototype.sort method after materializing into an array, but it
   * demonstrates a capability which allows much more complex behaviour such as
   * joins and so on.
   * @returns Returns a new query object to allow creating complex queries.
   */
  query() {
    return new CSVDBQuery(this);
  }

  /**
   * Used to implement joins with other CSVDB databases or any arbitrary
   * mechanism you choose.
   *
   * @param joinSpec This callback is called once per row of the current set.
   * The callback should return zero or more rows as appropriate to implement
   * the join.
   *
   * The `RowObject`s returned from the callback conceptually should include all
   * fields from the existing row plus the fields from the joined row, but
   * there's no strict requirement to do so.
   *
   * Returning an empty array is how you indicate that there are no rows in the
   * output set for the given input row. However, for convenience returning
   * `undefined` or `null` will be interpreted the same way.
   *
   * Multiple joins can be added with multiple calls to the `join()` method
   * and will be executed in sequence.
   *
   * Note: the `joinSpec` callback will be called one extra time with the `row`
   * parameter set to `null`. This allows RIGHT JOIN and FULL OUTER JOIN to be
   * implemented.
   *
   * @returns Returns the query object itself to allow chaining.
   * @example
   * ```js
   * const query = db.query()
   *  .join(row => row &&
   *    Array.from({length: +row.a})
   *    .map((_,i) => ({ ...row, d: i }))
   *  );
   *
   * console.table(query.toArray())
   * ```
   * Output
   * ```
   * ┌─────────┬─────┬─────┬─────┬───┐
   * │ (index) │ a   │ b   │ c   │ d │
   * ├─────────┼─────┼─────┼─────┼───┤
   * │ 0       │ '1' │ '2' │ '3' │ 0 │
   * │ 1       │ '4' │ '5' │ '6' │ 0 │
   * │ 2       │ '4' │ '5' │ '6' │ 1 │
   * │ 3       │ '4' │ '5' │ '6' │ 2 │
   * │ 4       │ '4' │ '5' │ '6' │ 3 │
   * └─────────┴─────┴─────┴─────┴───┘
   * ```
   */
  join(joinSpec: (row: RowObject | null) => RowObject[] | null) {
    this.#join.push(joinSpec);
    return this;
  }

  /**
   * Helper method to join two CSVDB objects (can also join `CSVDBQuery`s or
   * any other iterable).
   *
   * @param other Another `CSVDB` object; Another `CSVDBQuery` object; or any
   * other iterable.
   *
   * @param on A callback which is given two rows (one from each side of the
   * join) and returns a boolean to indicate whether or not this match should
   * be included in the result set.
   *
   * If `on` is not provided then the default behaviour is a cartesian join.
   * @returns Returns the query object itself to allow chaining.
   */
  joinOn(
    other: Iterable<CSVDB | CSVDBQuery | any>,
    on: (rowA: RowObject, rowB: RowObject) => boolean = () => true
  ) {
    let otherCache: RowObject[];

    this.#join.push((rowA) => {
      // Materialise `other` just once
      if (typeof otherCache === "undefined") {
        // @ts-ignore
        otherCache = [...other];
      }

      const out = [];

      if (rowA) {
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
   * Multiple calls will be AND'd together.
   *
   * @param predicate A callback which is provided with a `row` and an `index`
   * and returns a `boolean` indicating whether or not this row should be in the
   * result set.
   * @returns Returns the query object itself to allow chaining.
   *
   * @example
   * ```js
   * const query = db.query()
   *
   * query.where(r => r.a === "1")
   *
   * console.log(query.getNextRow())
   * ```
   * Output:
   * ```
   * {a: "1", b: "2", c: "3"}
   * ```
   */
  where(predicate: (row: RowObject, index: number) => boolean) {
    this.#where.push(predicate);
    return this;
  }

  /**
   * Group results into sets.
   * @param discriminator If discriminator is a function it is a selector which
   * extracts a value from a row which will then be used to group similar rows.
   * It can also be a string, in which case it is interpreted as a field name.
   *
   * Rows will be grouped by comparing the outputs from the discriminator with
   * `Object.is()`.
   *
   * @returns Returns the query object itself to allow chaining.
   * @example Using a discriminator function
   * ```js
   * const query = new CSVDB("a\n1\n2\n3").query()
   *
   * query
   *  .groupBy(r => +r.a % 2)
   *  .select({
   *    parity: r => +r.a % 2 ? "odd" : "even",
   *    count: "COUNT(*)"
   *  });
   *
   * console.log(query.toArray())
   * ```
   * Output:
   * ```
   * [ { parity: 'odd', count: 2 }, { parity: 'even', count: 1 } ]
   * ```
   *
   * @example Using a field name as a discriminator
   * ```js
   * const query = new CSVDB("a\n1\n2\n3\n2\n1").query()
   *
   * query
   *  .groupBy("a")
   *  .select({
   *    a: "a",
   *    count: "COUNT(*)"
   *  });
   *
   * console.log(query.toArray())
   * ```
   * Output:
   * ```
   * [ { a: '1', count: 2 }, { a: '2', count: 2 }, { a: '3', count: 1 } ]
   * ```
   */
  groupBy(discriminator: ((row: RowObject) => any) | string) {
    if (typeof discriminator === "string") {
      const d = discriminator;
      discriminator = (row) => row[d];
    }
    this.#groupBy = discriminator;
    return this;
  }

  /**
   * The `select()` method allows the user to specify the shape of the
   * {@link RowObject} produced by this query.
   *
   * There are two modes of specifying fields in the output with the `select()`
   * method. The simplest, and least flexible, is to specify an array of field
   * names. The other is to specify an object where the keys become column
   * aliases and each value is a {@link ColumnSpec}.
   *
   * A {@link ColumnSpec} is either a string or a javascript callback function
   * which receives the input row object and returns arbitrary values.
   *
   * If {@link ColumnSpec} is a string then it can either be a field name in the
   * input object or one of the builtin SQL functions. For a list of builtin
   * functions and examples of their use, see
   * {@link https://ijmacd.github.io/csvdb.js/}.
   *
   * An `OVER` clause can be specified by providing an array instead of just a
   * {@link ColumnSpec}:
   * ```
   * [column: ColumnSpec, over: windowName|WindowSpec]
   * ```
   *
   * A named {@link WindowSpec} can be created with the
   * {@link CSVDBQuery#window|window()} method.
   *
   * @param selection The specification for the output object.
   * @returns Returns the query object itself to allow chaining.
   *
   * @example Using field names
   * ```
   * const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
   * const query = db.query().select(["c", "a"]);
   *
   * console.log(query.toArray())
   * ```
   * Output:
   * ```
   * [ { c: '3', a: '1' }, { c: '6', a: '4' } ]
   * ```
   * @example Giving aliases
   * ```
   * const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
   * const query = db.query().select({
   *  first: "a",
   *  second: "b",
   * });
   *
   * console.log(query.toArray())
   * ```
   * Output:
   * ```
   * [ { first: '1', second: '2' }, { first: '4', second: '5' } ]
   * ```
   * @example Function columns
   * ```
   * const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
   * const query = db.query().select({
   *  summed: (row) => +row.a + +row.b + +row.c,
   * });
   *
   * console.log(query.toArray())
   * ```
   * Output:
   * ```
   * [ { summed: 6 }, { summed: 15 } ]
   * ```
   * @example Function column using index and group
   * The column function receives the row as the first argument and a 1-based
   * row number as the second argument. If the row is part of a group (either
   * due to a {@link CSVDBQuery#groupBy|groupBy()}, or a window clause) the
   * rowGroup is provided as the third argument.
   * ```
   * const db = new CSVDB("a,b,c\n1,2,3\n4,5,6\n7,8,9");
   * const query = db.query()
   *  .groupBy(r => +r.a % 2)
   *  .select({
   *    info: (row, i, group) =>
   *      `[Row: ${i}, Rows in group: ${group.length}] a=${group.map(r=> r.a).join()}`,
   *  });
   *
   * console.log(query.toArray())
   * ```
   * Output:
   * ```
   * [
   *  { info: '[Row: 1, Rows in group: 2] a=1,7' },
   *  { info: '[Row: 2, Rows in group: 1] a=4' }
   * ]
   * ```
   * @example With an OVER clause
   * ```
   * const db = new CSVDB("a,b,c\n1,2,3\n4,5,6\n7,8,9");
   * const query = db.query().select({
   *  a: "a",
   *  parity: r => +r.a % 2,
   *  parityCount: ["COUNT(*)", { partitionBy: r => +r.a % 2 }],
   * });
   *
   * console.log(query.toArray())
   * ```
   * Output:
   * ```
   * [
   *  { a: '1', parity: 1, count: 2 },
   *  { a: '4', parity: 0, count: 1 },
   *  { a: '7', parity: 1, count: 2 }
   * ]
   * ```
   */
  select(
    selection:
      | { [alias: string]: ColumnSpec | [ColumnSpec, string | WindowSpec] }
      | string[]
  ) {
    if (Array.isArray(selection)) {
      this.#selection = {};
      for (const col of selection) {
        this.#selection[col] = col;
      }
    } else {
      this.#selection = selection;
    }

    return this;
  }

  /**
   * Sorts based on input rows.
   *
   * To sort based on output from `select()` either chain queries together or
   * use `.toArray().sort()`.
   *
   * @param comparator A callback function which will be called with two rows
   * and should return a negative number, a positive number, or zero to indicate
   * the relative positioning of the two rows.
   *
   * Alternatively a string can be provided which will be used as a field name.
   * If the string is prefixed with `+` the rows are compared numerically. If
   * the string is prefixed with `-` the rows will be compared numerically in
   * descending order. Otherwise the fields will be compared as strings.
   *
   * @returns Returns the query object itself to allow chaining.
   *
   * @example Using a sort function
   * ```js
   * const query = new CSVDB("n\n1\n2\n10").query()
   *
   * query.orderBy((a, b) => (+a.n % 2) - (+b.n % 2))
   *
   * console.log(query.toArray())
   * ```
   * Output:
   * ```
   * [ { n: '2' }, { n: '10' }, { n: '1' } ]
   * ```
   *
   * @example Using a field name to sort alphabetically
   * ```js
   * const query = new CSVDB("n\n1\n2\n10").query()
   *
   * query.orderBy("n")
   *
   * console.log(query.toArray())
   * ```
   * Output:
   * ```
   * [ { n: '1' }, { n: '10' }, { n: '2' } ]
   * ```
   *
   * @example Using a field name to sort numerically
   * ```js
   * const query = new CSVDB("n\n1\n2\n10").query()
   *
   * query.orderBy("-n")
   *
   * console.log(query.toArray())
   * ```
   * Output:
   * ```
   * [ { n: '10' }, { n: '2' }, { n: '1' } ]
   * ```
   */
  orderBy(comparator: ((rowA: RowObject, rowB: RowObject) => number) | string) {
    if (typeof comparator === "string") {
      const c = comparator;

      if (c[0] === "-") {
        const f = comparator.substring(1);
        this.#sort = (rowA, rowB) => +rowB[f] - +rowA[f];
      } else if (c[0] === "+") {
        const f = comparator.substring(1);
        this.#sort = (rowA, rowB) => +rowA[f] - +rowB[f];
      } else {
        this.#sort = (rowA, rowB) => String(rowA[c]).localeCompare(rowB[c]);
      }
    } else {
      this.#sort = comparator;
    }

    return this;
  }

  /**
   * Skip the first `rows` number of rows of the result set.
   * @param rows Number of rows to skip
   * @returns Returns the query object itself to allow chaining.
   * @example
   * ```
   * const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
   * const query = db.query().offset(1);
   * console.log(query.getNextRow())
   * ```
   * Output:
   * ```
   * { a: '4', b: '5', c: '6' }
   * ```
   */
  offset(rows: number) {
    this.#offset = rows;
    return this;
  }

  /**
   * Limit the number of rows in the results set.
   * @param rows The number of rows to fetch
   * @returns Returns the query object itself to allow chaining.
   * @example
   * ```
   * const db = new CSVDB("a,b,c\n1,2,3\n4,5,6\n7,8,9");
   * const query = db.query().fetchFirst(2);
   * console.log(query.toArray())
   * ```
   * Output:
   * ```
   * {[ { a: '1', b: '2', c: '3' }, { a: '4', b: '5', c: '6' } ]
   * ```
   */
  fetchFirst(rows: number) {
    this.#limit = rows;
    return this;
  }

  /**
   * Create a named {@link WindowSpec} which can be referenced in an OVER clause,
   * although it offers little advantage over creating and reusing your own
   * {@link WindowSpec} objects.
   * @param name
   * @param spec
   * @returns Returns the query object itself to allow chaining.
   * @example
   * ```
   * const db = new CSVDB("a,b,c\n1,2,3\n2,4,6\n1,3,5");
   * const query = db.query().window("win1", { partitionBy: "a" }).select({
   *  a: "a",
   *  count: "COUNT(*) OVER win1",
   *  custom: [(row, i, rowGroup) => rowGroup.map(r => r.b).join(), "win1"],
   * });
   * console.log(query.toArray());
   * ```
   * Output:
   * ```
   * [
   *  { a: '1', count: 2, custom: '2,3' },
   *  { a: '2', count: 1, custom: '4' },
   *  { a: '1', count: 2, custom: '2,3' }
   * ]
   * ```
   * @example Alternative to window() method
   * ```
   * const db = new CSVDB("a,b,c\n1,2,3\n2,4,6\n1,3,5");
   * const win1 = { partitionBy: "a" };
   * const query = db.query().select({
   *  a: "a",
   *  count: ["COUNT(*)", win1],
   *  custom: [(row, i, rowGroup) => rowGroup.map(r => r.b).join(), win1],
   * });
   * console.log(query.toArray());
   * ```
   * Output:
   * ```
   * [
   *  { a: '1', count: 2, custom: '2,3' },
   *  { a: '2', count: 1, custom: '4' },
   *  { a: '1', count: 2, custom: '2,3' }
   * ]
   * ```
   */
  window(name: string, spec: WindowSpec) {
    this.#windowSpecs.set(name, spec);
    return this;
  }

  /**
   * Use `distinct()` to ensure results are a true set.
   *
   * @param distinct A boolean to force distinct on or off. Defaults to `true`
   * (on).
   *
   * @example
   * ```
   * const db = new CSVDB("a,b,c\n1,2,3\n1,2,4\n1,2,3");
   * const query = db.query().distinct();
   *
   * console.log(query.toArray());
   * ```
   * Output:
   * ```
   * [ { a: '1', b: '2', c: '3' }, { a: '1', b: '2', c: '4' } ]
   * ```
   */
  distinct(distinct = true) {
    this.#distinct = distinct;
    return this;
  }

  /**
   * Materialise result rows into a JavaScript array.
   */
  toArray() {
    return [...this];
  }

  /**
   * CSVDBQuery maintains its own internal iterator in order to provide this
   * convenience method.
   *
   * The iterator is started the first time either `getNextRow()` or
   * {@link CSVDBQuery#getNextValue| getNextValue()} is called.
   * @returns A single {@link RowObject}
   * @example
   * ```
   * const query = new CSVDB("a,b,c\n1,2,3\n4,5,6\n7,8,9").query();
   * console.log(query.getNextRow());
   * console.log(query.getNextRow());
   * ```
   * Output:
   * ```
   * { a: '1', b: '2', c: '3' }
   * { a: '4', b: '5', c: '6' }
   * ```
   */
  getNextRow(): RowObject {
    if (!this.#internalIterator) {
      this.#internalIterator = this[Symbol.iterator]();
    }

    return this.#internalIterator.next().value;
  }

  /**
   * CSVDBQuery maintains its own internal iterator in order to provide this
   * convenience method.
   *
   * The iterator is started the first time either
   * {@link CSVDBQuery#getNextRow | getNextRow()} or `getNextValue()` is called.
   * @param column The name of the column or the 0-indexed column number
   * (determined at the time the RowObject was created).
   * @returns A single value from the next row of the internal iterator
   * @example
   * ```
   * const query = new CSVDB("a,b,c\n1,2,3\n4,5,6\n7,8,9").query();
   * console.log(query.getNextValue("c"));
   * console.log(query.getNextValue(1));
   * console.log(query.getNextValue());
   * ```
   * Output:
   * ```
   * 3
   * 5
   * 7
   * ```
   */
  getNextValue(column: string | number = 0) {
    const row = this.getNextRow();
    return row
      ? typeof column === "string"
        ? row[column]
        : Object.values(row)[column]
      : undefined;
  }

  /**
   * CSVDBQuery implements `Symbol.Iterator` so that consumers can directly
   * iterate over the query.
   *
   * @example
   * ```
   * const db = new CSVDB(
   * `a,b
   * 1,2
   * 3,4
   * 5,6`);
   * const query = db.query().select(["a"]);
   *
   * for (const row of query) {
   *  console.log(`Value is ${row.a}`);
   * }
   * ```
   */
  [Symbol.iterator](): Iterator<RowObject> {
    return this.#iter();
  }

  *#iter(): Generator<RowObject> {
    if (this.#limit === 0) {
      return;
    }

    let rows: RowObject[] | Iterable<RowObject> = this.#rows;

    for (const join of this.#join) {
      const newRows = [];

      for (const row of rows) {
        const joinResult = join(row);
        joinResult && newRows.push(...joinResult);
      }

      // Once more with null to support RIGHT JOINs
      const joinResult = join(null);
      joinResult && newRows.push(...joinResult);

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
    let rowGroups: RowObject[][] | Iterable<RowObject> = rows;
    let allRowGroup: RowObject[] | undefined;

    const haveWindowFunctions =
      this.#windowSpecs.size > 0 ||
      (this.#selection &&
        Object.values(this.#selection).some(
          (s) =>
            (typeof s === "string" && s.endsWith(" OVER ()")) ||
            Array.isArray(s)
        ));

    if (this.#groupBy) {
      // groupRows() will materialise the rows
      rowGroups = groupRows(rows, this.#groupBy);
    } else if (this.#hasAggregates()) {
      // Produce a single row group with all rows
      // We're going to have to materialise the rows anyway so do it now
      rowGroups = [[...rows]];
    } else if (haveWindowFunctions) {
      // Produce an array a single level deep
      // Unfortunately we need to materialise the rows once to pass as the
      // 4th argument to mapSelectionToRow()
      allRowGroup = [...rows];
    }

    const distinctCache = [];

    // SELECT

    // Output row number
    let i = 0;

    for (const rowGroupOrRow of rowGroups) {
      // rowGroups can either be:
      // * An iterable of single rows
      // * An array of arrays of rows

      const isArrayOfArrays = Array.isArray(rowGroupOrRow);

      const sourceRow = isArrayOfArrays ? rowGroupOrRow[0] : rowGroupOrRow;
      const rowGroup = isArrayOfArrays
        ? rowGroupOrRow
        : allRowGroup || [sourceRow];

      const result = this.#mapSelectionToRow(
        sourceRow,
        this.#selection,
        i + 1,
        rowGroup
      );

      if (this.#distinct) {
        if (!isDistinct(distinctCache, result)) {
          continue;
        }

        distinctCache.push(result);
      }

      // OFFSET
      // We've done all the work (we had to wait until after `distinct()`) but
      // we'll only actually yield the result if we've passed the offset
      // threshold.
      if (i >= this.#offset) {
        yield result;
      }

      i++;

      // FETCH FIRST
      // Decide whether or not to continue onto next iteration
      if (i - this.#offset >= this.#limit) {
        return;
      }
    }
  }

  #hasAggregates() {
    if (!this.#selection) return false;

    return Object.values(this.#selection).some(
      (s) => typeof s === "string" && isAggregate(s)
    );
  }

  #mapSelectionToRow(
    sourceRow: RowObject,
    selection: SelectObject | null,
    index: number,
    groupRows: RowObject[]
  ) {
    const out: RowObject = {};

    if (!selection) {
      return sourceRow;
    }

    for (const [alias, col] of Object.entries(selection)) {
      let fn: SelectFunction | undefined;
      let field: string | undefined;
      let fnName: string | undefined;
      let args: string[] | undefined;
      let windowName: string | undefined;
      let windowSpec: WindowSpec | undefined;

      const re = /^([A-Z_]+)\(([^)]*)\)(?:\s+OVER\s+([\w\d_]+|\(\)))?$/;

      // Easily dealt with
      if (col instanceof Function) {
        out[alias] = col(sourceRow, index, groupRows);
        continue;
      }

      // Now check for array syntax
      if (Array.isArray(col)) {
        const [fnOrFnName, windowNameOrSpec] = col;

        if (typeof fnOrFnName === "string") {
          const fnString = fnOrFnName;

          const aggregateMatch = re.exec(fnString);
          if (aggregateMatch) {
            fnName = aggregateMatch[1];
            args = aggregateMatch[2].split(",");

            if (aggregateMatch[3]) {
              throw Error("Unexpected OVER");
            }
          } else {
            throw Error(`Bad Func: ${fnString}`);
          }
        } else {
          fn = fnOrFnName;
        }

        if (typeof windowNameOrSpec === "string") {
          windowName = windowNameOrSpec;
        } else {
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
        } else {
          field = col;
        }
      }

      if (windowName) {
        windowSpec =
          windowName === "()" ? {} : this.#windowSpecs.get(windowName);

        if (!windowSpec) {
          throw Error(`Bad Window: ${windowName}`);
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
        } else if (fnName in AGGREGATE_FUNCTIONS) {
          let values = rows.map((row) => row[args[0]]);
          value = AGGREGATE_FUNCTIONS[fnName](values);
        } else if (fnName in WINDOW_FUNCTIONS && windowSpec) {
          orderByCheck(windowSpec, fnName);

          value = WINDOW_FUNCTIONS[fnName](sourceRow, rows, args, windowSpec);
        } else if (fnName in POSITION_FUNCTIONS && windowSpec) {
          orderByCheck(windowSpec, fnName);
          let values = rows.map((row) => row[args[0]]);

          value = POSITION_FUNCTIONS[fnName](sourceRow, rows, args, values);
        } else if (fnName in STAT_FUNCTIONS) {
          let values = rows.map((row) => row[args[0]]);
          value = STAT_FUNCTIONS[fnName](values);
        } else {
          throw Error(`Bad Func: ${fnName}`);
        }

        out[alias] = value;
        continue;
      }

      // As long as the source row isn't null we can just copy the properties
      if (sourceRow) {
        if (col === "*") {
          Object.assign(
            out,
            alias === "*"
              ? sourceRow
              : Object.fromEntries(
                  Object.entries(sourceRow).map(([key, value]) => [
                    `${alias}${key}`,
                    value,
                  ])
                )
          );
        } else if (field) {
          out[alias] = sourceRow[field];
        }
      }
    }

    return out;
  }
}

function applyWindow(
  rows: RowObject[],
  windowSpec: WindowSpec,
  sourceRow: RowObject
) {
  if (windowSpec.partitionBy) {
    const pb = windowSpec.partitionBy;
    const fn = typeof pb === "string" ? (row: RowObject) => row[pb] : pb;
    const sympatheticValue = fn(sourceRow);
    rows = rows.filter((row) => fn(row) === sympatheticValue);
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
      } else if (windowSpec.framing[1] === "UNBOUNDED PRECEDING") {
        framingStart = -Infinity;
      } else if (windowSpec.framing[1] === CURRENT_ROW) {
        framingStart = 0;
      }

      if (typeof windowSpec.framing[2] === "number") {
        framingEnd = windowSpec.framing[2];
      } else if (windowSpec.framing[2] === "UNBOUNDED FOLLOWING") {
        framingEnd = Infinity;
      } else if (windowSpec.framing[2] === CURRENT_ROW) {
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

function getOrderBy(
  windowSpec: WindowSpec
): (rowA: RowObject, rowB: RowObject) => number {
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

function orderByCheck(windowSpec: WindowSpec, fnName: string) {
  if (!windowSpec?.orderBy) throw Error(`ORDER BY required: ${fnName}`);
}

function zip<T>(keys: string[], values: T[]) {
  const out: { [key: string]: T } = {};
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    out[key] = values[i];
  }
  return out;
}

function groupRows<T>(
  rows: Iterable<RowObject>,
  discriminator: (row: RowObject) => T
): RowObject[][] {
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

const isAggregate = (col: string) =>
  typeof col === "string" && /^[A-Z]+\([^)]*\)$/.test(col);

function parseCSVLine(line: string) {
  line = line.trim();
  const matches = line.matchAll(/([^",]*|"[^"]*")(,|$)/g);

  const m = [...matches];

  if (m[m.length - 1][0].length === 0) {
    m.length--;
  }

  return m.map((match) => match[1].trim().replace(/^"|"$/g, ""));
}

function* filter<T>(
  iterable: Iterable<T>,
  predicate: (item: T, index: number) => boolean
): Iterable<T> {
  let i = 0;
  for (const item of iterable) {
    if (predicate(item, i++)) {
      yield item;
    }
  }
}

function isDistinct(rows: RowObject[], row: RowObject) {
  return rows.every((rowB) => !isSame(row, rowB));
}

function isSame(rowA: RowObject, rowB: RowObject) {
  const keysA = Object.keys(rowA);
  const keysB = Object.keys(rowB);

  if (keysA.length !== keysB.length) return false;

  return keysA.every((key) => rowA[key] === rowB[key]);
}

function* except(resultsA: Iterable<RowObject>, resultsB: Iterable<RowObject>) {
  const cache = [...resultsB];

  for (const result of resultsA) {
    if (!isDistinct(cache, result)) {
      continue;
    }

    cache.push(result);

    yield result;
  }
}

function* intersect(
  resultsA: Iterable<RowObject>,
  resultsB: Iterable<RowObject>
) {
  const cache = [...resultsB];

  for (const result of resultsA) {
    if (isDistinct(cache, result)) {
      continue;
    }

    yield result;
  }
}

function* unionAll(
  resultsA: Iterable<RowObject>,
  resultsB: Iterable<RowObject>
) {
  for (const result of resultsA) {
    yield result;
  }

  for (const result of resultsB) {
    yield result;
  }
}

const SUM: (value: any[]) => number = (values) =>
  values.reduce((total: number, v) => total + +v, 0);

const AGGREGATE_FUNCTIONS: { [name: string]: (value: any[]) => any } = {
  SUM,
  AVG: (values) => SUM(values) / values.length,
  MAX: (values) => Math.max(...values),
  MIN: (values) => Math.min(...values),
  COUNT: (values) => values.length,
  LISTAGG: (values) => values.join(),
  ARRAY: (values) => values,
  JSON: (values) => JSON.stringify(values),
  ANY: (values) => values[0],
  RANDOM: (values) => values[Math.floor(Math.random() * values.length)],
};

const WINDOW_FUNCTIONS: {
  [name: string]: (
    sourceRow: RowObject,
    rows: RowObject[],
    args: string[],
    windowSpec: WindowSpec
  ) => any;
} = {
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
      const order = orderBy(rows[i - 1], rows[i]);
      if (order === 0) count++;
    }
    return index - count + 1;
  },
  NTILE: (sourceRow, rows, args, windowSpec) => {
    const index = rows.indexOf(sourceRow);
    return Math.floor((+args[0] * index) / rows.length) + 1;
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
      const [index, key] = result;

      return rows[index][key];
    }

    return null;
  },
  PERCENTILE_CONT: (sourceRow, rows, args, windowSpec) => {
    const result = findPercentile(rows, +args[0], windowSpec);
    if (result) {
      const [index, key, x] = result;

      const a = +rows[index - 1][key];
      const b = +rows[index][key];

      return x * (b - a) + a;
    }
    return null;
  },
};

function findPercentile(
  rows: RowObject[],
  percentile: number,
  windowSpec: WindowSpec
): [index: number, key: string, linear: number] | null {
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
      const x = (percentile - prevP) / (p - prevP);

      return [i, k, x];
    }

    prevP = p;
  }

  return null;
}

const POSITION_FUNCTIONS: {
  [name: string]: (
    sourceRow: RowObject,
    rows: RowObject[],
    args: string[],
    values: any[]
  ) => any;
} = {
  LEAD: (sourceRow, rows, args, values) => {
    const index = rows.indexOf(sourceRow);
    let delta = 1;
    if (args.length > 1) delta = +args[1];
    return values[index + delta] || null;
  },
  LAG: (sourceRow, rows, args, values) => {
    let delta = 1;
    if (args.length > 1) delta = +args[1];
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

const STAT_FUNCTIONS: { [name: string]: (value: any[]) => any } = {
  STDDEV_POP: (values) => Math.sqrt(VARIANCE_SUM(values) / values.length),
  STDDEV_SAMP: (values) =>
    Math.sqrt(VARIANCE_SUM(values) / (values.length - 1)),
  VAR_POP: (values) => VARIANCE_SUM(values) / values.length,
  VAR_SAMP: (values) => VARIANCE_SUM(values) / (values.length - 1),
};

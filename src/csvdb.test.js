import { CSVDB, CSVDBQuery } from "../dist/csvdb";

describe("CSVDB", () => {
  describe("rowCount", () => {
    it("is non-zero", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");

      expect(db.rowCount).toBe(2);
    });

    it("is zero", () => {
      const db = new CSVDB("a,b,c");

      expect(db.rowCount).toBe(0);
    });
  });

  describe("headers", () => {
    it("are returned", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");

      expect(db.headers).toEqual(["a", "b", "c"]);
    });
  });

  describe("iterator", () => {
    it("returns rows", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");

      const results = [...db];

      expect(results).toEqual([
        { a: "1", b: "2", c: "3" },
        { a: "4", b: "5", c: "6" },
      ]);
    });
  });

  describe("query()", () => {
    it("produces a CSVDBQuery object", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");

      expect(db.query() instanceof CSVDBQuery).toBe(true);
    });
  });

  describe("except", () => {
    it("produces expected rows", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
      const db2 = new CSVDB("a,b,c\n1,2,3\n1,3,5");

      const results = CSVDB.except(db, db2);

      expect([...results]).toEqual([{ a: "4", b: "5", c: "6" }]);
    });
  });

  describe("intersect", () => {
    it("produces expected rows", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
      const db2 = new CSVDB("a,b,c\n1,2,3\n1,3,5");

      const results = CSVDB.intersect(db, db2);

      expect([...results]).toEqual([{ a: "1", b: "2", c: "3" }]);
    });
  });

  describe("union", () => {
    it("produces expected rows", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
      const db2 = new CSVDB("a,b,c\n1,2,3\n1,3,5");

      const results = CSVDB.union(db, db2);

      expect([...results]).toEqual([
        { a: "1", b: "2", c: "3" },
        { a: "4", b: "5", c: "6" },
        { a: "1", b: "3", c: "5" },
      ]);
    });
  });

  describe("unionAll", () => {
    it("produces expected rows", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
      const db2 = new CSVDB("a,b,c\n1,2,3\n1,3,5");

      const results = CSVDB.unionAll(db, db2);

      expect([...results]).toEqual([
        { a: "1", b: "2", c: "3" },
        { a: "4", b: "5", c: "6" },
        { a: "1", b: "2", c: "3" },
        { a: "1", b: "3", c: "5" },
      ]);
    });
  });
});

describe("CSVDBQuery", () => {
  describe("join", () => {
    test("computed rows", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
      const query = db
        .query()
        .join(
          (row) =>
            row &&
            Array.from({ length: +row.a }).map((_, i) => ({ ...row, d: i }))
        );

      expect(query.toArray()).toEqual([
        { a: "1", b: "2", c: "3", d: 0 },
        { a: "4", b: "5", c: "6", d: 0 },
        { a: "4", b: "5", c: "6", d: 1 },
        { a: "4", b: "5", c: "6", d: 2 },
        { a: "4", b: "5", c: "6", d: 3 },
      ]);
    });

    test("inner join", () => {
      const db = new CSVDB("owner,animal\ntom,cat\ndick,dog\nharry,bird");
      const db2 = new CSVDB(
        "animal,family\ncat,feline\ndog,canine\nhorse,equine"
      );

      const query = db.query().join((rowA) => {
        // RIGHT
        if (!rowA) return [];
        // INNER
        return db2
          .query()
          .where((rowB) => rowB.animal === rowA.animal)
          .toArray()
          .map((rowB) => ({ ...rowA, ...rowB }));
      });

      expect(query.toArray()).toEqual([
        { owner: "tom", animal: "cat", family: "feline" },
        { owner: "dick", animal: "dog", family: "canine" },
      ]);
    });

    test("left join", () => {
      const db = new CSVDB("owner,animal\ntom,cat\ndick,dog\nharry,bird");
      const db2 = new CSVDB(
        "animal,family\ncat,feline\ndog,canine\nhorse,equine"
      );

      const nullRow = (/** @type {CSVDB} */ db) =>
        Object.fromEntries(db.headers.map((header) => [header, null]));

      const query = db.query().join((rowA) => {
        // RIGHT
        if (!rowA) return [];

        // INNER
        const rows = db2
          .query()
          .where((rowB) => rowB.animal === rowA.animal)
          .toArray();

        if (rows.length > 0) {
          return rows.map((rowB) => ({ ...rowA, ...rowB }));
        }

        // LEFT
        return [{ ...nullRow(db2), ...rowA }];
      });

      expect(query.toArray()).toEqual([
        { owner: "tom", animal: "cat", family: "feline" },
        { owner: "dick", animal: "dog", family: "canine" },
        { animal: "bird", family: null, owner: "harry" },
      ]);
    });

    test("right join", () => {
      const db = new CSVDB("owner,animal\ntom,cat\ndick,dog\nharry,bird");
      const db2 = new CSVDB(
        "animal,family\ncat,feline\ndog,canine\nhorse,equine"
      );
      const nullRow = (db) =>
        Object.fromEntries(db.headers.map((header) => [header, null]));
      const set = new Set();
      const query = db.query().join((rowA) => {
        // RIGHT
        if (!rowA) {
          const innerSet = [...set];

          return db2
            .query()
            .where((rowB) => !innerSet.includes(rowB.animal))
            .toArray()
            .map((rowB) => ({ ...nullRow(db), ...rowB }));
        }

        // Save the processed animals for later when we come to the right part
        // of the join
        set.add(rowA.animal);

        // INNER
        const rows = db2
          .query()
          .where((rowB) => rowB.animal === rowA.animal)
          .toArray();

        if (rows.length > 0) {
          return rows.map((rowB) => ({ ...rowA, ...rowB }));
        }

        // LEFT
        return [];
      });

      expect(query.toArray()).toEqual([
        { owner: "tom", animal: "cat", family: "feline" },
        { owner: "dick", animal: "dog", family: "canine" },
        { owner: null, animal: "horse", family: "equine" },
      ]);
    });

    test("full join", () => {
      const db = new CSVDB("owner,animal\ntom,cat\ndick,dog\nharry,bird");
      const db2 = new CSVDB(
        "animal,family\ncat,feline\ndog,canine\nhorse,equine"
      );
      const nullRow = (db) =>
        Object.fromEntries(db.headers.map((header) => [header, null]));
      const set = new Set();
      const query = db.query().join((rowA) => {
        // RIGHT
        if (!rowA) {
          const innerSet = [...set];
          return db2
            .query()
            .where((rowB) => !innerSet.includes(rowB.animal))
            .toArray()
            .map((rowB) => ({ ...nullRow(db), ...rowB }));
        }

        // Save the processed animals for later when we come to the right part
        // of the join
        set.add(rowA.animal);

        // INNER
        const rows = db2
          .query()
          .where((rowB) => rowB.animal === rowA.animal)
          .toArray();

        if (rows.length > 0) {
          return rows.map((rowB) => ({ ...rowA, ...rowB }));
        }

        // LEFT
        return [{ ...nullRow(db2), ...rowA }];
      });

      expect(query.toArray()).toEqual([
        { owner: "tom", animal: "cat", family: "feline" },
        { owner: "dick", animal: "dog", family: "canine" },
        { animal: "bird", family: null, owner: "harry" },
        { owner: null, animal: "horse", family: "equine" },
      ]);
    });
  });

  describe("joinOn", () => {
    test("string array", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
      const db2 = new CSVDB("i,j,k\n2,4,6\n3,6,9");
      const results = db
        .query()
        .joinOn(
          db2,
          (rowA, rowB) =>
            rowA.b === rowB.i || rowA.c === rowB.i || rowA.a === rowB.j
        );

      expect(results.toArray()).toEqual([
        { a: "1", b: "2", c: "3", i: "2", j: "4", k: "6" },
        { a: "1", b: "2", c: "3", i: "3", j: "6", k: "9" },
        { a: "4", b: "5", c: "6", i: "2", j: "4", k: "6" },
      ]);
    });
  });

  describe("where", () => {
    test("filters rows", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
      const query = db.query().where((r) => r.a === "1");

      expect(query.toArray()).toEqual([{ a: "1", b: "2", c: "3" }]);
    });
  });

  describe("groupBy", () => {
    test("groups rows", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n4,5,6\n1,3,5");
      const query = db
        .query()
        .groupBy((row) => row.a)
        .select(["a", "COUNT(*)"]);

      expect(query.toArray()).toEqual([
        { a: "1", "COUNT(*)": 2 },
        { a: "4", "COUNT(*)": 1 },
      ]);
    });
  });

  describe("select", () => {
    test("string array", () => {
      const db = new CSVDB("a,b,c\n1,2,3");
      const query = db.query().select(["c", "a"]);

      expect(query.toArray()).toEqual([{ c: "3", a: "1" }]);
    });

    test("object", () => {
      const db = new CSVDB("a,b,c\n1,2,3");
      const query = db.query().select({ col1: "a", col2: (r) => +r.b + +r.c });

      expect(query.toArray()).toEqual([{ col1: "1", col2: 5 }]);
    });

    test("over clause with window spec object", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,6\n1,3,5");
      const query = db
        .query()
        .select({
          col1: "a",
          col2: ["COUNT(*)", { partitionBy: "a" }],
        })
        .fetchFirst(2);

      expect(query.toArray()).toEqual([
        { col1: "1", col2: 2 },
        { col1: "2", col2: 1 },
      ]);
    });

    test("over clause with named window", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,6\n1,3,5");
      const query = db
        .query()
        .window("win1", { partitionBy: "a" })
        .select({
          col1: "a",
          col2: ["COUNT(*)", "win1"],
        })
        .fetchFirst(2);

      expect(query.toArray()).toEqual([
        { col1: "1", col2: 2 },
        { col1: "2", col2: 1 },
      ]);
    });
  });

  describe("orderBy", () => {
    test("sorts rows by custom function", () => {
      const query = new CSVDB("n\n1\n2\n10").query();

      query.orderBy((a, b) => (+a.n % 2) - (+b.n % 2));

      expect(query.toArray()).toEqual([{ n: "2" }, { n: "10" }, { n: "1" }]);
    });

    test("sorts rows by field name alphabetically", () => {
      const query = new CSVDB("n\n1\n2\n10").query();

      query.orderBy("n");

      expect(query.toArray()).toEqual([{ n: "1" }, { n: "10" }, { n: "2" }]);
    });

    test("sorts rows by field name numerically ascending", () => {
      const query = new CSVDB("n\n1\n2\n10").query();

      query.orderBy("+n");

      expect(query.toArray()).toEqual([{ n: "1" }, { n: "2" }, { n: "10" }]);
    });

    test("sorts rows by field name numerically descending", () => {
      const query = new CSVDB("n\n1\n2\n10").query();

      query.orderBy("-n");

      expect(query.toArray()).toEqual([{ n: "10" }, { n: "2" }, { n: "1" }]);
    });
  });

  describe("offset", () => {
    test("skip rows", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");
      const query = db.query().offset(1);

      expect(query.toArray()).toEqual([{ a: "4", b: "5", c: "6" }]);
    });
  });

  describe("fetch first", () => {
    test("limits rows", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n4,5,6\n7,8,9");
      const query = db.query().fetchFirst(2);

      expect(query.toArray()).toEqual([
        { a: "1", b: "2", c: "3" },
        { a: "4", b: "5", c: "6" },
      ]);
    });
  });

  describe("window", () => {
    test("defines windows", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,6\n1,3,5");
      const query = db
        .query()
        .window("win1", { partitionBy: "a" })
        .window("win2", { partitionBy: "b" })
        .select({
          a: "a",
          count: "COUNT(*) OVER win1",
        });

      expect(query.toArray()).toEqual([
        { a: "1", count: 2 },
        { a: "2", count: 1 },
        { a: "1", count: 2 },
      ]);
    });
  });

  describe("distinct", () => {
    test("ensures results are a true set", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n1,2,4\n1,2,3");
      const query = db.query().distinct();

      expect(query.toArray()).toEqual([
        { a: "1", b: "2", c: "3" },
        { a: "1", b: "2", c: "4" },
      ]);
    });
  });

  describe("getNextRow", () => {
    test("gets rows one by one", () => {
      const query = new CSVDB("a,b,c\n1,2,3\n4,5,6\n7,8,9").query();

      expect(query.getNextRow()).toEqual({ a: "1", b: "2", c: "3" });
      expect(query.getNextRow()).toEqual({ a: "4", b: "5", c: "6" });
    });
  });

  describe("getNextValue", () => {
    test("gets a value from rows one by one", () => {
      const query = new CSVDB("a,b,c\n1,2,3\n4,5,6\n7,8,9").query();

      expect(query.getNextValue("c")).toEqual("3");
      expect(query.getNextValue(1)).toEqual("5");
      expect(query.getNextValue()).toEqual("7");
    });
  });

  describe("iterator", () => {
    it("returns rows", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n4,5,6");

      const results = [...db.query()];

      expect(results).toEqual([
        { a: "1", b: "2", c: "3" },
        { a: "4", b: "5", c: "6" },
      ]);
    });
  });
});

describe("SQL Functions", () => {
  describe("Aggregate Functions", () => {
    describe("COUNT(*)", () => {
      it("counts 0 rows", () => {
        const db = new CSVDB("a,b,c");

        const query = db.query().select(["COUNT(*)"]);

        expect(query.getNextValue()).toBe(0);
      });

      it("counts 1 row", () => {
        const db = new CSVDB("a,b,c\n1,2,3");

        const query = db.query().select(["COUNT(*)"]);

        expect(query.getNextValue()).toBe(1);
      });

      it("counts 3 rows", () => {
        const db = new CSVDB("a,b,c\n1,2,3\n4,5,6\n7,8,9");

        const query = db.query().select(["COUNT(*)"]);

        expect(query.getNextValue()).toBe(3);
      });

      it("counts 10 rows", () => {
        const db = new CSVDB("a\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10");

        const query = db.query().select(["COUNT(*)"]);

        expect(query.getNextValue()).toBe(10);
      });
    });

    describe("MIN(a)", () => {
      it("finds the minimum of 1 row", () => {
        const db = new CSVDB("a,b,c\n4,5,6");

        const query = db.query().select(["MIN(a)"]);

        expect(query.getNextValue()).toBe(4);
      });

      it("finds the minimum of 3 rows", () => {
        const db = new CSVDB("a,b,c\n4,5,6\n1,2,3\n7,8,9");

        const query = db.query().select(["MIN(a)"]);

        expect(query.getNextValue()).toBe(1);
      });

      it("finds the minimum of 3 rows with negatives", () => {
        const db = new CSVDB("a,b,c\n4,5,6\n-1,2,3\n-7,8,9");

        const query = db.query().select(["MIN(a)"]);

        expect(query.getNextValue()).toBe(-7);
      });

      it("the minimum of 0 rows is infinity", () => {
        const db = new CSVDB("a,b,c");

        const query = db.query().select(["MIN(a)"]);

        expect(query.getNextValue()).toBe(Infinity);
      });
    });

    describe("MAX(a)", () => {
      it("finds the maximum of 1 row", () => {
        const db = new CSVDB("a,b,c\n4,5,6");

        const query = db.query().select(["MAX(a)"]);

        expect(query.getNextValue()).toBe(4);
      });

      it("finds the maximum of 3 rows", () => {
        const db = new CSVDB("a,b,c\n4,5,6\n1,2,3\n7,8,9");

        const query = db.query().select(["MAX(a)"]);

        expect(query.getNextValue()).toBe(7);
      });

      it("finds the maximum of 3 rows with negatives", () => {
        const db = new CSVDB("a,b,c\n4,5,6\n-1,2,3\n-7,8,9");

        const query = db.query().select(["MAX(a)"]);

        expect(query.getNextValue()).toBe(4);
      });

      it("the maximum of 0 rows is negative infinity", () => {
        const db = new CSVDB("a,b,c");

        const query = db.query().select(["MAX(a)"]);

        expect(query.getNextValue()).toBe(-Infinity);
      });
    });

    describe("SUM(a)", () => {
      it("sums 1 row", () => {
        const db = new CSVDB("a,b,c\n4,5,6");

        const query = db.query().select(["SUM(a)"]);

        expect(query.getNextValue()).toBe(4);
      });

      it("sums 3 rows", () => {
        const db = new CSVDB("a,b,c\n4,5,6\n1,2,3\n7,8,9");

        const query = db.query().select(["SUM(a)"]);

        expect(query.getNextValue()).toBe(12);
      });

      it("sums 3 rows with negative numbers", () => {
        const db = new CSVDB("a,b,c\n4,5,6\n-1,2,3\n-7,8,9");

        const query = db.query().select(["SUM(a)"]);

        expect(query.getNextValue()).toBe(-4);
      });

      it("sum of 0 rows is null", () => {
        const db = new CSVDB("a,b,c");

        const query = db.query().select(["SUM(a)"]);

        expect(query.getNextValue()).toBeNull();
      });
    });

    describe("AVG(a)", () => {
      it("finds the average of 1 row", () => {
        const db = new CSVDB("a,b,c\n4,5,6");

        const query = db.query().select(["AVG(a)"]);

        expect(query.getNextValue()).toBe(4);
      });

      it("finds the average of 3 rows", () => {
        const db = new CSVDB("a,b,c\n4,5,6\n1,2,3\n7,8,9");

        const query = db.query().select(["AVG(a)"]);

        expect(query.getNextValue()).toBe(4);
      });

      it("the average of 0 rows is null", () => {
        const db = new CSVDB("a,b,c");

        const query = db.query().select(["AVG(a)"]);

        expect(query.getNextValue()).toBeNull();
      });
    });

    describe("LISTAGG(a)", () => {
      it("lists the values of 3 rows", () => {
        const db = new CSVDB("a,b,c\n4,5,6\n1,2,3\n7,8,9");

        const query = db.query().select(["LISTAGG(a)"]);

        expect(query.getNextValue()).toBe("4,1,7");
      });

      it("list of 0 rows is null", () => {
        const db = new CSVDB("a,b,c");

        const query = db.query().select(["LISTAGG(a)"]);

        expect(query.getNextValue()).toBeNull();
      });
    });

    describe("JSON(a)", () => {
      it("creates a JSON array from 3 rows", () => {
        const db = new CSVDB("a,b,c\n4,5,6\n1,2,3\n7,8,9");

        const query = db.query().select(["JSON(a)"]);

        expect(query.getNextValue()).toBe(`["4","1","7"]`);
      });

      it("creates a JSON array from 0 rows", () => {
        const db = new CSVDB("a,b,c");

        const query = db.query().select(["JSON(a)"]);

        expect(query.getNextValue()).toBe(`[]`);
      });
    });

    describe("ARRAY(a)", () => {
      it("creates an array from 3 rows", () => {
        const db = new CSVDB("a,b,c\n4,5,6\n1,2,3\n7,8,9");

        const query = db.query().select(["ARRAY(a)"]);

        expect(query.getNextValue()).toEqual(["4", "1", "7"]);
      });

      it("creates an array from 0 rows", () => {
        const db = new CSVDB("a,b,c");

        const query = db.query().select(["ARRAY(a)"]);

        expect(query.getNextValue()).toEqual([]);
      });
    });

    describe("ANY(a)", () => {
      it("picks a value from 1 of 3 rows", () => {
        const db = new CSVDB("a,b,c\n4,5,6\n1,2,3\n7,8,9");

        const query = db.query().select(["ANY(a)"]);

        expect(query.getNextValue()).toBe("4");
      });

      it("any from 0 rows is null", () => {
        const db = new CSVDB("a,b,c");

        const query = db.query().select(["ANY(a)"]);

        expect(query.getNextValue()).toBeNull();
      });
    });

    describe("RANDOM(a)", () => {
      it("picks a random value 1 of 3 rows", () => {
        const db = new CSVDB("a,b,c\n4,5,6\n1,2,3\n7,8,9");

        const query = db.query().select(["RANDOM(a)"]);

        expect(["4", "1", "7"].includes(query.getNextValue())).toBe(true);
      });

      it("random value from 0 rows is null", () => {
        const db = new CSVDB("a,b,c");

        const query = db.query().select(["RANDOM(a)"]);

        expect(query.getNextValue()).toBeNull();
      });
    });
  });

  describe("Ranking Functions", () => {
    it("ROW_NUMBER()", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          rowNumber: "ROW_NUMBER() OVER win1",
        });

      expect(results.getNextValue()).toBe(1);
      expect(results.getNextValue()).toBe(2);
      expect(results.getNextValue()).toBe(3);
      expect(results.getNextValue()).toBe(4);
    });

    it("RANK()", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          rank: "RANK() OVER win1",
        });

      expect(results.getNextValue()).toBe(1);
      expect(results.getNextValue()).toBe(2);
      expect(results.getNextValue()).toBe(2);
      expect(results.getNextValue()).toBe(4);
    });

    it("DENSE_RANK()", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          denseRank: "DENSE_RANK() OVER win1",
        });
      expect(results.getNextValue()).toBe(1);
      expect(results.getNextValue()).toBe(2);
      expect(results.getNextValue()).toBe(2);
      expect(results.getNextValue()).toBe(3);
    });

    it("NTILE(2)", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          ntile: "NTILE(2) OVER win1",
        });
      expect(results.getNextValue()).toBe(1);
      expect(results.getNextValue()).toBe(1);
      expect(results.getNextValue()).toBe(2);
      expect(results.getNextValue()).toBe(2);
    });

    it("PERCENT_RANK()", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          percentRank: "PERCENT_RANK() OVER win1",
        });
      expect(results.getNextValue()).toBe(0);
      expect(results.getNextValue()).toBeCloseTo(1 / 3);
      expect(results.getNextValue()).toBeCloseTo(1 / 3);
      expect(results.getNextValue()).toBe(1);
    });

    it("CUME_DIST()", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          cumeDist: "CUME_DIST() OVER win1",
        });
      expect(results.getNextValue()).toBe(0.25);
      expect(results.getNextValue()).toBe(0.75);
      expect(results.getNextValue()).toBe(0.75);
      expect(results.getNextValue()).toBe(1);
    });

    it("PERCENTILE_DIST(0.5)", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          median: "PERCENTILE_DIST(0.5) OVER win1",
        });
      expect(results.getNextValue()).toBe("4");
      expect(results.getNextValue()).toBe("4");
      expect(results.getNextValue()).toBe("4");
      expect(results.getNextValue()).toBe("4");
    });

    it("PERCENTILE_CONT(0.5)", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          medianCont: "PERCENTILE_CONT(0.5) OVER win1",
        });
      expect(results.getNextValue()).toBe(3);
      expect(results.getNextValue()).toBe(3);
      expect(results.getNextValue()).toBe(3);
      expect(results.getNextValue()).toBe(3);
    });
  });

  describe("Position Functions", () => {
    it("LAG(b)", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n1,3,5\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          rowNumber: "LAG(b) OVER win1",
        });

      expect(results.getNextValue()).toBeNull();
      expect(results.getNextValue()).toBe("2");
      expect(results.getNextValue()).toBe("3");
      expect(results.getNextValue()).toBe("4");
    });

    it("LEAD(c, 2)", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n1,3,5\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          rowNumber: "LEAD(c, 2) OVER win1",
        });

      expect(results.getNextValue()).toBe("7");
      expect(results.getNextValue()).toBe("6");
      expect(results.getNextValue()).toBeNull();
      expect(results.getNextValue()).toBeNull();
    });

    it("FIRST_VALUE(b)", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n1,3,5\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          partitionBy: "a",
          orderBy: "b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          rowNumber: "FIRST_VALUE(b) OVER win1",
        });

      expect(results.getNextValue()).toBe("2");
      expect(results.getNextValue()).toBe("2");
      expect(results.getNextValue()).toBe("2");
      expect(results.getNextValue()).toBe("5");
    });

    it("LAST_VALUE(c)", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n1,3,5\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          partitionBy: "a",
          orderBy: "b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          rowNumber: "LAST_VALUE(c) OVER win1",
        });

      expect(results.getNextValue()).toBe("7");
      expect(results.getNextValue()).toBe("7");
      expect(results.getNextValue()).toBe("7");
      expect(results.getNextValue()).toBe("6");
    });

    it("NTH_VALUE(c, 2)", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n1,3,5\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          partitionBy: "a",
          orderBy: "b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          rowNumber: "NTH_VALUE(c, 2) OVER win1",
        });

      expect(results.getNextValue()).toBe("5");
      expect(results.getNextValue()).toBe("5");
      expect(results.getNextValue()).toBe("5");
      expect(results.getNextValue()).toBeNull();
    });
  });

  describe("Statistical Functions", () => {
    it("PERCENTILE_DIST(0.25)", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          rowNumber: "PERCENTILE_DIST(0.25) OVER win1",
        });

      expect(results.getNextValue()).toBe("2");
    });

    it("PERCENTILE_DIST(0.5)", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          rowNumber: "PERCENTILE_DIST(0.5) OVER win1",
        });

      expect(results.getNextValue()).toBe("4");
    });

    it("PERCENTILE_DIST(0.75)", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          rowNumber: "PERCENTILE_DIST(0.75) OVER win1",
        });

      expect(results.getNextValue()).toBe("4");
    });

    it("STDDEV_POP(b)", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          rowNumber: "STDDEV_POP(b) OVER win1",
        });

      expect(results.getNextValue()).toBeCloseTo(0.521949407482461);
    });

    it("STDDEV_SAMP(b)", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          rowNumber: "STDDEV_SAMP(b) OVER win1",
        });

      expect(results.getNextValue()).toBeCloseTo(0.6026952618267291);
    });

    it("VAR_POP(b)", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          rowNumber: "VAR_POP(b) OVER win1",
        });

      expect(results.getNextValue()).toBeCloseTo(0.2724311839712921);
    });

    it("VAR_SAMP(b)", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          rowNumber: "VAR_SAMP(b) OVER win1",
        });

      expect(results.getNextValue()).toBeCloseTo(0.3632415786283895);
    });
  });
});

describe("Windows", () => {
  describe("orderBy", () => {
    it("field alphabetically", () => {
      const db = new CSVDB("a,b,c\n1,20,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "b",
        })
        .select({
          a: "LISTAGG(a) OVER win1",
        });

      expect(results.getNextValue()).toBe("1");
      expect(results.getNextValue()).toBe("1,2");
      expect(results.getNextValue()).toBe("1,2,1");
      expect(results.getNextValue()).toBe("1,2,1,4");
    });

    it("field numerically", () => {
      const db = new CSVDB("a,b,c\n1,20,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
        })
        .select({
          a: "LISTAGG(a) OVER win1",
        });

      expect(results.getNextValue()).toBe("2,1,4,1");
      expect(results.getNextValue()).toBe("2");
      expect(results.getNextValue()).toBe("2,1");
      expect(results.getNextValue()).toBe("2,1,4");
    });

    it("field numerically descending", () => {
      const db = new CSVDB("a,b,c\n1,20,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "-b",
        })
        .select({
          a: "LISTAGG(a) OVER win1",
        });

      expect(results.getNextValue()).toBe("1");
      expect(results.getNextValue()).toBe("1,4,2");
      expect(results.getNextValue()).toBe("1,4,2,1");
      expect(results.getNextValue()).toBe("1,4");
    });

    it("custom function", () => {
      const db = new CSVDB("a,b,c\n1,20,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: (rowA, rowB) => (rowA.c % 6) - (rowB.c % 6),
        })
        .select({
          a: "LISTAGG(a) OVER win1",
        });

      expect(results.getNextValue()).toBe("4,1,2,1");
      expect(results.getNextValue()).toBe("4,1,2");
      expect(results.getNextValue()).toBe("4,1");
      expect(results.getNextValue()).toBe("4");
    });
  });

  describe("Framing", () => {
    it("Default", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
        })
        .select({
          a: "LISTAGG(a) OVER win1",
        });

      expect(results.getNextValue()).toBe("1");
      expect(results.getNextValue()).toBe("1,2");
      expect(results.getNextValue()).toBe("1,2,1");
      expect(results.getNextValue()).toBe("1,2,1,4");
    });

    it("Explicit preceding", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", 0],
        })
        .select({
          a: "LISTAGG(a) OVER win1",
        });

      expect(results.getNextValue()).toBe("1");
      expect(results.getNextValue()).toBe("1,2");
      expect(results.getNextValue()).toBe("1,2,1");
      expect(results.getNextValue()).toBe("1,2,1,4");
    });

    it("Explicit following", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", 0, "UNBOUNDED FOLLOWING"],
        })
        .select({
          a: "LISTAGG(a) OVER win1",
        });

      expect(results.getNextValue()).toBe("1,2,1,4");
      expect(results.getNextValue()).toBe("2,1,4");
      expect(results.getNextValue()).toBe("1,4");
      expect(results.getNextValue()).toBe("4");
    });

    it("Explicit both", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          orderBy: "+b",
          framing: ["ROWS", "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"],
        })
        .select({
          a: "LISTAGG(a) OVER win1",
        });
      expect(results.getNextValue()).toBe("1,2,1,4");
      expect(results.getNextValue()).toBe("1,2,1,4");
      expect(results.getNextValue()).toBe("1,2,1,4");
      expect(results.getNextValue()).toBe("1,2,1,4");
    });
  });

  describe("Partition", () => {
    it("field", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          partitionBy: "b",
          orderBy: "+a",
        })
        .select({
          a: "LISTAGG(a) OVER win1",
        });

      expect(results.getNextValue()).toBe("1");
      expect(results.getNextValue()).toBe("1,2");
      expect(results.getNextValue()).toBe("1");
      expect(results.getNextValue()).toBe("4");
    });

    it("function", () => {
      const db = new CSVDB("a,b,c\n1,2,3\n2,4,8\n1,4,7\n4,5,6");
      const results = db
        .query()
        .window("win1", {
          partitionBy: (row) => row.a % 2,
          orderBy: "+b",
        })
        .select({
          a: "LISTAGG(a) OVER win1",
        });

      expect(results.getNextValue()).toBe("1");
      expect(results.getNextValue()).toBe("2");
      expect(results.getNextValue()).toBe("1,1");
      expect(results.getNextValue()).toBe("2,4");
    });
  });
});

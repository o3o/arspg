module arspg;

pragma(lib, "pq");


import std.string;
import std.exception;
import std.variant;
import std.datetime;
import std.conv;

class DatabaseException : Exception {
   this(string msg, string file = __FILE__, size_t line = __LINE__) {
      super(msg, file, line);
   }
}

class PostgreSql {
   this(string connectionString) {
      this.connectionString = connectionString;
      conn = PQconnectdb(toStringz(connectionString));
      if (conn is null)
         throw new DatabaseException("Unable to allocate PG connection object");
      if (PQstatus(conn) != CONNECTION_OK)
         throw new DatabaseException(error());
      query("SET NAMES 'utf8'"); // D does everything with utf8
   }

   string connectionString;

   ~this() {
      PQfinish(conn);
   }

   /// Just executes a query. It supports placeholders for parameters
   final ResultSet query(T...)(string sql, T t) {
      Variant[] args;
      foreach (arg; t) {
         Variant a;
         static if (__traits(compiles, a = arg))
            a = arg;
         else
            a = to!string(t);
         args ~= a;
      }
      return queryImpl(sql, args);
   }

   string sysTimeToValue(SysTime s) {
      return "'" ~ escape(s.toISOExtString()) ~ "'::timestamptz";
   }

   ResultSet executePreparedStatement(T...)(string name, T args) {
      char*[args.length] argsStrings;

      foreach (idx, arg; args) {
         // FIXME: optimize to remove allocations here
         static if (!is(typeof(arg) == typeof(null)))
            argsStrings[idx] = toStringz(to!string(arg));
         // else make it null
      }

      auto res = PQexecPrepared(conn, toStringz(name), argsStrings.length, argStrings.ptr, 0, null, 0);

      int ress = PQresultStatus(res);
      if (ress != PGRES_TUPLES_OK && ress != PGRES_COMMAND_OK)
         throw new DatabaseException(error());

      return new PostgresResult(res);

   }

   ///
   void startTransaction() {
      query("START TRANSACTION");
   }

   ResultSet queryImpl(string sql, Variant[] args...) {
      sql = escapedVariants(this, sql, args);

      bool first_retry = true;

   retry:

      auto res = PQexec(conn, toStringz(sql));
      int ress = PQresultStatus(res);
      // https://www.postgresql.org/docs/current/libpq-exec.html
      // FIXME: PQresultErrorField can get a lot more info in a more structured way
      if (ress != PGRES_TUPLES_OK && ress != PGRES_COMMAND_OK) {
         if (first_retry && error() == "no connection to the server\n") {
            first_retry = false;
            // try to reconnect...
            PQfinish(conn);
            conn = PQconnectdb(toStringz(connectionString));
            if (conn is null)
               throw new DatabaseException("Unable to allocate PG connection object");
            if (PQstatus(conn) != CONNECTION_OK)
               throw new DatabaseException(error());
            goto retry;
         }
         throw new DatabaseException(error());
      }

      return new ResultSet(res);
   }

   string escape(string sqlData) {
      char* buffer = (new char[sqlData.length * 2 + 1]).ptr;
      ulong size = PQescapeString(buffer, sqlData.ptr, sqlData.length);

      string ret = assumeUnique(buffer[0 .. cast(size_t)size]);

      return ret;
   }

   ///
   string error() {
      return copyCString(PQerrorMessage(conn));
   }

   private PGconn* conn;
}

class ResultSet {
   // name for associative array to result index
   int getFieldIndex(string field) {
      if (mapping is null)
         makeFieldMapping();
      field = field.toLower;
      if (field in mapping)
         return mapping[field];
      else
         throw new Exception("no mapping " ~ field);
   }

   string[] fieldNames() {
      if (mapping is null)
         makeFieldMapping();
      return columnNames;
   }

   // this is a range that can offer other ranges to access it
   bool empty() {
      return position == numRows;
   }

   Row front() {
      return row;
   }

   int affectedRows() {
      auto g = PQcmdTuples(res);
      if (g is null)
         return 0;
      int num;
      while (*g) {
         num *= 10;
         num += *g - '0';
         g++;
      }
      return num;
   }

   void popFront() {
      position++;
      if (position < numRows)
         fetchNext();
   }

   size_t length() {
      return numRows;
   }

   this(PGresult* res) {
      this.res = res;
      numFields = PQnfields(res);
      numRows = PQntuples(res);

      if (numRows)
         fetchNext();
   }

   ~this() {
      PQclear(res);
   }

private:
   PGresult* res;
   int[string] mapping;
   string[] columnNames;
   int numFields;

   int position;

   int numRows;

   Row row;

   void fetchNext() {
      Row r;
      r.resultSet = this;
      string[] row;

      for (int i = 0; i < numFields; i++) {
         string a;

         if (PQgetisnull(res, position, i))
            a = null;
         else {
            a = copyCString(PQgetvalue(res, position, i), PQgetlength(res, position, i));

         }
         row ~= a;
      }

      r.row = row;
      this.row = r;
   }

   void makeFieldMapping() {
      for (int i = 0; i < numFields; i++) {
         string a = copyCString(PQfname(res, i));

         columnNames ~= a;
         mapping[a] = i;
      }

   }
}

struct Row {
   package string[] row;
   package ResultSet resultSet;

   string opIndex(size_t idx, string file = __FILE__, int line = __LINE__) {
      if (idx >= row.length)
         throw new Exception(text("index ", idx, " is out of bounds on result"), file, line);
      return row[idx];
   }

   string opIndex(string name, string file = __FILE__, int line = __LINE__) {
      auto idx = resultSet.getFieldIndex(name);
      if (idx >= row.length)
         throw new Exception(text("no field ", name, " in result"), file, line);
      return row[idx];
   }

   string toString() {
      return to!string(row);
   }

   string[string] toAA() {
      string[string] a;

      string[] fn = resultSet.fieldNames();

      foreach (i, r; row)
         a[fn[i]] = r;

      return a;
   }

   int opApply(int delegate(ref string, ref string) dg) {
      foreach (a, b; toAA())
         mixin(yield("a, b"));

      return 0;
   }

   string[] toStringArray() {
      return row;
   }
}

string copyCString(const char* c, int actualLength = -1) {
   const(char)* a = c;
   if (a is null)
      return null;

   string ret;
   if (actualLength == -1)
      while (*a) {
         ret ~= *a;
         a++;
      } else {
      ret = a[0 .. actualLength].idup;
   }

   return ret;
}

string toSql(PostgreSql db, Variant a) {
   auto v = a.peek!(void*);
   if (v && (*v is null)) {
      return "NULL";
   } else if (auto t = a.peek!(SysTime)) {
      return db.sysTimeToValue(*t);
   } else if (auto t = a.peek!(DateTime)) {
      // FIXME: this might be broken cuz of timezones!
      return db.sysTimeToValue(cast(SysTime)*t);
   } else if (auto t = a.peek!string) {
      auto str = *t;
      if (str is null)
         return "NULL";
      else
         return '\'' ~ db.escape(str) ~ '\'';
   } else {
      string str = to!string(a);
      return '\'' ~ db.escape(str) ~ '\'';
   }

   assert(0);
}

// just for convenience; "str".toSql(db);
string toSql(string s, PostgreSql db) {
   //if(s is null)
   //return "NULL";
   return '\'' ~ db.escape(s) ~ '\'';
}

string toSql(long s, PostgreSql db) {
   return to!string(s);
}

string escapedVariants(PostgreSql db, in string sql, Variant[string] t) {
   if (t.keys.length <= 0 || sql.indexOf("?") == -1) {
      return sql;
   }

   string fixedup;
   int currentStart = 0;
   // FIXME: let's make ?? render as ? so we have some escaping capability
   foreach (i, dchar c; sql) {
      if (c == '?') {
         fixedup ~= sql[currentStart .. i];

         int idxStart = cast(int)i + 1;
         int idxLength;

         bool isFirst = true;

         while (idxStart + idxLength < sql.length) {
            char C = sql[idxStart + idxLength];

            if ((C >= 'a' && C <= 'z') || (C >= 'A' && C <= 'Z') || C == '_' || (!isFirst && C >= '0' && C <= '9'))
               idxLength++;
            else
               break;

            isFirst = false;
         }

         auto idx = sql[idxStart .. idxStart + idxLength];

         if (idx in t) {
            fixedup ~= toSql(db, t[idx]);
            currentStart = idxStart + idxLength;
         } else {
            // just leave it there, it might be done on another layer
            currentStart = cast(int)i;
         }
      }
   }

   fixedup ~= sql[currentStart .. $];

   return fixedup;
}

/// Note: ?n params are zero based!
string escapedVariants(PostgreSql db, in string sql, Variant[] t) {
   // FIXME: let's make ?? render as ? so we have some escaping capability
   // if nothing to escape or nothing to escape with, don't bother
   if (t.length > 0 && sql.indexOf("?") != -1) {
      string fixedup;
      int currentIndex;
      int currentStart = 0;
      foreach (i, dchar c; sql) {
         if (c == '?') {
            fixedup ~= sql[currentStart .. i];

            int idx = -1;
            currentStart = cast(int)i + 1;
            if ((i + 1) < sql.length) {
               auto n = sql[i + 1];
               if (n >= '0' && n <= '9') {
                  currentStart = cast(int)i + 2;
                  idx = n - '0';
               }
            }
            if (idx == -1) {
               idx = currentIndex;
               currentIndex++;
            }

            if (idx < 0 || idx >= t.length)
               throw new Exception("SQL Parameter index is out of bounds: " ~ to!string(idx) ~ " at `" ~ sql[0 .. i] ~ "`");

            fixedup ~= toSql(db, t[idx]);
         }
      }

      fixedup ~= sql[currentStart .. $];

      return fixedup;
   }

   return sql;
}

private string yield(string what) {
   return `if(auto result = dg(` ~ what ~ `)) return result;`;
}

extern (C) {
   struct PGconn {
   };
   struct PGresult {
   };

   void PQfinish(PGconn*);
   PGconn* PQconnectdb(const char*);

   int PQstatus(PGconn*); // FIXME check return value

   const(char*) PQerrorMessage(PGconn*);

   PGresult* PQexec(PGconn*, const char*);
   void PQclear(PGresult*);

   PGresult* PQprepare(PGconn*, const char* stmtName, const char* query, int nParams, const void* paramTypes);

   PGresult* PQexecPrepared(PGconn*, const char* stmtName, int nParams, const char** paramValues,
         const int* paramLengths, const int* paramFormats, int resultFormat);

   int PQresultStatus(PGresult*); // FIXME check return value

   int PQnfields(PGresult*); // number of fields in a result
   const(char*) PQfname(PGresult*, int); // name of field

   int PQntuples(PGresult*); // number of rows in result
   const(char*) PQgetvalue(PGresult*, int row, int column);

   size_t PQescapeString(char* to, const char* from, size_t length);

   enum int CONNECTION_OK = 0;
   enum int PGRES_COMMAND_OK = 1;
   enum int PGRES_TUPLES_OK = 2;

   int PQgetlength(const PGresult* res, int row_number, int column_number);
   int PQgetisnull(const PGresult* res, int row_number, int column_number);

   char* PQcmdTuples(PGresult* res);

}
